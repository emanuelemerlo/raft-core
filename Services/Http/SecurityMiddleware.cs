using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using RaftCore.Configuration;
using RaftCore.Services.Common;
using RaftCore.Services.Core;
using RaftCore.Services.Network;

namespace RaftCore.Services.Http;

/// <summary>
/// Autenticazione delle RPC interne (§16): HMAC, appartenenza, finestra temporale, nonce anti-replay,
/// ClusterId/ClusterSize/MembersHash (REQ-CLU-4/5, REQ-SEC-3/4).
/// </summary>
public sealed class SecurityMiddleware
{
    private const long MaxInternalBody = 64L * 1024 * 1024;
    private readonly RequestDelegate _next;
    private readonly ClusterConfig _cfg;
    private readonly Membership _members;
    private readonly PeerRegistry _registry;
    private readonly Metrics _metrics;
    private readonly ILogger<SecurityMiddleware> _log;
    private readonly ConcurrentDictionary<string, long> _nonces = new(); // (nodo:nonce) -> scadenza (ms epoch)
    private long _lastPurge;

    public SecurityMiddleware(RequestDelegate next, ClusterConfig cfg, Membership members, PeerRegistry registry, Metrics metrics, ILogger<SecurityMiddleware> log)
    { _next = next; _cfg = cfg; _members = members; _registry = registry; _metrics = metrics; _log = log; }

    public async Task InvokeAsync(HttpContext ctx)
    {
        if (!ctx.Request.Path.StartsWithSegments("/internal")) { await _next(ctx); return; }
        var h = ctx.Request.Headers;
        if (!h.TryGetValue(RpcAuth.HNode, out var nodeV) || !h.TryGetValue(RpcAuth.HTime, out var tsV) || !h.TryGetValue(RpcAuth.HNonce, out var nonceV)
            || !h.TryGetValue(RpcAuth.HSig, out var sigV) || !long.TryParse(tsV, out long ts))
        { _metrics.Inc("rpcInvalidAuth"); ctx.Response.StatusCode = 401; return; }
        if (ctx.Request.ContentLength is > MaxInternalBody) { ctx.Response.StatusCode = 413; return; }

        ctx.Request.EnableBuffering(MaxInternalBody);
        using var ms = new MemoryStream();
        await ctx.Request.Body.CopyToAsync(ms, ctx.RequestAborted);
        byte[] body = ms.ToArray();
        ctx.Request.Body.Position = 0;

        string node = ClusterConfig.Norm(nodeV.ToString()), nonce = nonceV.ToString();
        // 1) HMAC
        var expected = RpcAuth.Sign(_cfg.ClusterSecret, ctx.Request.Method, ctx.Request.Path.Value ?? "", ts, body, nodeV.ToString(), nonce);
        if (!CryptographicOperations.FixedTimeEquals(Encoding.ASCII.GetBytes(expected), Encoding.ASCII.GetBytes(sigV.ToString())))
        { _metrics.Inc("rpcInvalidAuth"); ctx.Response.StatusCode = 401; return; }
        // 2) appartenenza (REQ-CLU-5)
        if (!_members.Contains(node))
        {
            _metrics.Inc("nonMemberRejected");
            _log.LogWarning("ALLARME: RPC da non membro {Node} rifiutata", node);
            ctx.Response.StatusCode = 403; return;
        }
        // 3) finestra temporale (REQ-SEC-4/5): solo disponibilità, mai safety
        long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (Math.Abs(nowMs - ts) > _cfg.RpcClockToleranceMs)
        {
            _metrics.Inc("rpcClockSkewRejected");
            _log.LogError("ALLARME: RPC da {Node} fuori finestra temporale (skew {Skew} ms)", node, nowMs - ts);
            ctx.Response.StatusCode = 401; return;
        }
        // 4) nonce anti-replay, conservato almeno 2 x finestra
        Purge(nowMs);
        if (!_nonces.TryAdd(node + ":" + nonce, nowMs + 2L * _cfg.RpcClockToleranceMs + 1000))
        { _metrics.Inc("rpcReplayRejected"); ctx.Response.StatusCode = 401; return; }
        // 5) ClusterId / ClusterSize / MembersHash (REQ-CLU-4)
        try
        {
            using var doc = JsonDocument.Parse(body);
            var r = doc.RootElement;
            string cid = r.TryGetProperty("clusterId", out var a) ? a.GetString() ?? "" : "";
            int size = r.TryGetProperty("clusterSize", out var b) ? b.GetInt32() : -1;
            string mh = r.TryGetProperty("membersHash", out var c) ? c.GetString() ?? "" : "";
            string sender = r.TryGetProperty("senderNodeId", out var d) ? d.GetString() ?? "" : "";
            string url = r.TryGetProperty("senderUrl", out var e) ? e.GetString() ?? "" : "";
            if (ClusterConfig.Norm(sender) != node) { ctx.Response.StatusCode = 400; return; }
            if (cid != _cfg.ClusterId || size != _cfg.ClusterSize || mh != _members.Hash)
            {
                _metrics.Inc("membersHashMismatch");
                _log.LogError("ALLARME: RPC da {Node} con ClusterId/ClusterSize/MembersHash discordanti", node);
                ctx.Response.StatusCode = 409; return;
            }
            _registry.NoteRpcUrl(node, url);
        }
        catch (Exception) { ctx.Response.StatusCode = 400; return; }
        await _next(ctx);
    }

    private void Purge(long nowMs)
    {
        if (nowMs - _lastPurge < 5000) return;
        _lastPurge = nowMs;
        foreach (var kv in _nonces) if (kv.Value < nowMs) _nonces.TryRemove(kv.Key, out _);
    }
}
