using System.Text.Json;
using RaftCore.Configuration;
using RaftCore.Services.Common;
using RaftCore.Services.Core;
using RaftCore.Services.Network;

namespace RaftCore.Services.Http;

/// <summary>
/// Autenticazione delle RPC interne (§16) e firma delle loro risposte.
/// Ordine: header e appartenenza e finestra temporale (senza leggere il body), poi HMAC, destinatario, nonce,
/// ClusterId/ClusterSize/MembersHash (REQ-CLU-4/5, REQ-SEC-3/4). La risposta viene firmata con l'identità del nodo.
/// </summary>
public sealed class SecurityMiddleware
{
    private const long MaxInternalBody = 96L * 1024 * 1024;
    private readonly RequestDelegate _next;
    private readonly ClusterConfig _cfg;
    private readonly Membership _members;
    private readonly PeerRegistry _registry;
    private readonly RpcVerifier _verifier;
    private readonly Metrics _metrics;
    private readonly ILogger<SecurityMiddleware> _log;
    private readonly LogThrottle _throttle = new(TimeSpan.FromSeconds(30));

    public SecurityMiddleware(RequestDelegate next, ClusterConfig cfg, Membership members, PeerRegistry registry, RpcVerifier verifier, Metrics metrics, ILogger<SecurityMiddleware> log)
    { _next = next; _cfg = cfg; _members = members; _registry = registry; _verifier = verifier; _metrics = metrics; _log = log; }

    public async Task InvokeAsync(HttpContext ctx)
    {
        if (!ctx.Request.Path.StartsWithSegments("/internal")) { await _next(ctx); return; }

        var pre = _verifier.PreCheck(ctx.Request.Headers, out var node, out _);
        if (pre != RpcVerifyResult.Ok) { Reject(ctx, pre, node); return; }
        if (ctx.Request.ContentLength is null or > MaxInternalBody) { ctx.Response.StatusCode = 413; return; }

        var body = new byte[ctx.Request.ContentLength.Value];
        await ctx.Request.Body.ReadExactlyAsync(body, ctx.RequestAborted);
        var res = _verifier.Verify(ctx.Request, body);
        if (res != RpcVerifyResult.Ok) { Reject(ctx, res, node); return; }

        try
        {
            using var doc = JsonDocument.Parse(body);
            var r = doc.RootElement;
            string cid = r.TryGetProperty("clusterId", out var a) ? a.GetString() ?? "" : "";
            int size = r.TryGetProperty("clusterSize", out var b) ? b.GetInt32() : -1;
            string mh = r.TryGetProperty("membersHash", out var c) ? c.GetString() ?? "" : "";
            string sender = r.TryGetProperty("senderNodeId", out var d) ? d.GetString() ?? "" : "";
            string dest = r.TryGetProperty("destNodeId", out var f) ? f.GetString() ?? "" : "";
            string url = r.TryGetProperty("senderUrl", out var e) ? e.GetString() ?? "" : "";
            if (ClusterConfig.Norm(sender) != node || !RpcAuth.DestinationMatches(dest, _members.SelfId, ctx.Request.Path.Value)) { ctx.Response.StatusCode = 400; return; }
            if (cid != _cfg.ClusterId || size != _cfg.ClusterSize || mh != _members.Hash)
            {
                _metrics.Inc("membersHashMismatch");
                if (_throttle.ShouldLog("mismatch:" + node))
                    _log.LogError("ALLARME: RPC da {Node} con ClusterId/ClusterSize/MembersHash discordanti", node);
                ctx.Response.StatusCode = 409; return;
            }
            _registry.NoteRpcUrl(node, url);
        }
        catch (Exception) { ctx.Response.StatusCode = 400; return; }

        ctx.Request.Body = new MemoryStream(body, writable: false);
        ctx.Request.ContentLength = body.Length;

        // Firma della risposta: identità del risponditore legata al nonce della richiesta
        var original = ctx.Response.Body;
        using var buffer = new MemoryStream();
        ctx.Response.Body = buffer;
        try { await _next(ctx); }
        finally { ctx.Response.Body = original; }
        var respBytes = buffer.ToArray();
        ctx.Response.Headers[RpcAuth.HNode] = _members.SelfId;
        ctx.Response.Headers[RpcAuth.HSig] = RpcAuth.SignResponse(_cfg.ClusterSecret, _members.SelfId, ctx.Request.Headers[RpcAuth.HNonce].ToString(), ctx.Response.StatusCode, respBytes);
        ctx.Response.ContentLength = respBytes.Length;
        await original.WriteAsync(respBytes, ctx.RequestAborted);
    }

    private void Reject(HttpContext ctx, RpcVerifyResult r, string node)
    {
        if (r is RpcVerifyResult.NonMember or RpcVerifyResult.ClockSkew or RpcVerifyResult.WrongDestination && _throttle.ShouldLog(r + ":" + node))
            _log.LogWarning("ALLARME: RPC da {Node} rifiutata ({Reason})", node, r);
        ctx.Response.StatusCode = RpcVerifier.HttpStatusFor(r);
        if (r == RpcVerifyResult.WrongDestination)
        {
            // Il chiamante ha un URL che punta al nodo sbagliato: si firma la risposta così può invalidarlo in sicurezza
            ctx.Response.Headers[RpcAuth.HNode] = _members.SelfId;
            ctx.Response.Headers[RpcAuth.HSig] = RpcAuth.SignResponse(_cfg.ClusterSecret, _members.SelfId, ctx.Request.Headers[RpcAuth.HNonce].ToString(), 421, ReadOnlySpan<byte>.Empty);
        }
    }
}
