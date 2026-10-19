using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Http;
using RaftCore.Configuration;
using RaftCore.Services.Common;
using RaftCore.Services.Core;

namespace RaftCore.Services.Http;

/// <summary>
/// REQ-SEC-3: HMAC su metodo, path, timestamp, hash del body, NodeId mittente, nonce e NodeId destinatario.
/// Le risposte sono firmate dal risponditore su (NodeId risponditore, nonce della richiesta, status, hash del body):
/// chi chiama verifica di aver parlato con il nodo atteso e che la risposta non sia un replay (punto 8).
/// </summary>
public static class RpcAuth
{
    public const string HNode = "X-Cluster-Node", HTime = "X-Cluster-Timestamp", HNonce = "X-Cluster-Nonce", HSig = "X-Cluster-Signature",
        HDest = "X-Cluster-Dest", HExtra = "X-Cluster-Extra";
    /// <summary>Endpoint di sondaggio della discovery: l'unico che accetta il destinatario jolly (non modifica stato).</summary>
    public const string HelloPath = "/internal/v1/hello", AnyDestination = "*";

    public static bool DestinationMatches(string dest, string selfId, string? path) =>
        ClusterConfig.Norm(dest) == selfId || (dest == AnyDestination && path == HelloPath);

    public static string Sign(string secret, string method, string path, long tsMs, ReadOnlySpan<byte> body, string nodeId, string nonce, string dest, string extra = "")
    {
        var bodyHash = Convert.ToHexString(SHA256.HashData(body)).ToLowerInvariant();
        var raw = $"REQ\n{method.ToUpperInvariant()}\n{path}\n{tsMs}\n{bodyHash}\n{ClusterConfig.Norm(nodeId)}\n{nonce}\n{ClusterConfig.Norm(dest)}\n{extra}";
        return Mac(secret, raw);
    }

    public static string SignResponse(string secret, string responderId, string requestNonce, int status, ReadOnlySpan<byte> body)
    {
        var bodyHash = Convert.ToHexString(SHA256.HashData(body)).ToLowerInvariant();
        return Mac(secret, $"RESP\n{ClusterConfig.Norm(responderId)}\n{requestNonce}\n{status}\n{bodyHash}");
    }

    private static string Mac(string secret, string raw) =>
        Convert.ToHexString(HMACSHA256.HashData(Encoding.UTF8.GetBytes(secret), Encoding.UTF8.GetBytes(raw))).ToLowerInvariant();

    public static bool FixedEquals(string a, string b) =>
        CryptographicOperations.FixedTimeEquals(Encoding.ASCII.GetBytes(a), Encoding.ASCII.GetBytes(b));
}

public enum RpcVerifyResult { Ok, Missing, BadSignature, NonMember, ClockSkew, Replay, WrongDestination }

/// <summary>Verifica condivisa da RPC interne e inoltri del proxy: firma, appartenenza, finestra temporale, nonce, destinatario.</summary>
public sealed class RpcVerifier
{
    private readonly ClusterConfig _cfg;
    private readonly Membership _members;
    private readonly Metrics _metrics;
    private readonly ConcurrentDictionary<string, long> _nonces = new(); // (nodo:nonce) -> scadenza (ms epoch)
    private long _lastPurge;

    public RpcVerifier(ClusterConfig cfg, Membership members, Metrics metrics) { _cfg = cfg; _members = members; _metrics = metrics; }

    /// <summary>Controlli che non richiedono il body: eseguiti PRIMA di leggerlo (evita DoS con body grandi non autenticati).</summary>
    public RpcVerifyResult PreCheck(IHeaderDictionary h, out string node, out long ts)
    {
        node = ""; ts = 0;
        if (!h.TryGetValue(RpcAuth.HNode, out var nodeV) || !h.TryGetValue(RpcAuth.HTime, out var tsV) || !h.TryGetValue(RpcAuth.HNonce, out _)
            || !h.TryGetValue(RpcAuth.HSig, out _) || !h.TryGetValue(RpcAuth.HDest, out _) || !long.TryParse(tsV, out ts))
        { _metrics.Inc("rpcInvalidAuth"); return RpcVerifyResult.Missing; }
        node = ClusterConfig.Norm(nodeV.ToString());
        if (!_members.Contains(node)) { _metrics.Inc("nonMemberRejected"); return RpcVerifyResult.NonMember; }
        long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (Math.Abs(nowMs - ts) > _cfg.RpcClockToleranceMs) { _metrics.Inc("rpcClockSkewRejected"); return RpcVerifyResult.ClockSkew; }
        return RpcVerifyResult.Ok;
    }

    public RpcVerifyResult Verify(HttpRequest req, ReadOnlySpan<byte> body, string extra = "")
    {
        var pre = PreCheck(req.Headers, out var node, out var ts);
        if (pre != RpcVerifyResult.Ok) return pre;
        var h = req.Headers;
        string nonce = h[RpcAuth.HNonce].ToString(), dest = h[RpcAuth.HDest].ToString();
        var expected = RpcAuth.Sign(_cfg.ClusterSecret, req.Method, req.Path.Value ?? "", ts, body, node, nonce, dest, extra);
        if (!RpcAuth.FixedEquals(expected, h[RpcAuth.HSig].ToString())) { _metrics.Inc("rpcInvalidAuth"); return RpcVerifyResult.BadSignature; }
        if (!RpcAuth.DestinationMatches(dest, _members.SelfId, req.Path.Value)) { _metrics.Inc("rpcWrongDestination"); return RpcVerifyResult.WrongDestination; }
        long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        Purge(nowMs);
        if (!_nonces.TryAdd(node + ":" + nonce, nowMs + 2L * _cfg.RpcClockToleranceMs + 1000)) { _metrics.Inc("rpcReplayRejected"); return RpcVerifyResult.Replay; }
        return RpcVerifyResult.Ok;
    }

    private void Purge(long nowMs)
    {
        long last = Interlocked.Read(ref _lastPurge);
        if (nowMs - last < 5000 || Interlocked.CompareExchange(ref _lastPurge, nowMs, last) != last) return;
        foreach (var kv in _nonces) if (kv.Value < nowMs) _nonces.TryRemove(kv.Key, out _);
    }

    public static int HttpStatusFor(RpcVerifyResult r) => r switch
    {
        RpcVerifyResult.NonMember => 403,
        RpcVerifyResult.WrongDestination => 421,
        _ => 401
    };
}
