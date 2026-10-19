using System.Buffers;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text.Json;
using RaftCore.Abstractions;
using RaftCore.Configuration;
using RaftCore.Services.Common;
using RaftCore.Services.Core;

namespace RaftCore.Services.Http;

/// <summary>POST /api/v1/data: validazione, Smart Proxy (§15) e percorso sul Leader (REQ-PRX-9).</summary>
public sealed class ClientApi
{
    public const string HForwardHops = "X-Forward-Hops", HForwardedBy = "X-Forwarded-By", HDeadline = "X-Raft-Deadline-Ms";
    private const string DataPath = "/api/v1/data";

    private static readonly HashSet<string> HopByHop = new(StringComparer.OrdinalIgnoreCase)
    { "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding", "Upgrade", "Host", "Content-Length", "Proxy-Connection" };

    /// <summary>Header interni: mai copiati da una richiesta client né restituiti al client.</summary>
    private static readonly HashSet<string> Internal = new(StringComparer.OrdinalIgnoreCase)
    { HForwardHops, HForwardedBy, HDeadline, RpcAuth.HNode, RpcAuth.HTime, RpcAuth.HNonce, RpcAuth.HSig, RpcAuth.HDest, RpcAuth.HExtra };

    private readonly ClusterConfig _cfg;
    private readonly RaftNode _node;
    private readonly IClock _clock;
    private readonly Metrics _metrics;
    private readonly RpcVerifier _verifier;
    private readonly HttpClient _http;
    private readonly string _self;
    private readonly ILogger<ClientApi> _log;
    private int _inflight;

    public ClientApi(ClusterConfig cfg, RaftNode node, IClock clock, Metrics metrics, RpcVerifier verifier, IHttpClientFactory f, Membership members, ILogger<ClientApi> log)
    { _cfg = cfg; _node = node; _clock = clock; _metrics = metrics; _verifier = verifier; _http = f.CreateClient("forward"); _self = members.SelfId; _log = log; }

    public async Task HandleAsync(HttpContext ctx)
    {
        // REQ-PRX-10: limite sulle richieste in corso, PRIMA di leggere il body
        if (Interlocked.Increment(ref _inflight) > _cfg.MaxConcurrentRequests)
        {
            Interlocked.Decrement(ref _inflight);
            _metrics.Inc("requestRejectedOverload");
            ctx.Response.Headers["Retry-After"] = "1"; ctx.Response.StatusCode = 503;
            return;
        }
        byte[]? buffer = null;
        try
        {
            long deadline = _clock.MonotonicMs + _cfg.RequestDeadlineMs;                  // REQ-PRX-8: un unico deadline
            _metrics.Inc("requestCount");

            // REQ-IDM-1
            if (!ctx.Request.Headers.TryGetValue("X-Request-ID", out var ridV) || !Guid.TryParse(ridV.ToString(), out var rid))
            { await Reply(ctx, 400, "{\"error\":\"X-Request-ID mancante o non UUID\"}"); return; }
            string requestId = rid.ToString("D");
            using var scope = _log.BeginScope(new Dictionary<string, object> { ["RequestId"] = requestId });

            // REQ-PRX-2 / API-2: 413 prima di inoltrare o scrivere
            if (ctx.Request.ContentLength is long cl && cl > _cfg.MaxBodyBytes) { await Reply(ctx, 413, "{\"error\":\"body troppo grande\"}"); return; }
            // REQ-PRX-10 / IDM-8: body letto UNA sola volta in un buffer riutilizzabile
            int len = 0;
            buffer = ArrayPool<byte>.Shared.Rent((int)Math.Min(_cfg.MaxBodyBytes + 1, Math.Max(4096, ctx.Request.ContentLength ?? 16384)));
            while (true)
            {
                if (len == buffer.Length)
                {
                    var bigger = ArrayPool<byte>.Shared.Rent((int)Math.Min(_cfg.MaxBodyBytes + 1, (long)buffer.Length * 2));
                    Buffer.BlockCopy(buffer, 0, bigger, 0, len); ArrayPool<byte>.Shared.Return(buffer); buffer = bigger;
                }
                int n = await ctx.Request.Body.ReadAsync(buffer.AsMemory(len, buffer.Length - len), ctx.RequestAborted);
                if (n == 0) break;
                len += n;
                if (len > _cfg.MaxBodyBytes) { await Reply(ctx, 413, "{\"error\":\"body troppo grande\"}"); return; }
            }
            var body = new ReadOnlyMemory<byte>(buffer, 0, len);
            if (!IsValidJson(body.Span)) { await Reply(ctx, 400, "{\"error\":\"payload JSON non valido\"}"); return; }
            string hash = Convert.ToHexString(SHA256.HashData(body.Span)).ToLowerInvariant();       // REQ-IDM-8

            // Gli header di inoltro valgono solo se la richiesta è un inoltro AUTENTICATO di un altro membro
            int hops = 0;
            if (ctx.Request.Headers.ContainsKey(RpcAuth.HSig) || ctx.Request.Headers.ContainsKey(HForwardHops))
            {
                var extra = ForwardExtra(ctx.Request.Headers[HForwardHops].ToString(), ctx.Request.Headers[HDeadline].ToString(), requestId);
                var vr = _verifier.Verify(ctx.Request, body.Span, extra);
                if (vr == RpcVerifyResult.WrongDestination) { ctx.Response.StatusCode = 421; return; } // il mittente ha un URL stantio: ritenta
                if (vr == RpcVerifyResult.Ok)
                {
                    hops = int.TryParse(ctx.Request.Headers[HForwardHops], out var h) ? h : 1;
                    if (long.TryParse(ctx.Request.Headers[HDeadline], out var remaining) && remaining > 0)
                        deadline = Math.Min(deadline, _clock.MonotonicMs + remaining);
                }
                else _metrics.Inc("forwardUntrustedHeaders");                             // header ignorati: richiesta trattata come client
            }

            await RouteAsync(ctx, requestId, hash, body, hops, deadline);
        }
        catch (OperationCanceledException) when (ctx.RequestAborted.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _metrics.Inc("requestFailureCount");
            _log.LogError(ex, "Errore nella gestione della richiesta");
            if (!ctx.Response.HasStarted) { ctx.Response.Headers["Retry-After"] = "1"; ctx.Response.StatusCode = 503; }
        }
        finally
        {
            if (buffer != null) ArrayPool<byte>.Shared.Return(buffer);
            Interlocked.Decrement(ref _inflight);
        }
    }

    internal static string ForwardExtra(string hops, string deadline, string requestId) => $"{hops}|{deadline}|{requestId}";

    private async Task RouteAsync(HttpContext ctx, string requestId, string hash, ReadOnlyMemory<byte> body, int hops, long deadline)
    {
        bool maybeDelivered = false;
        var ct = ctx.RequestAborted;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            long remaining = deadline - _clock.MonotonicMs;
            if (remaining <= 0) break;

            // --- sono (diventato) Leader: percorso locale
            if (_node.IsLeader)
            {
                var r = await _node.WriteAsync(requestId, hash, body, deadline, ct);
                if (!r.NotLeader)
                {
                    if (r.Status == 504) maybeDelivered = true;
                    // 503 = leadership persa prima dell'esito: se la richiesta arriva da un client si attende il nuovo Leader
                    // entro il deadline; se arriva da un inoltro risponde il nodo che ha inoltrato, che ritenta.
                    if (r.Status == 503 && hops == 0 && deadline - _clock.MonotonicMs > 0) { maybeDelivered = true; await Backoff(deadline, ct); continue; }
                    if (r.Status >= 500) { _metrics.Inc("requestFailureCount"); if (r.Status == 503) ctx.Response.Headers["Retry-After"] = "1"; }
                    if (r.Replay) ctx.Response.Headers["X-Idempotent-Replay"] = "true";
                    ctx.Response.StatusCode = r.Status;
                    if (r.Body.Length > 0) { ctx.Response.ContentType = "application/json"; await ctx.Response.Body.WriteAsync(r.Body, ct); }
                    return;
                }
                continue;
            }

            // --- REQ-PRX-6: anti-loop (solo per inoltri autenticati)
            if (hops >= _cfg.MaxForwardHops) { ctx.Response.StatusCode = 421; return; }

            // --- Follower: individua il Leader (REQ-PRX-4) o attendi (REQ-PRX-8)
            var leader = _node.GetKnownLeader();
            if (leader == null)
            {
                if (!await WaitForLeaderAsync(Math.Min(_cfg.FailoverWaitMs, deadline - _clock.MonotonicMs), ct)) break;
                continue;
            }

            // --- inoltro trasparente e firmato (REQ-PRX-1/3)
            try
            {
                using var req = BuildForward(ctx, leader.Value.Id, leader.Value.Url, body, hops, deadline - _clock.MonotonicMs, requestId);
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                // Timeout PER TENTATIVO interno al deadline unico (REQ-PRX-8): oltre, il Leader è congelato o partizionato
                // e si ritenta (sicuro grazie all'idempotenza) sul Leader corrente.
                cts.CancelAfter((int)Math.Max(1, Math.Min(deadline - _clock.MonotonicMs, _cfg.ReplicationTimeoutMs + _cfg.ElectionTimeoutMaxMs)));
                using var res = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token);
                int code = (int)res.StatusCode;
                if (code is 503 or 421 or 504)
                {
                    // Leader non pronto, non più Leader, o esito non ancora osservato: si ritenta entro il deadline
                    if (code == 504) maybeDelivered = true;
                }
                else
                {
                    // REQ-PRX-7: status, header e body del Leader invariati (esclusi gli header interni)
                    ctx.Response.StatusCode = code;
                    foreach (var hd in res.Headers.Concat(res.Content.Headers))
                        if (!HopByHop.Contains(hd.Key) && !Internal.Contains(hd.Key)) ctx.Response.Headers[hd.Key] = hd.Value.ToArray();
                    await res.Content.CopyToAsync(ctx.Response.Body, ct);
                    return;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
            catch (Exception ex)
            {
                // connessione rifiutata = mai consegnata; reset/timeout dopo l'invio = esito ignoto
                if (!(ex is HttpRequestException { InnerException: SocketException { SocketErrorCode: SocketError.ConnectionRefused } })) maybeDelivered = true;
            }

            _metrics.Inc("requestRetryCount");
            await Backoff(deadline, ct);
        }

        // deadline scaduto (REQ-PRX-8): 503 se mai consegnata a un Leader, 504 se esito non osservato
        _metrics.Inc("requestFailureCount");
        if (maybeDelivered) { ctx.Response.StatusCode = 504; }
        else { ctx.Response.Headers["Retry-After"] = "1"; ctx.Response.StatusCode = 503; }
    }

    private HttpRequestMessage BuildForward(HttpContext ctx, string leaderId, string leaderUrl, ReadOnlyMemory<byte> body, int hops, long remainingMs, string requestId)
    {
        var req = new HttpRequestMessage(HttpMethod.Post, leaderUrl.TrimEnd('/') + DataPath);
        var seg = Segment(body);
        req.Content = new ByteArrayContent(seg.Array!, seg.Offset, seg.Count); // stesso buffer, nessuna rilettura dello stream
        foreach (var hd in ctx.Request.Headers)
        {
            if (HopByHop.Contains(hd.Key) || Internal.Contains(hd.Key)) continue;
            if (!req.Headers.TryAddWithoutValidation(hd.Key, hd.Value.ToArray())) req.Content.Headers.TryAddWithoutValidation(hd.Key, hd.Value.ToArray());
        }
        string hopsV = (hops + 1).ToString(), deadlineV = Math.Max(1, remainingMs).ToString();
        long ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var nonce = Guid.NewGuid().ToString("N");
        req.Headers.TryAddWithoutValidation(HForwardedBy, _self);
        req.Headers.TryAddWithoutValidation(HForwardHops, hopsV);
        req.Headers.TryAddWithoutValidation(HDeadline, deadlineV);
        req.Headers.TryAddWithoutValidation(RpcAuth.HNode, _self);
        req.Headers.TryAddWithoutValidation(RpcAuth.HTime, ts.ToString());
        req.Headers.TryAddWithoutValidation(RpcAuth.HNonce, nonce);
        req.Headers.TryAddWithoutValidation(RpcAuth.HDest, leaderId);
        req.Headers.TryAddWithoutValidation(RpcAuth.HSig, RpcAuth.Sign(_cfg.ClusterSecret, "POST", DataPath, ts, body.Span, _self, nonce, leaderId,
            ForwardExtra(hopsV, deadlineV, requestId)));
        return req;
    }

    /// <summary>REQ-PRX-5: backoff 100-500 ms con jitter, entro il deadline.</summary>
    private async Task Backoff(long deadline, CancellationToken ct)
    {
        int backoff = (int)Math.Min(Random.Shared.Next(100, 501), Math.Max(0, deadline - _clock.MonotonicMs));
        if (backoff > 0) await Task.Delay(backoff, ct);
    }

    private async Task<bool> WaitForLeaderAsync(long maxMs, CancellationToken ct)
    {
        long end = _clock.MonotonicMs + Math.Max(0, maxMs);
        while (_clock.MonotonicMs < end)
        {
            ct.ThrowIfCancellationRequested();                                              // client disconnesso: nessun ciclo a vuoto
            if (_node.IsLeader || _node.GetKnownLeader() != null) return true;
            await Task.WhenAny(_node.LeaderChanged, Task.Delay(50, ct));
        }
        return _node.IsLeader || _node.GetKnownLeader() != null;
    }

    private static ArraySegment<byte> Segment(ReadOnlyMemory<byte> m) =>
        System.Runtime.InteropServices.MemoryMarshal.TryGetArray(m, out var seg) ? seg : new ArraySegment<byte>(m.ToArray());

    private static bool IsValidJson(ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty) return false;
        try { var r = new Utf8JsonReader(data); while (r.Read()) { } return true; } catch (JsonException) { return false; }
    }

    private static async Task Reply(HttpContext ctx, int status, string json)
    {
        ctx.Response.StatusCode = status; ctx.Response.ContentType = "application/json";
        await ctx.Response.WriteAsync(json);
    }
}
