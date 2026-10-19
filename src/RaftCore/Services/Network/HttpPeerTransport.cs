using System.Net.Http.Headers;
using System.Text.Json;
using RaftCore.Abstractions;
using RaftCore.Configuration;
using RaftCore.Models;
using RaftCore.Services.Common;
using RaftCore.Services.Http;

namespace RaftCore.Services.Network;

/// <summary>
/// RPC tra nodi su HTTP. Ogni richiesta è firmata e indica il destinatario; ogni risposta deve essere firmata
/// dal nodo atteso e legata al nonce della richiesta, altrimenti viene scartata (punto 8: identità del risponditore).
/// </summary>
public sealed class HttpPeerTransport : IPeerTransport
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly HttpClient _http;
    private readonly ClusterConfig _cfg;
    private readonly string _self;
    private readonly PeerRegistry _registry;
    private readonly Metrics _metrics;
    private readonly ILogger<HttpPeerTransport> _log;
    private readonly LogThrottle _throttle = new(TimeSpan.FromSeconds(30));

    public HttpPeerTransport(IHttpClientFactory f, ClusterConfig cfg, Core.Membership members, PeerRegistry registry, Metrics metrics, ILogger<HttpPeerTransport> log)
    {
        _http = f.CreateClient("raft-rpc"); _cfg = cfg; _self = members.SelfId; _registry = registry; _metrics = metrics; _log = log;
    }

    public async Task<RpcResult<TResp>> SendAsync<TReq, TResp>(PeerEndpoint peer, string path, TReq request, TimeSpan timeout, CancellationToken ct)
        where TReq : BaseRpcRequest
    {
        try
        {
            request.DestNodeId = peer.NodeId;
            var body = JsonSerializer.SerializeToUtf8Bytes(request, Json);
            long ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var nonce = Guid.NewGuid().ToString("N");
            using var msg = new HttpRequestMessage(HttpMethod.Post, peer.Url.TrimEnd('/') + path) { Content = new ByteArrayContent(body) };
            msg.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            msg.Headers.TryAddWithoutValidation(RpcAuth.HNode, _self);
            msg.Headers.TryAddWithoutValidation(RpcAuth.HTime, ts.ToString());
            msg.Headers.TryAddWithoutValidation(RpcAuth.HNonce, nonce);
            msg.Headers.TryAddWithoutValidation(RpcAuth.HDest, peer.NodeId);
            msg.Headers.TryAddWithoutValidation(RpcAuth.HSig, RpcAuth.Sign(_cfg.ClusterSecret, "POST", path, ts, body, _self, nonce, peer.NodeId));
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct); cts.CancelAfter(timeout);
            using var res = await _http.SendAsync(msg, cts.Token);
            var respBytes = await res.Content.ReadAsByteArrayAsync(cts.Token);
            int status = (int)res.StatusCode;

            string responder = res.Headers.TryGetValues(RpcAuth.HNode, out var n) ? ClusterConfig.Norm(n.First()) : "";
            string sig = res.Headers.TryGetValues(RpcAuth.HSig, out var s) ? s.First() : "";
            bool signed = sig.Length > 0 && responder.Length > 0
                && RpcAuth.FixedEquals(RpcAuth.SignResponse(_cfg.ClusterSecret, responder, nonce, status, respBytes), sig);

            if (signed && responder != peer.NodeId)
            {
                // L'URL porta a un altro membro: non deve MAI essere contato come peer.NodeId (doppio conteggio di voti/conferme)
                _metrics.Inc("rpcWrongDestination");
                _registry.InvalidateUrl(peer.NodeId, peer.Url);
                if (_throttle.ShouldLog("wrong:" + peer.NodeId + peer.Url))
                    _log.LogWarning("L'URL {Url} di {Peer} porta al nodo {Other}: URL invalidato", peer.Url, peer.NodeId, responder);
                return new RpcResult<TResp>(0, default);
            }
            if (status == 421 && signed) { _registry.InvalidateUrl(peer.NodeId, peer.Url); return new RpcResult<TResp>(0, default); }
            if (status != 200) return new RpcResult<TResp>(status, default);
            if (!signed)
            {
                _metrics.Inc("rpcResponseAuthFailed");
                if (_throttle.ShouldLog("unsigned:" + peer.NodeId))
                    _log.LogWarning("Risposta di {Peer} ({Url}) non firmata o firma non valida: scartata", peer.NodeId, peer.Url);
                return new RpcResult<TResp>(0, default);
            }
            _registry.NoteSuccess(peer.NodeId);
            return new RpcResult<TResp>(200, JsonSerializer.Deserialize<TResp>(respBytes, Json));
        }
        catch (Exception ex) when (ex is HttpRequestException or OperationCanceledException && !ct.IsCancellationRequested)
        {
            // nodo spento, porta cambiata o rete interrotta: dopo alcuni fallimenti il peer viene cercato dalla discovery
            if (_registry.NoteFailure(peer.NodeId, peer.Url) && _throttle.ShouldLog("unreach:" + peer.NodeId))
                _log.LogWarning("Membro {Peer} irraggiungibile su {Url}: avviata la ricerca su host e porte noti", peer.NodeId, peer.Url);
            return new RpcResult<TResp>(0, default);
        }
        catch { return new RpcResult<TResp>(0, default); }
    }
}
