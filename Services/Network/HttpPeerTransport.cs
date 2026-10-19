using System.Net.Http.Headers;
using System.Text.Json;
using RaftCore.Abstractions;
using RaftCore.Configuration;
using RaftCore.Models;
using RaftCore.Services.Http;

namespace RaftCore.Services.Network;

public sealed class HttpPeerTransport : IPeerTransport
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly HttpClient _http;
    private readonly ClusterConfig _cfg;
    private readonly string _self;

    public HttpPeerTransport(IHttpClientFactory f, ClusterConfig cfg, RaftCore.Services.Core.Membership members)
    {
        _http = f.CreateClient("raft-rpc"); _cfg = cfg; _self = members.SelfId;
    }

    public async Task<RpcResult<TResp>> SendAsync<TReq, TResp>(PeerEndpoint peer, string path, TReq request, TimeSpan timeout, CancellationToken ct)
        where TReq : BaseRpcRequest
    {
        try
        {
            var body = JsonSerializer.SerializeToUtf8Bytes(request, Json);
            long ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var nonce = Guid.NewGuid().ToString("N");
            using var msg = new HttpRequestMessage(HttpMethod.Post, peer.Url.TrimEnd('/') + path) { Content = new ByteArrayContent(body) };
            msg.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            msg.Headers.TryAddWithoutValidation(RpcAuth.HNode, _self);
            msg.Headers.TryAddWithoutValidation(RpcAuth.HTime, ts.ToString());
            msg.Headers.TryAddWithoutValidation(RpcAuth.HNonce, nonce);
            msg.Headers.TryAddWithoutValidation(RpcAuth.HSig, RpcAuth.Sign(_cfg.ClusterSecret, "POST", path, ts, body, _self, nonce));
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct); cts.CancelAfter(timeout);
            using var res = await _http.SendAsync(msg, cts.Token);
            if ((int)res.StatusCode != 200) return new RpcResult<TResp>((int)res.StatusCode, default);
            var resp = await res.Content.ReadFromJsonAsync<TResp>(Json, cts.Token);
            return new RpcResult<TResp>(200, resp);
        }
        catch { return new RpcResult<TResp>(0, default); }
    }
}
