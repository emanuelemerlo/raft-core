using System.Net.Http.Headers;
using System.Text.Json;
using RaftCore.Configuration;
using RaftCore.Models;
using RaftCore.Services.Common;
using RaftCore.Services.Core;
using RaftCore.Services.Http;

namespace RaftCore.Services.Network;

/// <summary>
/// Discovery a sondaggio: finché qualche membro non ha un URL noto o è irraggiungibile, il nodo prova gli host noti
/// (Discovery:SeedHosts e host di Peers) su tutte le porte ammesse (Discovery:ProbePorts, altrimenti RestPorts).
/// La risposta è firmata dal nodo che la invia: si registra solo l'URL effettivamente raggiunto, con l'identità verificata.
/// Così le istanze si trovano anche quando ciascuna ha scelto una porta diversa dell'elenco, senza broadcast.
/// </summary>
public sealed class PeerProbeService : BackgroundService
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly ClusterConfig _cfg;
    private readonly Membership _members;
    private readonly PeerRegistry _registry;
    private readonly RaftNode _node;
    private readonly Metrics _metrics;
    private readonly HttpClient _http;
    private readonly ILogger<PeerProbeService> _log;
    private readonly LogThrottle _throttle = new(TimeSpan.FromSeconds(60));
    private readonly long _startedMs = Environment.TickCount64;
    /// <summary>All'avvio gli altri nodi possono non essere ancora partiti: niente warning nei primi secondi.</summary>
    private const int WarnGraceMs = 15_000;

    public PeerProbeService(ClusterConfig cfg, Membership members, PeerRegistry registry, RaftNode node, Metrics metrics,
        IHttpClientFactory f, ILogger<PeerProbeService> log)
    {
        _cfg = cfg; _members = members; _registry = registry; _node = node; _metrics = metrics; _http = f.CreateClient("probe"); _log = log;
    }

    /// <summary>Coppie host:porta da sondare.</summary>
    public IReadOnlyList<string> Candidates()
    {
        var hosts = new List<string>(_cfg.Discovery.SeedHosts.Where(h => !string.IsNullOrWhiteSpace(h)).Select(h => h.Trim()));
        if (hosts.Count == 0) hosts.Add("127.0.0.1");
        foreach (var u in _cfg.Peers.Values)
            if (Uri.TryCreate(u, UriKind.Absolute, out var uri) && !hosts.Contains(uri.Host, StringComparer.OrdinalIgnoreCase)) hosts.Add(uri.Host);
        var ports = string.IsNullOrWhiteSpace(_cfg.Discovery.ProbePorts) ? _cfg.CandidatePorts() : ClusterConfig.ParsePortSpec(_cfg.Discovery.ProbePorts!);
        var self = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "127.0.0.1", "localhost", "::1", Environment.MachineName };
        var list = new List<string>();
        foreach (var h in hosts.Distinct(StringComparer.OrdinalIgnoreCase))
            foreach (var p in ports)
            {
                if (p == _cfg.RestPort && self.Contains(h)) continue;                 // sé stesso
                list.Add($"http://{(h.Contains(':') && !h.StartsWith('[') ? "[" + h + "]" : h)}:{p}");
            }
        return list;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        if (!_cfg.Discovery.ProbeEnabled || _cfg.ClusterSize == 1) return;
        try { await Task.Delay(300, ct); } catch (OperationCanceledException) { return; }
        while (!ct.IsCancellationRequested)
        {
            var missing = _registry.MissingOrUnreachable(_members.Others);
            if (missing.Count > 0)
            {
                try { await ProbeAsync(missing, ct); }
                catch (OperationCanceledException) { return; }
                catch (Exception ex) { _log.LogDebug("Sondaggio fallito: {Msg}", ex.Message); }
            }
            int wait = _registry.MissingOrUnreachable(_members.Others).Count > 0 ? _cfg.Discovery.ProbeIntervalMs : _cfg.Discovery.ProbeIdleIntervalMs;
            try { await Task.Delay(wait, ct); } catch (OperationCanceledException) { return; }
        }
    }

    internal async Task ProbeAsync(IReadOnlyCollection<string> missing, CancellationToken ct)
    {
        var candidates = Candidates();
        _metrics.Inc("probeRounds");
        using var gate = new SemaphoreSlim(32);
        var found = new List<(string Id, string Url)>();
        await Task.WhenAll(candidates.Select(async url =>
        {
            await gate.WaitAsync(ct);
            try
            {
                var who = await HelloAsync(url, ct);
                if (who != null) lock (found) found.Add((who, url));
            }
            finally { gate.Release(); }
        }));
        foreach (var (id, url) in found)
        {
            bool wasMissing = missing.Contains(id);
            _registry.NoteRpcUrl(id, url);
            if (wasMissing)
            {
                _metrics.Inc("probeFound");
                _log.LogInformation("Membro {Peer} trovato su {Url} (discovery a sondaggio)", id, url);
            }
        }
        var still = _registry.MissingOrUnreachable(_members.Others);
        if (still.Count > 0 && Environment.TickCount64 - _startedMs > WarnGraceMs && _throttle.ShouldLog("missing"))
            _log.LogWarning("Membri non ancora raggiungibili: {Peers}. Sondati {N} indirizzi (host {Hosts}).",
                string.Join(", ", still), candidates.Count, string.Join(", ", candidates.Select(c => new Uri(c).Host).Distinct()));
    }

    /// <summary>NodeId verificato del membro che risponde su url, oppure null.</summary>
    private async Task<string?> HelloAsync(string url, CancellationToken ct)
    {
        try
        {
            var req = new HelloRequest
            {
                ClusterId = _cfg.ClusterId, ClusterSize = _cfg.ClusterSize, MembersHash = _members.Hash, SenderNodeId = _members.SelfId,
                DestNodeId = RpcAuth.AnyDestination, SenderUrl = _node.SelfUrl
            };
            var body = JsonSerializer.SerializeToUtf8Bytes(req, Json);
            long ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var nonce = Guid.NewGuid().ToString("N");
            using var msg = new HttpRequestMessage(HttpMethod.Post, url + RpcAuth.HelloPath) { Content = new ByteArrayContent(body) };
            msg.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            msg.Headers.TryAddWithoutValidation(RpcAuth.HNode, _members.SelfId);
            msg.Headers.TryAddWithoutValidation(RpcAuth.HTime, ts.ToString());
            msg.Headers.TryAddWithoutValidation(RpcAuth.HNonce, nonce);
            msg.Headers.TryAddWithoutValidation(RpcAuth.HDest, RpcAuth.AnyDestination);
            msg.Headers.TryAddWithoutValidation(RpcAuth.HSig, RpcAuth.Sign(_cfg.ClusterSecret, "POST", RpcAuth.HelloPath, ts, body, _members.SelfId, nonce, RpcAuth.AnyDestination));
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct); cts.CancelAfter(_cfg.Discovery.ProbeTimeoutMs);
            using var res = await _http.SendAsync(msg, cts.Token);
            if ((int)res.StatusCode != 200) return null;
            var bytes = await res.Content.ReadAsByteArrayAsync(cts.Token);
            string responder = res.Headers.TryGetValues(RpcAuth.HNode, out var n) ? ClusterConfig.Norm(n.First()) : "";
            string sig = res.Headers.TryGetValues(RpcAuth.HSig, out var s) ? s.First() : "";
            if (responder.Length == 0 || !RpcAuth.FixedEquals(RpcAuth.SignResponse(_cfg.ClusterSecret, responder, nonce, 200, bytes), sig)) return null;
            var hello = JsonSerializer.Deserialize<HelloResponse>(bytes, Json);
            if (hello == null || ClusterConfig.Norm(hello.NodeId) != responder || responder == _members.SelfId || !_members.Contains(responder)) return null;
            return responder;
        }
        catch { return null; }                                                      // porta chiusa, altro servizio, timeout
    }
}
