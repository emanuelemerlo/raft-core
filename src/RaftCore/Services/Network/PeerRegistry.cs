using RaftCore.Abstractions;
using RaftCore.Configuration;
using RaftCore.Models;

namespace RaftCore.Services.Network;

public class PeerInfo
{
    public string NodeId { get; set; } = string.Empty;
    /// <summary>Dall'ultima RPC autenticata ricevuta dal peer stesso (fonte più affidabile, REQ-DIS-5).</summary>
    public string? RpcUrl { get; set; }
    /// <summary>Dall'annuncio UDP firmato.</summary>
    public string? UdpUrl { get; set; }
    public long UdpSeenMs { get; set; } = long.MinValue / 2;
    /// <summary>Da configurazione (Peers).</summary>
    public string? StaticUrl { get; set; }
    /// <summary>Propagato dal Leader negli AppendEntries.</summary>
    public string? GossipUrl { get; set; }
    public string Incarnation { get; set; } = string.Empty;
    public long Sequence { get; set; }
    public NodeRole Role { get; set; }
    public long Term { get; set; }
    public string? BestUrl => RpcUrl ?? UdpUrl ?? StaticUrl ?? GossipUrl;
}

public enum RegisterResult { Accepted, Replay }

/// <summary>
/// REQ-DIS-3/4/5: endpoint dei peer, NON influisce sul quorum. Più fonti con priorità
/// RPC autenticata &gt; annuncio UDP &gt; configurazione statica &gt; gossip del Leader.
/// Un URL che risulta portare a un altro nodo (risposta firmata da un NodeId diverso) viene invalidato.
/// </summary>
public sealed class PeerRegistry
{
    private readonly object _l = new();
    private readonly Dictionary<string, PeerInfo> _peers = new();
    private readonly IClock _clock;
    private readonly int _timeoutMs;

    public PeerRegistry(IClock clock, ClusterConfig cfg)
    {
        _clock = clock; _timeoutMs = cfg.AnnounceTimeoutMs;
        foreach (var kv in cfg.Peers)
        {
            var id = ClusterConfig.Norm(kv.Key);
            _peers[id] = new PeerInfo { NodeId = id, StaticUrl = Clean(kv.Value) };
        }
    }

    private static string Clean(string url) => url.Trim().TrimEnd('/');
    private PeerInfo Get(string id) => _peers.TryGetValue(id, out var p) ? p : _peers[id] = new PeerInfo { NodeId = id };

    public RegisterResult RegisterOrUpdate(DiscoveryMessage msg)
    {
        lock (_l)
        {
            var p = Get(msg.NodeId);
            if (p.Incarnation != "" && p.Incarnation == msg.Incarnation && msg.Sequence <= p.Sequence) return RegisterResult.Replay; // REQ-DIS-5
            p.UdpUrl = Clean(msg.RestUrl); p.UdpSeenMs = _clock.MonotonicMs;
            p.Incarnation = msg.Incarnation; p.Sequence = msg.Sequence; p.Role = msg.Role; p.Term = msg.Term;
            return RegisterResult.Accepted;
        }
    }

    public void NoteRpcUrl(string nodeId, string url)
    {
        if (string.IsNullOrWhiteSpace(url)) return;
        lock (_l) Get(ClusterConfig.Norm(nodeId)).RpcUrl = Clean(url);
    }

    /// <summary>URL propagati dal Leader: usati solo per i peer di cui non si conosce un URL diretto.</summary>
    public void NoteGossip(IReadOnlyDictionary<string, string> urls, string selfId)
    {
        if (urls.Count == 0) return;
        lock (_l)
        {
            foreach (var (id0, url) in urls)
            {
                var id = ClusterConfig.Norm(id0);
                if (id == selfId || string.IsNullOrWhiteSpace(url)) continue;
                Get(id).GossipUrl = Clean(url);
            }
        }
    }

    /// <summary>Il nodo raggiunto tramite questo URL non è nodeId: l'URL viene rimosso da tutte le fonti di quel peer.</summary>
    public void InvalidateUrl(string nodeId, string url)
    {
        var u = Clean(url);
        lock (_l)
        {
            if (!_peers.TryGetValue(nodeId, out var p)) return;
            if (p.RpcUrl == u) p.RpcUrl = null;
            if (p.UdpUrl == u) p.UdpUrl = null;
            if (p.GossipUrl == u) p.GossipUrl = null;
            if (p.StaticUrl == u) p.StaticUrl = null;
        }
    }

    /// <summary>URL noto di un peer (anche non recente: serve a ritentare; la freschezza non è una condizione di quorum).</summary>
    public string? GetUrl(string nodeId)
    {
        lock (_l) return _peers.TryGetValue(nodeId, out var p) ? p.BestUrl : null;
    }

    public Dictionary<string, string> KnownUrls(IEnumerable<string> members)
    {
        lock (_l)
        {
            var d = new Dictionary<string, string>();
            foreach (var m in members) if (_peers.TryGetValue(m, out var p) && p.BestUrl != null) d[m] = p.BestUrl;
            return d;
        }
    }

    public List<PeerInfo> GetVisiblePeers()
    {
        lock (_l)
        {
            long now = _clock.MonotonicMs;
            return _peers.Values.Where(p => p.BestUrl != null).Select(p => new PeerInfo
            {
                NodeId = p.NodeId, RpcUrl = p.RpcUrl, UdpUrl = now - p.UdpSeenMs <= _timeoutMs ? p.UdpUrl : null, StaticUrl = p.StaticUrl,
                GossipUrl = p.GossipUrl, Role = p.Role, Term = p.Term, UdpSeenMs = p.UdpSeenMs
            }).ToList();
        }
    }
}
