using RaftCore.Abstractions;
using RaftCore.Models;

namespace RaftCore.Services.Network;

public class PeerInfo
{
    public string NodeId { get; set; } = string.Empty;
    public string RestUrl { get; set; } = string.Empty;   // da annuncio UDP
    public string? RpcUrl { get; set; }                   // dall'ultima RPC valida (preferito, REQ-DIS-5)
    public long LastSeenMs { get; set; }
    public string Incarnation { get; set; } = string.Empty;
    public long Sequence { get; set; }
    public NodeRole Role { get; set; }
    public long Term { get; set; }
    public bool IsStatic { get; set; }
}

public enum RegisterResult { Accepted, Replay }

/// <summary>REQ-DIS-3/4/5: solo peer validi; freschezza su clock monotono locale; NON influisce sul quorum.</summary>
public sealed class PeerRegistry
{
    private readonly object _l = new();
    private readonly Dictionary<string, PeerInfo> _peers = new();
    private readonly IClock _clock;
    private readonly int _timeoutMs;
    public PeerRegistry(IClock clock, RaftCore.Configuration.ClusterConfig cfg)
    {
        _clock = clock; _timeoutMs = cfg.AnnounceTimeoutMs;
        foreach (var kv in cfg.Peers)
            _peers[RaftCore.Configuration.ClusterConfig.Norm(kv.Key)] = new PeerInfo { NodeId = RaftCore.Configuration.ClusterConfig.Norm(kv.Key), RestUrl = kv.Value.TrimEnd('/'), IsStatic = true, LastSeenMs = long.MaxValue / 2 };
    }

    public RegisterResult RegisterOrUpdate(DiscoveryMessage msg)
    {
        lock (_l)
        {
            long now = _clock.MonotonicMs;
            if (_peers.TryGetValue(msg.NodeId, out var p) && p.Incarnation != "")
            {
                if (p.Incarnation == msg.Incarnation && msg.Sequence <= p.Sequence) return RegisterResult.Replay; // REQ-DIS-5
            }
            p ??= _peers[msg.NodeId] = new PeerInfo { NodeId = msg.NodeId };
            if (!p.IsStatic || true) { p.RestUrl = msg.RestUrl; }
            p.IsStatic = false; p.LastSeenMs = now; p.Incarnation = msg.Incarnation; p.Sequence = msg.Sequence; p.Role = msg.Role; p.Term = msg.Term;
            return RegisterResult.Accepted;
        }
    }

    public void NoteRpcUrl(string nodeId, string url)
    {
        if (string.IsNullOrWhiteSpace(url)) return;
        lock (_l)
        {
            if (!_peers.TryGetValue(nodeId, out var p)) p = _peers[nodeId] = new PeerInfo { NodeId = nodeId, RestUrl = url, LastSeenMs = _clock.MonotonicMs };
            p.RpcUrl = url.TrimEnd('/');
        }
    }

    /// <summary>URL noto di un peer (anche scaduto: serve a ritentare; la freschezza non è una condizione di quorum).</summary>
    public string? GetUrl(string nodeId)
    {
        lock (_l) return _peers.TryGetValue(nodeId, out var p) ? (p.RpcUrl ?? p.RestUrl) : null;
    }

    public List<PeerInfo> GetVisiblePeers()
    {
        lock (_l)
        {
            long now = _clock.MonotonicMs;
            return _peers.Values.Where(p => p.IsStatic || now - p.LastSeenMs <= _timeoutMs).ToList();
        }
    }
}
