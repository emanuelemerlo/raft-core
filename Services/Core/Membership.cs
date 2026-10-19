using RaftCore.Configuration;

namespace RaftCore.Services.Core;

/// <summary>Insieme dei votanti (REQ-CLU-2/3). Static: dalla configurazione. Bootstrap: primi ClusterSize NodeId distinti, poi immutabile.</summary>
public sealed class Membership
{
    private readonly object _l = new();
    private readonly ClusterConfig _cfg;
    private List<string> _members;
    private string _hash;

    public string SelfId { get; }
    public Action<IReadOnlyList<string>>? Persist { get; set; }
    public Membership(ClusterConfig cfg, string selfId, IEnumerable<string>? initial = null)
    {
        _cfg = cfg; SelfId = ClusterConfig.Norm(selfId);
        _members = (cfg.MembershipMode == MembershipMode.Static ? cfg.Members : (initial ?? new List<string>()))
            .Select(ClusterConfig.Norm).Distinct().ToList();
        if (!_members.Contains(SelfId) && cfg.MembershipMode == MembershipMode.Bootstrap) _members.Add(SelfId);
        _hash = ClusterConfig.ComputeMembersHash(_members);
    }
    public bool IsComplete { get { lock (_l) return _members.Count >= _cfg.ClusterSize; } }
    public IReadOnlyList<string> Members { get { lock (_l) return _members.ToList(); } }
    public string Hash { get { lock (_l) return _hash; } }
    public bool Contains(string nodeId) { var n = ClusterConfig.Norm(nodeId); lock (_l) return _members.Contains(n); }
    public IEnumerable<string> Others => Members.Where(m => m != SelfId);

    /// <summary>Solo Bootstrap, solo con HMAC già verificato dal chiamante.</summary>
    public bool TryAdmit(string nodeId)
    {
        if (_cfg.MembershipMode != MembershipMode.Bootstrap) return false;
        var n = ClusterConfig.Norm(nodeId);
        lock (_l)
        {
            if (_members.Count >= _cfg.ClusterSize || _members.Contains(n)) return false;
            _members.Add(n); _hash = ClusterConfig.ComputeMembersHash(_members);
            Persist?.Invoke(_members.ToList());
            return true;
        }
    }
}
