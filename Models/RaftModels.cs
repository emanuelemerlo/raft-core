namespace RaftCore.Models;

public class DiscoveryMessage
{
    public string ClusterId { get; set; } = string.Empty;
    public int ClusterSize { get; set; }
    public string MembersHash { get; set; } = string.Empty;
    public string NodeId { get; set; } = string.Empty;
    public string RestUrl { get; set; } = string.Empty;
    public NodeRole Role { get; set; }
    public long Term { get; set; }
    public string Incarnation { get; set; } = string.Empty;
    public long Sequence { get; set; }
    public string Hmac { get; set; } = string.Empty;
}

public class BaseRpcRequest
{
    public string ClusterId { get; set; } = string.Empty;
    public int ClusterSize { get; set; }
    public string MembersHash { get; set; } = string.Empty;
    public string SenderNodeId { get; set; } = string.Empty;
    /// <summary>URL REST del mittente (preferito all'annuncio UDP, REQ-DIS-5).</summary>
    public string SenderUrl { get; set; } = string.Empty;
}

public class PreVoteRequest : BaseRpcRequest
{
    public long Term { get; set; }
    public string CandidateId { get; set; } = string.Empty;
    public long LastLogIndex { get; set; }
    public long LastLogTerm { get; set; }
}

public class PreVoteResponse
{
    public long Term { get; set; }
    public bool VoteGranted { get; set; }
}

public class VoteRequest : BaseRpcRequest
{
    public long Term { get; set; }
    public string CandidateId { get; set; } = string.Empty;
    public long LastLogIndex { get; set; }
    public long LastLogTerm { get; set; }
}

public class VoteResponse
{
    public long Term { get; set; }
    public bool VoteGranted { get; set; }
}

public class AppendEntriesRequest : BaseRpcRequest
{
    public long Term { get; set; }
    public string LeaderId { get; set; } = string.Empty;
    public long PrevLogIndex { get; set; }
    public long PrevLogTerm { get; set; }
    public List<LogEntry> Entries { get; set; } = new();
    public long LeaderCommit { get; set; }
}

public class AppendEntriesResponse
{
    public long Term { get; set; }
    public bool Success { get; set; }
    public long MatchIndex { get; set; }
    public long ConflictIndex { get; set; }
    public long ConflictTerm { get; set; }
}