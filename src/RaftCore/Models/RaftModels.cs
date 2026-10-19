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
    /// <summary>NodeId del destinatario atteso: il ricevente rifiuta (421) se non coincide con sé stesso (identità del risponditore).</summary>
    public string DestNodeId { get; set; } = string.Empty;
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
    /// <summary>Ultimo indice applicato al database condiviso: consente la compattazione anche sui Follower.</summary>
    public long LeaderApplied { get; set; }
    /// <summary>URL noti dei membri (incluso il Leader): i Follower imparano come raggiungersi tra loro.</summary>
    public Dictionary<string, string> MemberUrls { get; set; } = new();
}

public class AppendEntriesResponse
{
    public long Term { get; set; }
    public bool Success { get; set; }
    public long MatchIndex { get; set; }
    public long ConflictIndex { get; set; }
    public long ConflictTerm { get; set; }
}

/// <summary>
/// InstallSnapshot semplificato: lo stato applicativo vive nel database condiviso, quindi lo snapshot
/// contiene solo (SnapshotIndex, SnapshotTerm). Il Follower scarta il prefisso del log fino a SnapshotIndex.
/// </summary>
public class InstallSnapshotRequest : BaseRpcRequest
{
    public long Term { get; set; }
    public string LeaderId { get; set; } = string.Empty;
    public long SnapshotIndex { get; set; }
    public long SnapshotTerm { get; set; }
    public long LeaderCommit { get; set; }
    public Dictionary<string, string> MemberUrls { get; set; } = new();
}

public class InstallSnapshotResponse
{
    public long Term { get; set; }
    public bool Success { get; set; }
    public long MatchIndex { get; set; }
}

/// <summary>Sondaggio della discovery: "chi risponde su questa porta?". Destinatario jolly, nessun effetto sullo stato.</summary>
public class HelloRequest : BaseRpcRequest { }

public class HelloResponse
{
    public string NodeId { get; set; } = string.Empty;
    public string Url { get; set; } = string.Empty;
    public string Role { get; set; } = string.Empty;
    public long Term { get; set; }
}
