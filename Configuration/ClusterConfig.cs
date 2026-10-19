using System.Security.Cryptography;
using System.Text;

namespace RaftCore.Configuration;

public enum MembershipMode { Static, Bootstrap }
public enum UdpMode { Broadcast, Multicast }

/// <summary>Errore di configurazione: elenca tutti i vincoli violati (REQ-CFG-1).</summary>
public sealed class ConfigurationValidationException : Exception
{
    public IReadOnlyList<string> Violations { get; }
    public ConfigurationValidationException(IReadOnlyList<string> violations)
        : base("Configurazione non valida:\n - " + string.Join("\n - ", violations)) => Violations = violations;
}

/// <summary>Sezione "Storage" (spec §18).</summary>
public class StorageConfig
{
    public string DataDir { get; set; } = "./data";
    public string? OutputFilePath { get; set; }
    public bool FsyncOnCommit { get; set; } = true;
    public int IdempotencyRetentionHours { get; set; } = 0;
    public long MinFreeBytes { get; set; } = 1_073_741_824;
    public bool AllowEmptyState { get; set; } = false;
    public long OutputMaxFileBytes { get; set; } = 64L * 1024 * 1024;

    public string ResolvedOutputPath => string.IsNullOrWhiteSpace(OutputFilePath)
        ? Path.Combine(DataDir, "output_data.txt") : OutputFilePath!;
}

/// <summary>Configurazione baseline (spec §18). Sezione "RaftCluster".</summary>
public class ClusterConfig
{
    public string ClusterId { get; set; } = string.Empty;
    public int ClusterSize { get; set; }
    /// <summary>REQ-SEC-1: mai nel repository. Variabile d'ambiente CLUSTER_SECRET o RaftCluster__ClusterSecret.</summary>
    public string ClusterSecret { get; set; } = string.Empty;
    public MembershipMode MembershipMode { get; set; } = MembershipMode.Static;
    public string NodeId { get; set; } = string.Empty;
    public List<string> Members { get; set; } = new();
    public string NodeIdFile { get; set; } = "./data/node-id";

    public int RestPort { get; set; } = 5000;
    public string? AdvertiseUrl { get; set; }
    public string? BindInterface { get; set; }

    public UdpMode UdpMode { get; set; } = UdpMode.Broadcast;
    public string UdpBroadcastAddress { get; set; } = "255.255.255.255";
    public string UdpMulticastAddress { get; set; } = "239.255.42.99";
    public int UdpDiscoveryPort { get; set; } = 8888;
    public int AnnounceIntervalMs { get; set; } = 1000;
    public int AnnounceTimeoutMs { get; set; } = 4000;
    public int DiscoveryPhaseMs { get; set; } = 2000;

    public int HeartbeatIntervalMs { get; set; } = 250;
    public int ElectionTimeoutMinMs { get; set; } = 1000;
    public int ElectionTimeoutMaxMs { get; set; } = 1800;
    public int LeaderLeaseMs { get; set; } = 750;
    public int ReplicationTimeoutMs { get; set; } = 1500;
    public int FailoverWaitMs { get; set; } = 8000;
    public int RequestDeadlineMs { get; set; } = 10000;
    public long MaxBodyBytes { get; set; } = 1_048_576;
    public int MaxForwardHops { get; set; } = 1;
    public int RpcClockToleranceMs { get; set; } = 60000;
    public int ShutdownGraceMs { get; set; } = 5000;
    public int MaxConcurrentRequests { get; set; } = 256;

    public StorageConfig Storage { get; set; } = new();

    /// <summary>Solo test: abilita FsyncOnCommit=false, retention &gt; 0 e fault injection su file.</summary>
    public bool TestMode { get; set; } = false;
    /// <summary>Opzionale (test / reti senza broadcast): URL statici dei peer, NodeId -> URL. La discovery UDP resta il meccanismo standard.</summary>
    public Dictionary<string, string> Peers { get; set; } = new();

    public int Quorum => ClusterSize / 2 + 1;

    /// <summary>Normalizza un NodeId (GUID) in forma canonica minuscola.</summary>
    public static string Norm(string id) => Guid.TryParse(id, out var g) ? g.ToString("D") : id;

    public static string ComputeMembersHash(IEnumerable<string> members)
    {
        var raw = string.Join(",", members.Select(Norm).OrderBy(m => m, StringComparer.Ordinal));
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(raw))).ToLowerInvariant();
    }

    /// <summary>Applica il segreto da variabile d'ambiente (REQ-SEC-1).</summary>
    public void ApplyEnvironment()
    {
        var env = Environment.GetEnvironmentVariable("CLUSTER_SECRET");
        if (!string.IsNullOrEmpty(env)) ClusterSecret = env;
    }

    /// <summary>REQ-CFG-1: valida tutti i vincoli; se uno è violato il nodo NON deve partire.</summary>
    public void Validate()
    {
        var v = new List<string>();
        if (string.IsNullOrWhiteSpace(ClusterId)) v.Add("ClusterId è obbligatorio.");
        if (ClusterSize < 1 || ClusterSize > 7) v.Add($"1 <= ClusterSize <= 7 (valore: {ClusterSize}).");
        if (string.IsNullOrEmpty(ClusterSecret) || ClusterSecret.Length < 16)
            v.Add("ClusterSecret obbligatorio (>= 16 caratteri), da variabile d'ambiente CLUSTER_SECRET o secret store (REQ-SEC-1).");

        if (ElectionTimeoutMinMs < 4 * HeartbeatIntervalMs)
            v.Add($"ElectionTimeoutMinMs ({ElectionTimeoutMinMs}) >= 4 * HeartbeatIntervalMs ({4 * HeartbeatIntervalMs}) violato.");
        if (ElectionTimeoutMaxMs - ElectionTimeoutMinMs < 2 * HeartbeatIntervalMs)
            v.Add($"ElectionTimeoutMaxMs - ElectionTimeoutMinMs ({ElectionTimeoutMaxMs - ElectionTimeoutMinMs}) >= 2 * HeartbeatIntervalMs ({2 * HeartbeatIntervalMs}) violato.");
        if (LeaderLeaseMs < 3 * HeartbeatIntervalMs)
            v.Add($"LeaderLeaseMs ({LeaderLeaseMs}) >= 3 * HeartbeatIntervalMs ({3 * HeartbeatIntervalMs}) violato.");
        if (LeaderLeaseMs >= ElectionTimeoutMinMs)
            v.Add($"LeaderLeaseMs ({LeaderLeaseMs}) < ElectionTimeoutMinMs ({ElectionTimeoutMinMs}) violato.");
        if (ReplicationTimeoutMs >= RequestDeadlineMs)
            v.Add("ReplicationTimeoutMs < RequestDeadlineMs violato.");
        if (FailoverWaitMs >= RequestDeadlineMs)
            v.Add("FailoverWaitMs < RequestDeadlineMs violato.");
        if (AnnounceTimeoutMs < 3 * AnnounceIntervalMs)
            v.Add($"AnnounceTimeoutMs ({AnnounceTimeoutMs}) >= 3 * AnnounceIntervalMs ({3 * AnnounceIntervalMs}) violato.");
        if (MaxConcurrentRequests < 1) v.Add("MaxConcurrentRequests deve essere >= 1.");
        if (MaxBodyBytes < 1) v.Add("MaxBodyBytes deve essere >= 1.");
        if (MaxForwardHops < 1) v.Add("MaxForwardHops deve essere >= 1.");

        if (MembershipMode == MembershipMode.Static)
        {
            if (!Guid.TryParse(NodeId, out _)) v.Add("In modalità Static NodeId deve essere un GUID.");
            var bad = Members.Where(m => !Guid.TryParse(m, out _)).ToList();
            if (bad.Count > 0) v.Add("Members deve contenere solo GUID (non validi: " + string.Join(", ", bad) + ").");
            var norm = Members.Select(Norm).ToList();
            if (norm.Count != ClusterSize || norm.Distinct().Count() != ClusterSize)
                v.Add($"Static: Members deve contenere esattamente ClusterSize ({ClusterSize}) NodeId distinti (trovati {norm.Distinct().Count()}).");
            if (!norm.Contains(Norm(NodeId))) v.Add("Static: Members deve includere il NodeId del nodo.");
        }

        if (Storage.IdempotencyRetentionHours < 0) v.Add("IdempotencyRetentionHours deve essere >= 0.");
        if (Storage.IdempotencyRetentionHours > 0 && !TestMode)
            v.Add("IdempotencyRetentionHours > 0 richiede gli snapshot (non ammesso in produzione in questa revisione).");
        if (!Storage.FsyncOnCommit && !TestMode)
            v.Add("Storage.FsyncOnCommit=false è consentito solo nei test (REQ-WAL-2).");

        if (v.Count > 0) throw new ConfigurationValidationException(v);
    }
}
