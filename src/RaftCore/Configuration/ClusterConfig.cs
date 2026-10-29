using System.Security.Cryptography;
using System.Text;

namespace RaftCore.Configuration;

public enum MembershipMode { Static, Bootstrap }
public enum UdpMode { Broadcast, Multicast, Disabled }
public enum StateMachineProvider { SharedFile, SqlServer }

/// <summary>Errore di configurazione: elenca tutti i vincoli violati (REQ-CFG-1).</summary>
public sealed class ConfigurationValidationException : Exception
{
    public IReadOnlyList<string> Violations { get; }
    public ConfigurationValidationException(IReadOnlyList<string> violations)
        : base("Configurazione non valida:\n - " + string.Join("\n - ", violations)) => Violations = violations;
}

/// <summary>Sezione "Storage": WAL e stato Raft locali del nodo.</summary>
public class StorageConfig
{
    /// <summary>Directory locale del nodo. Supporta i segnaposto {port} e {nodeId}. Percorsi relativi: vedi ClusterConfig.BaseDirectory.</summary>
    public string DataDir { get; set; } = "./data";
    public bool FsyncOnCommit { get; set; } = true;
    public long MinFreeBytes { get; set; } = 1_073_741_824;
    public bool AllowEmptyState { get; set; } = false;
}

/// <summary>
/// Sezione "Discovery": oltre agli annunci UDP, il nodo sonda gli host noti sulle porte di RestPorts finché trova tutti
/// i membri, e ricomincia quando un membro smette di rispondere. Funziona anche senza broadcast e con porte dinamiche.
/// </summary>
public class DiscoveryConfig
{
    /// <summary>Host su cui cercare gli altri nodi; vuoto = 127.0.0.1. Si aggiungono automaticamente gli host di Peers.</summary>
    public List<string> SeedHosts { get; set; } = new();
    /// <summary>Porte da sondare; vuoto = le stesse di RestPorts/RestPort.</summary>
    public string? ProbePorts { get; set; }
    public bool ProbeEnabled { get; set; } = true;
    public int ProbeIntervalMs { get; set; } = 2000;
    /// <summary>Con tutti i membri raggiungibili si sonda solo ogni ProbeIdleIntervalMs.</summary>
    public int ProbeIdleIntervalMs { get; set; } = 30000;
    public int ProbeTimeoutMs { get; set; } = 500;
    /// <summary>RPC fallite consecutive dopo le quali un membro è considerato irraggiungibile e viene cercato di nuovo.</summary>
    public int UnreachableAfterFailures { get; set; } = 3;
}

/// <summary>
/// Sezione "DevCluster" (solo ambiente Development): il processo avviato (es. con F5 in Visual Studio) diventa il primo
/// nodo e lancia gli altri Members come processi figli non in debug, che terminano insieme a lui.
/// </summary>
public class DevClusterConfig
{
    /// <summary>Numero di nodi da avviare in totale (1 = solo questo processo).</summary>
    public int Instances { get; set; } = 1;
    /// <summary>Impostato dal launcher nei processi figli: PID del padre da sorvegliare.</summary>
    public int ParentPid { get; set; }
    /// <summary>Window (nuova finestra console, solo Windows), Prefix (output nel processo padre con prefisso) o File.</summary>
    public string ChildOutput { get; set; } = "Auto";
}

/// <summary>Sezione "Log": segmentazione e compattazione del WAL (limita disco e RAM).</summary>
public class LogConfig
{
    /// <summary>Dimensione oltre la quale il segmento WAL attivo viene chiuso e se ne apre uno nuovo.</summary>
    public long SegmentBytes { get; set; } = 64L * 1024 * 1024;
    /// <summary>Voci già applicate da mantenere nel log per i Follower in ritardo; oltre si usa InstallSnapshot.</summary>
    public int RetainEntries { get; set; } = 10_000;
    /// <summary>Numero minimo di voci compattabili prima di eseguire una compattazione.</summary>
    public int CompactionBatch { get; set; } = 5_000;
    public int CompactionIntervalMs { get; set; } = 5_000;
}

public class SharedFileConfig
{
    /// <summary>Directory condivisa tra i nodi (solo sviluppo/test sulla stessa macchina o su share).</summary>
    public string Directory { get; set; } = "./shared-state";
    public int LockTimeoutMs { get; set; } = 5_000;
}

public class SqlServerConfig
{
    /// <summary>Stringa di connessione. In produzione da variabile d'ambiente RAFT_SQL_CONNECTION (REQ-SEC-1).</summary>
    public string ConnectionString { get; set; } = string.Empty;
    public string Schema { get; set; } = "raft";
    /// <summary>Tabella applicativa scritta dal writer di default (JsonPayloadWriter).</summary>
    public string DataTable { get; set; } = "raft.IngestedData";
    public int CommandTimeoutSeconds { get; set; } = 30;
    public bool AutoCreateSchema { get; set; } = true;
}

/// <summary>Sezione "StateMachine": database condiviso a cui il Leader applica le voci committed.</summary>
public class StateMachineConfig
{
    public StateMachineProvider Provider { get; set; } = StateMachineProvider.SharedFile;
    /// <summary>Una voce committed oltre questa età (rispetto a ReceivedUtc) NON viene scritta: esito Expired. 0 = disattivato.</summary>
    public int MaxPendingAgeMinutes { get; set; } = 1440;
    /// <summary>Tentativi per un errore non transitorio e non classificato prima della quarantena (poison entry).</summary>
    public int MaxApplyAttempts { get; set; } = 10;
    public int ApplyRetryMaxDelayMs { get; set; } = 5_000;
    public SharedFileConfig SharedFile { get; set; } = new();
    public SqlServerConfig SqlServer { get; set; } = new();
}

/// <summary>Configurazione baseline (spec §18). Sezione "RaftCluster".</summary>
/// <summary>Sezione "Management": esposizione degli endpoint informativi.</summary>
public class ManagementConfig
{
    /// <summary>
    /// Se false (default), GET /api/v1/cluster/status e /api/v1/metrics rispondono solo a chiamanti su loopback;
    /// le richieste da altri indirizzi ricevono 403. Metterlo a true solo quando l'accesso è già ristretto a monte
    /// (rete di gestione, reverse proxy o firewall). Gli endpoint /health/* non sono interessati.
    /// Dietro un reverse proxy l'indirizzo visto è quello del proxy: in quel caso serve comunque true.
    /// </summary>
    public bool ExposeStatusEndpoints { get; set; } = false;
}

public class ClusterConfig
{
    public const long MaxBodyBytesLimit = 64L * 1024 * 1024;

    public string ClusterId { get; set; } = string.Empty;
    public int ClusterSize { get; set; }
    /// <summary>REQ-SEC-1: mai nel repository. Variabile d'ambiente CLUSTER_SECRET o RaftCluster__ClusterSecret.</summary>
    public string ClusterSecret { get; set; } = string.Empty;
    public MembershipMode MembershipMode { get; set; } = MembershipMode.Static;
    public string NodeId { get; set; } = string.Empty;
    public List<string> Members { get; set; } = new();
    public string NodeIdFile { get; set; } = "./data/node-id";

    /// <summary>Base per i percorsi relativi. Vuoto: directory dell'eseguibile se in esecuzione come servizio, altrimenti directory corrente.</summary>
    public string? BaseDirectory { get; set; }

    public int RestPort { get; set; } = 5000;
    /// <summary>
    /// Opzionale: porte da provare in ordine, es. "5001-5005", "5001,5003,5007" o "5001-5005;5010;5012".
    /// Il nodo usa la prima libera; gli altri nodi la trovano con la discovery a sondaggio (Discovery:SeedHosts). Prevale su RestPort.
    /// </summary>
    public string? RestPorts { get; set; }
    /// <summary>Alias di RestPorts (v0.2.0).</summary>
    public string? RestPortRange { get; set; }
    /// <summary>URL pubblicato. Supporta {port}.</summary>
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

    public DiscoveryConfig Discovery { get; set; } = new();
    public DevClusterConfig DevCluster { get; set; } = new();
    public StorageConfig Storage { get; set; } = new();
    public LogConfig Log { get; set; } = new();
    public StateMachineConfig StateMachine { get; set; } = new();
    public ManagementConfig Management { get; set; } = new();

    /// <summary>Solo test: abilita FsyncOnCommit=false e la fault injection su file.</summary>
    public bool TestMode { get; set; } = false;
    /// <summary>URL statici dei peer (seed), NodeId -> URL. Utili senza broadcast; gli URL vengono poi propagati dal Leader.</summary>
    public Dictionary<string, string> Peers { get; set; } = new();

    public int Quorum => ClusterSize / 2 + 1;

    /// <summary>Normalizza un NodeId (GUID) in forma canonica minuscola.</summary>
    public static string Norm(string id) => Guid.TryParse(id, out var g) ? g.ToString("D") : id;

    public static string ComputeMembersHash(IEnumerable<string> members)
    {
        var raw = string.Join(",", members.Select(Norm).OrderBy(m => m, StringComparer.Ordinal));
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(raw))).ToLowerInvariant();
    }

    /// <summary>Applica i segreti da variabili d'ambiente (REQ-SEC-1).</summary>
    public void ApplyEnvironment()
    {
        var env = Environment.GetEnvironmentVariable("CLUSTER_SECRET");
        if (!string.IsNullOrEmpty(env)) ClusterSecret = env;
        var sql = Environment.GetEnvironmentVariable("RAFT_SQL_CONNECTION");
        if (!string.IsNullOrEmpty(sql)) StateMachine.SqlServer.ConnectionString = sql;
    }

    /// <summary>Porte REST da provare, in ordine (RestPorts, poi l'alias RestPortRange, poi RestPort).</summary>
    public IReadOnlyList<int> CandidatePorts()
    {
        var spec = !string.IsNullOrWhiteSpace(RestPorts) ? RestPorts : RestPortRange;
        return string.IsNullOrWhiteSpace(spec) ? new[] { RestPort } : ParsePortSpec(spec!);
    }

    /// <summary>
    /// Elenco di porte in formato intervallo ("5001-5005"), lista ("5001,5003,5007") o misto ("5001-5005;5010;5012"):
    /// separatori ',' e ';', ordine conservato, duplicati ignorati.
    /// </summary>
    public static IReadOnlyList<int> ParsePortSpec(string spec)
    {
        var result = new List<int>();
        var errors = new List<string>();
        foreach (var raw in spec.Split(new[] { ',', ';' }, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            var bounds = raw.Split('-', StringSplitOptions.TrimEntries);
            if (bounds.Length == 1 && int.TryParse(bounds[0], out var p) && p is >= 1 and <= 65535) { if (!result.Contains(p)) result.Add(p); }
            else if (bounds.Length == 2 && int.TryParse(bounds[0], out var a) && int.TryParse(bounds[1], out var b) && a is >= 1 and <= 65535 && b is >= 1 and <= 65535 && a <= b)
            {
                if (b - a > 1000) errors.Add($"intervallo '{raw}' troppo ampio (massimo 1000 porte)");
                else for (int x = a; x <= b; x++) if (!result.Contains(x)) result.Add(x);
            }
            else errors.Add($"elemento '{raw}' non valido");
        }
        if (result.Count == 0 && errors.Count == 0) errors.Add("nessuna porta");
        if (errors.Count > 0)
            throw new ConfigurationValidationException(new[] { $"Elenco di porte non valido '{spec}': {string.Join("; ", errors)}. Formati ammessi: 5001-5005, 5001,5003,5007 oppure 5001-5005;5010;5012." });
        return result;
    }

    /// <summary>Sostituisce i segnaposto e risolve i percorsi relativi. Da chiamare dopo aver scelto la porta.</summary>
    public void ResolvePlaceholdersAndPaths(int port, string baseDir)
    {
        RestPort = port;
        string Sub(string s) => s.Replace("{port}", port.ToString(), StringComparison.OrdinalIgnoreCase)
                                 .Replace("{nodeId}", string.IsNullOrWhiteSpace(NodeId) ? "node" : Norm(NodeId), StringComparison.OrdinalIgnoreCase);
        string Abs(string p) => Path.GetFullPath(Path.IsPathRooted(p) ? p : Path.Combine(baseDir, p));
        if (!string.IsNullOrWhiteSpace(AdvertiseUrl)) AdvertiseUrl = Sub(AdvertiseUrl);
        Storage.DataDir = Abs(Sub(Storage.DataDir));
        NodeIdFile = Abs(Sub(NodeIdFile));
        StateMachine.SharedFile.Directory = Abs(Sub(StateMachine.SharedFile.Directory));
    }

    /// <summary>REQ-CFG-1: valida tutti i vincoli; se uno è violato il nodo NON deve partire.</summary>
    public void Validate()
    {
        // gli override (variabili d'ambiente, riga di comando) non possono accorciare un array: le voci vuote si ignorano
        Members = Members.Where(m => !string.IsNullOrWhiteSpace(m)).ToList();
        foreach (var k in Peers.Where(kv => string.IsNullOrWhiteSpace(kv.Value)).Select(kv => kv.Key).ToList()) Peers.Remove(k);
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
        if (MaxBodyBytes < 1 || MaxBodyBytes > MaxBodyBytesLimit) v.Add($"MaxBodyBytes deve essere tra 1 e {MaxBodyBytesLimit}.");
        if (MaxForwardHops < 1) v.Add("MaxForwardHops deve essere >= 1.");
        try { _ = CandidatePorts(); } catch (ConfigurationValidationException ex) { v.AddRange(ex.Violations); }
        if (!string.IsNullOrWhiteSpace(Discovery.ProbePorts))
            try { _ = ParsePortSpec(Discovery.ProbePorts!); } catch (ConfigurationValidationException ex) { v.AddRange(ex.Violations.Select(x => "Discovery:ProbePorts: " + x)); }
        if (Discovery.ProbeIntervalMs < 200 || Discovery.ProbeIdleIntervalMs < Discovery.ProbeIntervalMs) v.Add("Discovery: ProbeIntervalMs >= 200 e ProbeIdleIntervalMs >= ProbeIntervalMs.");
        if (Discovery.ProbeTimeoutMs < 50) v.Add("Discovery:ProbeTimeoutMs deve essere >= 50.");
        if (DevCluster.Instances < 1 || DevCluster.Instances > 7) v.Add("DevCluster:Instances deve essere tra 1 e 7.");

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

        if (!Storage.FsyncOnCommit && !TestMode)
            v.Add("Storage.FsyncOnCommit=false è consentito solo nei test (REQ-WAL-2).");
        if (Log.SegmentBytes < 1024 * 1024) v.Add("Log.SegmentBytes deve essere >= 1 MiB.");
        if (Log.RetainEntries < 100) v.Add("Log.RetainEntries deve essere >= 100.");
        if (Log.CompactionBatch < 1) v.Add("Log.CompactionBatch deve essere >= 1.");
        if (Log.CompactionIntervalMs < 100) v.Add("Log.CompactionIntervalMs deve essere >= 100.");

        var sm = StateMachine;
        if (sm.MaxPendingAgeMinutes < 0) v.Add("StateMachine.MaxPendingAgeMinutes deve essere >= 0.");
        if (sm.MaxPendingAgeMinutes > 0 && sm.MaxPendingAgeMinutes * 60_000L <= RequestDeadlineMs)
            v.Add("StateMachine.MaxPendingAgeMinutes deve superare RequestDeadlineMs.");
        if (sm.MaxApplyAttempts < 1) v.Add("StateMachine.MaxApplyAttempts deve essere >= 1.");
        if (sm.ApplyRetryMaxDelayMs < 100) v.Add("StateMachine.ApplyRetryMaxDelayMs deve essere >= 100.");
        if (sm.Provider == StateMachineProvider.SqlServer)
        {
            if (string.IsNullOrWhiteSpace(sm.SqlServer.ConnectionString))
                v.Add("StateMachine.SqlServer.ConnectionString obbligatoria (variabile d'ambiente RAFT_SQL_CONNECTION).");
            if (!SqlName.IsValidQualified(sm.SqlServer.DataTable)) v.Add("StateMachine.SqlServer.DataTable non valido (atteso schema.tabella).");
            if (!SqlName.IsValid(sm.SqlServer.Schema)) v.Add("StateMachine.SqlServer.Schema non valido.");
        }

        if (v.Count > 0) throw new ConfigurationValidationException(v);
    }
}

/// <summary>Validazione minima dei nomi SQL configurabili (evita injection nei nomi di schema/tabella).</summary>
public static class SqlName
{
    public static bool IsValid(string s) => !string.IsNullOrEmpty(s) && s.Length <= 128 && s.All(c => char.IsLetterOrDigit(c) || c == '_');
    public static bool IsValidQualified(string s)
    {
        var p = s.Split('.');
        return p.Length == 2 && IsValid(p[0]) && IsValid(p[1]);
    }
    public static string Quote(string qualified) => string.Join(".", qualified.Split('.').Select(x => "[" + x + "]"));
}
