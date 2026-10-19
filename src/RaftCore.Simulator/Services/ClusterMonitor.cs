using System.Text.Json;

namespace RaftCore.Simulator.Services;

public sealed class NodeView
{
    public string NodeId { get; init; } = "";
    public string ShortId => NodeId.Length >= 8 ? NodeId[..8] : NodeId;
    public string Url { get; set; } = "";
    public int Port { get; set; }
    public bool Reachable { get; set; }
    public string Role { get; set; } = "?";
    public long Term { get; set; }
    public string? LeaderId { get; set; }
    public long CommitIndex { get; set; }
    public long LastApplied { get; set; }
    public long LastLogIndex { get; set; }
    public long SnapshotIndex { get; set; }
    public bool Healthy { get; set; }
    public bool StateMachineHealthy { get; set; }
    public string? StateMachineError { get; set; }
    public string? UnhealthyReason { get; set; }
    public bool Ready { get; set; }
    public string? ReadyReason { get; set; }
    public int? Pid { get; set; }
    public bool IsDevParent { get; set; }
    public bool Paused { get; set; }
    public bool KilledBySimulator { get; set; }
    public bool CanRestart { get; set; }
    public long LastSeenAgoMs { get; set; }
    internal DateTime LastSeenUtc { get; set; }
    internal LaunchInfo? Launch { get; set; }

    public NodeView Clone() => (NodeView)MemberwiseClone();
}

/// <summary>
/// Sonda periodicamente host e porte configurati (GET /api/v1/cluster/status e /health/ready), tiene traccia dei nodi
/// visti, del Leader corrente e del PID di ogni nodo. Esegue i guasti a livello di processo (kill, pausa, riavvio).
/// </summary>
public sealed class ClusterMonitor : BackgroundService
{
    private readonly SimulatorOptions _opt;
    private readonly DataClient _client;
    private readonly ProcessControl _proc;
    private readonly EventLog _events;
    private readonly PathResolver _paths;
    private readonly Dictionary<string, NodeView> _nodes = new(StringComparer.OrdinalIgnoreCase);
    private readonly IReadOnlyList<string> _targets;
    private readonly object _l = new();
    private string? _leaderId;
    private long _leaderTerm;
    private bool _anySeen;

    public ClusterMonitor(SimulatorOptions opt, DataClient client, ProcessControl proc, EventLog events, PathResolver paths)
    {
        _opt = opt; _client = client; _proc = proc; _events = events; _paths = paths;
        var ports = PortSpec.Parse(opt.Ports);
        var hosts = opt.Hosts.Count == 0 ? new List<string> { "127.0.0.1" } : opt.Hosts.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        _targets = hosts.SelectMany(h => ports.Select(p => $"http://{h}:{p}")).ToList();
    }

    public IReadOnlyList<string> ScannedTargets => _targets;

    // ------------------------------------------------------------------ viste per l'interfaccia e per il generatore
    public IReadOnlyList<NodeView> Snapshot()
    {
        lock (_l)
        {
            var now = DateTime.UtcNow;
            return _nodes.Values.Select(n => View(n, now)).OrderBy(n => n.IsDevParent ? 0 : 1).ThenBy(n => n.NodeId).ToList();
        }
    }

    /// <summary>Copia per l'esterno, con i campi derivati. Va chiamata sotto lock.</summary>
    private NodeView View(NodeView n, DateTime now)
    {
        var c = n.Clone();
        c.LastSeenAgoMs = (long)(now - n.LastSeenUtc).TotalMilliseconds;
        c.Paused = n.Pid is int pid && _proc.IsPaused(pid);
        // Il nodo 1 del cluster di sviluppo (F5, sotto debugger) è il processo indicato come DevCluster:ParentPid dagli
        // altri nodi. Un nodo avviato da solo (profili "Solo nodo N" o riga di comando) non è padre di nessuno.
        c.IsDevParent = n.Pid is int self && _nodes.Values.Any(o => !ReferenceEquals(o, n) && o.Launch?.ParentPid == self);
        c.CanRestart = !n.Reachable && n.Launch != null && !c.IsDevParent && !IsAlive(n.Pid)
                       && (n.Launch.ParentPid is not int parent || IsAlive(parent));
        return c;
    }

    public IReadOnlyList<NodeView> Reachable() => Snapshot().Where(n => n.Reachable).OrderBy(n => n.Port).ToList();

    public NodeView? Leader()
    {
        lock (_l)
            return _nodes.Values.Where(n => n.Reachable && n.Role == "Leader").OrderByDescending(n => n.Term).FirstOrDefault()?.Clone();
    }

    public NodeView? Find(string nodeId) { lock (_l) return _nodes.TryGetValue(nodeId, out var n) ? View(n, DateTime.UtcNow) : null; }

    // ------------------------------------------------------------------ sondaggio
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var started = DateTime.UtcNow;
            try { await PollOnceAsync(ct); } catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex) { _events.Warn("Errore nel sondaggio del cluster: " + ex.Message); }
            var wait = TimeSpan.FromMilliseconds(_opt.StatusPollMs) - (DateTime.UtcNow - started);
            if (wait > TimeSpan.Zero) await Task.Delay(wait, ct);
        }
    }

    public async Task PollOnceAsync(CancellationToken ct)
    {
        var results = await Task.WhenAll(_targets.Select(t => ProbeAsync(t, ct)));
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var newPids = new List<NodeView>();
        lock (_l)
        {
            foreach (var r in results.Where(r => r != null))
            {
                var s = r!;
                // la stessa istanza può rispondere su più URL solo per errore di configurazione: vince il primo
                if (!seen.Add(s.NodeId)) continue;
                if (!_nodes.TryGetValue(s.NodeId, out var n))
                {
                    n = new NodeView { NodeId = s.NodeId };
                    _nodes[s.NodeId] = n;
                    _events.Info($"Nodo {n.ShortId} trovato su {s.Url}");
                }
                else if (!n.Reachable) _events.Ok($"Nodo {n.ShortId} di nuovo raggiungibile su {s.Url}");
                if (n.Port != s.Port) n.Pid = null;                                   // porta cambiata: PID da rileggere
                n.Url = s.Url; n.Port = s.Port; n.Reachable = true; n.LastSeenUtc = DateTime.UtcNow;
                n.Role = s.Role; n.Term = s.Term; n.LeaderId = s.LeaderId; n.CommitIndex = s.CommitIndex; n.LastApplied = s.LastApplied;
                n.LastLogIndex = s.LastLogIndex; n.SnapshotIndex = s.SnapshotIndex; n.Healthy = s.Healthy;
                n.StateMachineHealthy = s.StateMachineHealthy; n.StateMachineError = s.StateMachineError; n.UnhealthyReason = s.UnhealthyReason;
                n.Ready = s.Ready; n.ReadyReason = s.ReadyReason; n.KilledBySimulator = false;
                if (n.Pid == null || !IsAlive(n.Pid)) newPids.Add(n);
            }
            foreach (var n in _nodes.Values.Where(n => n.Reachable && !seen.Contains(n.NodeId)))
            {
                n.Reachable = false;
                bool paused = n.Pid is int pid && _proc.IsPaused(pid);
                if (!paused && !n.KilledBySimulator) _events.Warn($"Nodo {n.ShortId} non risponde più ({n.Url})");
            }
            _anySeen |= seen.Count > 0;
            TrackLeader();
        }
        // PID e riga di comando fuori dal lock (WMI su Windows può richiedere centinaia di ms)
        foreach (var n in newPids)
        {
            var pid = _proc.FindListeningPid(n.Port);
            LaunchInfo? info = pid is int p ? _proc.GetLaunchInfo(p, _paths.Resolve(_opt.NodeWorkingDirectory)) : null;
            lock (_l)
            {
                if (!_nodes.TryGetValue(n.NodeId, out var live)) continue;
                live.Pid = pid;
                if (info != null) live.Launch = info;
            }
        }
    }

    private void TrackLeader()
    {
        var leader = _nodes.Values.Where(n => n.Reachable && n.Role == "Leader").OrderByDescending(n => n.Term).FirstOrDefault();
        if (leader == null)
        {
            if (_leaderId != null && _anySeen) _events.Warn("Nessun Leader visibile");
            _leaderId = null;
            return;
        }
        if (!string.Equals(leader.NodeId, _leaderId, StringComparison.OrdinalIgnoreCase) || leader.Term != _leaderTerm)
        {
            _events.Ok($"Leader: nodo {leader.ShortId} (porta {leader.Port}, term {leader.Term})");
            _leaderId = leader.NodeId; _leaderTerm = leader.Term;
        }
    }

    private sealed record Probe(string NodeId, string Url, int Port, string Role, long Term, string? LeaderId, long CommitIndex, long LastApplied,
        long LastLogIndex, long SnapshotIndex, bool Healthy, bool StateMachineHealthy, string? StateMachineError, string? UnhealthyReason,
        bool Ready, string? ReadyReason);

    private async Task<Probe?> ProbeAsync(string baseUrl, CancellationToken ct)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(_opt.StatusTimeoutMs);
        try
        {
            using var res = await _client.Http.GetAsync(baseUrl + "/api/v1/cluster/status", cts.Token);
            if (!res.IsSuccessStatusCode) return null;
            using var doc = JsonDocument.Parse(await res.Content.ReadAsStringAsync(cts.Token));
            var r = doc.RootElement;
            string? Str(string name) => r.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;
            long Num(string name) => r.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.Number ? v.GetInt64() : 0;
            bool Bool(string name) => r.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.True;
            var nodeId = Str("nodeId");
            if (string.IsNullOrEmpty(nodeId)) return null;

            bool ready = false; string? reason = null;
            try
            {
                using var hr = await _client.Http.GetAsync(baseUrl + "/health/ready", cts.Token);
                ready = hr.IsSuccessStatusCode;
                using var hd = JsonDocument.Parse(await hr.Content.ReadAsStringAsync(cts.Token));
                if (hd.RootElement.TryGetProperty("reason", out var rv)) reason = rv.GetString();
            }
            catch (Exception) when (!ct.IsCancellationRequested) { }

            return new Probe(nodeId, baseUrl, new Uri(baseUrl).Port, Str("role") ?? "?", Num("term"), Str("leaderId"), Num("commitIndex"),
                Num("lastApplied"), Num("lastLogIndex"), Num("snapshotIndex"), Bool("healthy"), Bool("stateMachineHealthy"),
                Str("stateMachineError"), Str("unhealthyReason"), ready, reason);
        }
        catch (Exception) when (!ct.IsCancellationRequested) { return null; }
    }

    private static bool IsAlive(int? pid)
    {
        if (pid is not int p) return false;
        try { using var proc = System.Diagnostics.Process.GetProcessById(p); return !proc.HasExited; }
        catch { return false; }
    }

    // ------------------------------------------------------------------ guasti
    public string Kill(string nodeId)
    {
        var (n, pid) = RequireRunning(nodeId);
        _proc.Kill(pid);
        lock (_l)
        {
            if (_nodes.TryGetValue(nodeId, out var live)) { live.KilledBySimulator = true; live.Reachable = false; }
        }
        var msg = $"Nodo {n.ShortId} (porta {n.Port}, {n.Role}) terminato con kill immediato (PID {pid})";
        _events.Error(msg);
        if (n.IsDevParent)
            _events.Warn("Era il nodo 1 del cluster di sviluppo (sotto debugger): anche gli altri nodi si chiudono. Riavviare con F5.");
        return msg;
    }

    public string Pause(string nodeId, int seconds)
    {
        var (n, pid) = RequireRunning(nodeId);
        seconds = Math.Clamp(seconds, 1, 300);
        _proc.Pause(pid, seconds, () => _events.Info($"Nodo {n.ShortId} ripreso dopo {seconds} s di pausa"));
        var msg = $"Nodo {n.ShortId} (porta {n.Port}, {n.Role}) in pausa per {seconds} s (PID {pid})";
        _events.Warn(msg);
        return msg;
    }

    public string Resume(string nodeId)
    {
        var n = Find(nodeId) ?? throw new SimulatorException($"Nodo {nodeId} sconosciuto");
        if (n.Pid is not int pid || !_proc.IsPaused(pid)) throw new SimulatorException("Il nodo non è in pausa");
        _proc.Resume(pid);
        var msg = $"Nodo {n.ShortId} ripreso";
        _events.Info(msg);
        return msg;
    }

    public string Restart(string nodeId)
    {
        LaunchInfo? info; NodeView n;
        lock (_l)
        {
            if (!_nodes.TryGetValue(nodeId, out var live)) throw new SimulatorException($"Nodo {nodeId} sconosciuto");
            n = View(live, DateTime.UtcNow); info = live.Launch;
        }
        if (n.Reachable || IsAlive(n.Pid)) throw new SimulatorException("Il nodo è ancora in esecuzione");
        if (n.IsDevParent) throw new SimulatorException("Il nodo 1 del cluster di sviluppo si riavvia dalla sessione di debug (F5)");
        if (info == null) throw new SimulatorException("Riga di comando del nodo non disponibile: riavviarlo a mano");
        if (info.ParentPid is int parent && !IsAlive(parent))
            throw new SimulatorException("Il nodo 1 del cluster di sviluppo è terminato: riavviare il cluster con F5");
        foreach (var kv in _opt.NodeEnvironment)
            if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(kv.Key))) Environment.SetEnvironmentVariable(kv.Key, kv.Value);
        var p = _proc.Start(info);
        lock (_l) { if (_nodes.TryGetValue(nodeId, out var live)) { live.Pid = p.Id; live.KilledBySimulator = false; } }
        var msg = $"Nodo {n.ShortId} riavviato (PID {p.Id}): si riallinea dal Leader";
        _events.Info(msg);
        return msg;
    }

    private (NodeView Node, int Pid) RequireRunning(string nodeId)
    {
        var n = Find(nodeId) ?? throw new SimulatorException($"Nodo {nodeId} sconosciuto");
        int? pid = n.Pid ?? (n.Port > 0 ? _proc.FindListeningPid(n.Port) : null);
        if (pid is not int p || !IsAlive(p)) throw new SimulatorException($"PID del nodo {n.ShortId} non individuato");
        var info = n.Launch ?? _proc.GetLaunchInfo(p, _paths.Resolve(_opt.NodeWorkingDirectory));
        if (!_proc.IsRaftNode(p, info)) throw new SimulatorException($"Il processo {p} sulla porta {n.Port} non è un nodo RaftCore: azione rifiutata");
        lock (_l)
        {
            if (_nodes.TryGetValue(nodeId, out var live))
            {
                live.Pid = p;
                if (live.Launch == null && info != null) live.Launch = info;
            }
        }
        return (n, p);
    }
}

public sealed class SimulatorException : Exception
{
    public SimulatorException(string message) : base(message) { }
}
