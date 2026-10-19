using RaftCore.Abstractions;
using RaftCore.Configuration;
using RaftCore.Models;
using RaftCore.Services.Common;
using RaftCore.Services.Network;
using RaftCore.Services.Storage;

namespace RaftCore.Services.Core;

public sealed record WriteResult(int Status, byte[] Body, bool Replay = false, bool NotLeader = false)
{
    public static readonly WriteResult NotLeaderResult = new(0, Array.Empty<byte>(), false, true);
    public static WriteResult From(ApplyOutcome o) => new(o.HttpStatus, o.Body, o.Replay || o.Status == ApplyStatus.Duplicate);
}
public sealed class StartupBlockedException : Exception { public StartupBlockedException(string m) : base(m) { } }
public sealed class PeerProgress
{
    public long Next = 1, Match = 0, LastAckSendMs = long.MinValue / 2;
    public readonly SemaphoreSlim Signal = new(0, 1);
}
public sealed record IdemRec(string Hash, long Index);
internal sealed record PendingWrite(long Index, long Term, string RequestId, string Hash, byte[] Payload, long ReceivedTicks);

/// <summary>
/// Sottoinsieme rigoroso di Raft: elezione (PreVote obbligatorio, leader stickiness), replica del log, commit,
/// lease del Leader, compattazione con InstallSnapshot. La state machine è un database CONDIVISO: la applica solo il
/// Leader, in modo condizionale su LastApplied (vedi IDataRepository). Tutti i timer usano IClock monotono (REQ-LDR-5).
/// Lo stato è protetto da un unico lock (_mu) mai tenuto durante una chiamata di rete o un accesso al database.
/// Ordine dei lock: _mu, poi il lock di scrittura del WAL, poi il lock interno del WAL.
/// </summary>
public sealed class RaftNode : BackgroundService
{
    private const int TickMs = 10;
    private readonly ClusterConfig _cfg;
    private readonly Membership _members;
    private readonly PeerRegistry _peers;
    private readonly IPeerTransport _tx;
    private readonly IClock _clock;
    private readonly Metrics _metrics;
    private readonly IDataRepository _repo;
    private readonly ILogger<RaftNode> _log;
    private readonly WalLog _wal;
    private readonly RaftStateStore _store;
    private readonly CommitIndexFile _commitFile;
    private readonly string _walDir;
    private readonly Random _rnd = new();
    private readonly object _mu = new();
    private readonly string _self;
    private readonly LogThrottle _throttle = new(TimeSpan.FromSeconds(30));
    private string _selfUrl = "";

    // stato Raft
    private NodeRole _role = NodeRole.Discovery;
    private long _term;
    private string? _votedFor;
    private long _commit;
    /// <summary>Leader: cursore del database condiviso (LastApplied). Follower: ultimo LastApplied comunicato dal Leader.</summary>
    private long _lastApplied;
    private string? _leaderId;
    private long _lastAppendMs = long.MinValue / 2;
    private long _electionDeadlineMs;
    private long _discoveryEndMs;
    private bool _electionRunning, _recovered, _recoveryRequired, _ioFailed, _storageLow, _draining;
    private string? _unhealthyReason;
    private long _lastStorageCheckMs, _lastCommitPersistMs;
    private bool _alarm70, _alarm85;

    // stato Leader
    private readonly Dictionary<string, PeerProgress> _progress = new();
    private CancellationTokenSource? _leaderCts;
    private readonly Dictionary<long, TaskCompletionSource<ApplyOutcome?>> _waiters = new();
    private readonly List<PendingWrite> _pending = new();
    private long _nextIndex;
    private readonly SemaphoreSlim _writerSignal = new(0, int.MaxValue);
    // state machine (solo Leader)
    private bool _applyCursorKnown, _repoInitialized;
    private bool _smHealthy = true;
    private string? _smError;
    // indice di idempotenza della finestra di log non compattata (REQ-IDM-2); oltre, decide il database
    private readonly Dictionary<string, IdemRec> _idem = new();

    private TaskCompletionSource _leaderTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly SemaphoreSlim _applySignal = new(0, int.MaxValue);
    public SemaphoreSlim AnnounceNow { get; } = new(0, 1);

    public RaftNode(ClusterConfig cfg, Membership members, PeerRegistry peers, IPeerTransport tx, IClock clock, Metrics metrics,
        IDataRepository repo, ILogger<RaftNode> log)
    {
        _cfg = cfg; _members = members; _peers = peers; _tx = tx; _clock = clock; _metrics = metrics; _repo = repo; _log = log;
        _self = members.SelfId;
        var dir = Path.GetFullPath(cfg.Storage.DataDir);
        Directory.CreateDirectory(dir);
        _store = new RaftStateStore(dir);
        _commitFile = new CommitIndexFile(dir);
        _walDir = Path.Combine(dir, "wal");
        MigrateLegacyWal(dir, _walDir);
        var inject = Path.Combine(dir, ".inject_io_error");
        _wal = new WalLog(_walDir, cfg.Storage.FsyncOnCommit, cfg.Log.SegmentBytes, () => cfg.TestMode && File.Exists(inject), metrics.ObserveFsync);
        members.Persist = _ => { lock (_mu) { try { SaveState(); } catch (Exception ex) { _log.LogError("Persistenza members fallita: {M}", ex.Message); } } };
    }

    /// <summary>v0.1 usava un unico file raft.wal con lo stesso formato dei record: diventa il primo segmento.</summary>
    private static void MigrateLegacyWal(string dataDir, string walDir)
    {
        var legacy = Path.Combine(dataDir, "raft.wal");
        if (!File.Exists(legacy)) return;
        Directory.CreateDirectory(walDir);
        if (Directory.GetFiles(walDir, "*.wal").Length > 0) return;
        File.Move(legacy, Path.Combine(walDir, "00000001.wal"));
        Durability.SyncDirectory(walDir);
        Durability.SyncDirectory(dataDir);
    }

    // ===================================================================== recovery (REQ-WAL-3, REQ-ID-3)
    public void Initialize()
    {
        lock (_mu)
        {
            _selfUrl = NetworkHelper.ResolveAdvertiseUrl(_cfg);
            bool stateExists = _store.Exists;
            var open = _wal.Open();                                           // 1-2) validazione e troncamento del record finale incompleto
            if (open.TruncatedBytes > 0) _log.LogWarning("Recovery: troncati {B} byte di record finale incompleto", open.TruncatedBytes);
            if (open.CorruptNonFinal) { SetRecoveryRequired(open.Message!); return; }   // 3)

            if (!stateExists && !open.HadData)
            {
                if (!_cfg.Storage.AllowEmptyState)
                {
                    throw new StartupBlockedException("REQ-ID-3: stato persistente assente (WAL e stato Raft) e Storage.AllowEmptyState=false: il nodo NON parte. " +
                        "Se è il primo deploy o un nuovo NodeId impostare AllowEmptyState=true e rimuoverlo subito dopo.");
                }
                SaveState();
            }
            else if (!stateExists) { SetRecoveryRequired("Stato Raft (term/voto) assente ma WAL presente: richiesta recovery esplicita."); return; }
            else
            {
                PersistentRaftState st;
                try { st = _store.Load(); }
                catch (Exception ex) { SetRecoveryRequired("raft_state.json illeggibile: " + ex.Message); return; }
                if (st.NodeId != null && ClusterConfig.Norm(st.NodeId) != _self)
                { SetRecoveryRequired($"La DataDir appartiene al NodeId {st.NodeId}, non a {_self}."); return; }
                _term = st.CurrentTerm; _votedFor = st.VotedFor;
            }

            long persistedCommit = _commitFile.Read();
            if (persistedCommit > _wal.LastIndex)
            { SetRecoveryRequired($"commitIndex persistito ({persistedCommit}) oltre la fine del WAL ({_wal.LastIndex}): WAL perso o troncato."); return; }
            _commit = Math.Max(persistedCommit, _wal.SnapshotIndex);           // 5) limite inferiore
            _lastApplied = _wal.SnapshotIndex;
            RebuildIdem();                                                    // 4) indice delle richieste (finestra non compattata)
            _discoveryEndMs = _clock.MonotonicMs + _cfg.DiscoveryPhaseMs;
            _recovered = true;                                                // REQ-WAL-4
            _log.LogInformation("Recovery completato: term={Term} snapshot={Snap} lastIndex={Last} commitIndex={Commit} segmenti={Seg}",
                _term, _wal.SnapshotIndex, _wal.LastIndex, _commit, _wal.SegmentCount);
        }
    }

    private void SetRecoveryRequired(string why)
    {
        _recoveryRequired = true; _unhealthyReason = why;
        _log.LogCritical("EVENTO ALTA SEVERITÀ: {Why} Il nodo non vota e non accetta voci (unhealthy).", why);
    }

    // ===================================================================== helper di stato (sotto _mu)
    private string Ctx => $"[Term={_term} Role={_role}]";

    private void SaveState() => _store.Save(new PersistentRaftState { CurrentTerm = _term, VotedFor = _votedFor, Members = _members.Members.ToList(), NodeId = _self });

    private void PersistCommit(long now, bool force = false)
    {
        if (!force && now - _lastCommitPersistMs <= 100) return;
        _lastCommitPersistMs = now;
        try { _commitFile.Write(_commit); } catch (IOException) { /* limite inferiore: non critico */ }
    }

    /// <summary>REQ-ID-5: lo stato è durevole PRIMA di aggiornare la memoria e quindi di rispondere.</summary>
    private void PersistTermVote(long term, string? vote)
    {
        try { _store.Save(new PersistentRaftState { CurrentTerm = term, VotedFor = vote, Members = _members.Members.ToList(), NodeId = _self }); }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { MarkIoFailure(ex); throw new WalIoException(ex.Message, ex); }
        _term = term; _votedFor = vote;
    }

    private void MarkIoFailure(Exception ex)
    {
        _ioFailed = true; _unhealthyReason = "Errore di I/O: " + ex.Message;
        _metrics.Inc("walWriteErrors");
        _log.LogCritical("EVENTO ALTA SEVERITÀ {Ctx}: storage non affidabile ({Msg}); nodo unhealthy", Ctx, ex.Message);
        if (_role == NodeRole.Leader) StepDownInternal("errore di storage (REQ-WAL-5)");
    }

    private bool StorageHealthy => !_ioFailed && !_storageLow && !_recoveryRequired && !_wal.Failed;
    private bool CanParticipate => _recovered && StorageHealthy && _members.IsComplete && _role != NodeRole.Discovery;

    private void ResetElectionTimer(long now) =>
        _electionDeadlineMs = now + _rnd.Next(_cfg.ElectionTimeoutMinMs, _cfg.ElectionTimeoutMaxMs + 1);

    private void SetRole(NodeRole r)
    {
        if (_role == r) return;
        var old = _role;
        _role = r;
        if (old != NodeRole.Discovery || r != NodeRole.Follower)
            _log.LogInformation("Ruolo {Old} -> {New} (term {Term})", old, r, _term);
        if (AnnounceNow.CurrentCount == 0) try { AnnounceNow.Release(); } catch (SemaphoreFullException) { }
        NotifyLeaderChange();
    }

    private void NotifyLeaderChange() => Interlocked.Exchange(ref _leaderTcs, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).TrySetResult();
    public Task LeaderChanged => _leaderTcs.Task;

    private void StepDownInternal(string reason)
    {
        bool wasLeader = _role == NodeRole.Leader;
        if (wasLeader)
        {
            _metrics.Inc("stepDownCount");
            _log.LogWarning("{Ctx} step down: {Reason}", Ctx, reason);
            _leaderCts?.Cancel(); _leaderCts = null;
            FailBatch(_pending.ToList());                                   // richieste non ancora scritte: prenotazioni rimosse
            _pending.Clear();
            foreach (var w in _waiters.Values) w.TrySetResult(null);       // REQ-STA-1: 503 al client
            _waiters.Clear();
            _progress.Clear();
            _applyCursorKnown = false;
        }
        if (_role != NodeRole.Discovery) SetRole(NodeRole.Follower);
        if (wasLeader) { _leaderId = null; ResetElectionTimer(_clock.MonotonicMs); NotifyLeaderChange(); }
    }

    private void AdoptHigherTerm(long term)
    {
        if (term <= _term) return;
        PersistTermVote(term, null);
        _leaderId = null;
        if (_role == NodeRole.Leader || _role == NodeRole.Candidate) StepDownInternal("term maggiore");
    }

    private bool LeaseValid(long now)
    {
        if (_role != NodeRole.Leader) return false;
        int n = 1; foreach (var p in _progress.Values) if (now - p.LastAckSendMs <= _cfg.LeaderLeaseMs) n++;  // REQ-LDR-3
        return n >= _cfg.Quorum;
    }

    // ===================================================================== loop principali
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _ = Task.Run(() => ApplyLoopAsync(ct), ct);
        _ = Task.Run(() => WriterLoopAsync(ct), ct);
        _ = Task.Run(() => MaintenanceLoopAsync(ct), ct);
        while (!ct.IsCancellationRequested)
        {
            try { Tick(ct); } catch (Exception ex) { _log.LogError(ex, "Errore nel tick"); }
            try { await _clock.Delay(TickMs, ct); } catch (OperationCanceledException) { break; }
        }
    }

    private void Tick(CancellationToken ct)
    {
        bool startElection = false;
        lock (_mu)
        {
            if (!_recovered) return;
            long now = _clock.MonotonicMs;
            if (now - _lastStorageCheckMs >= 1000) { _lastStorageCheckMs = now; CheckStorage(); }
            switch (_role)
            {
                case NodeRole.Discovery:
                    if (_cfg.ClusterSize == 1 && StorageHealthy) { try { PersistTermVote(_term + 1, _self); BecomeLeader(now, new HashSet<string>()); } catch (IOException) { } }
                    else if (now >= _discoveryEndMs) { SetRole(NodeRole.Follower); ResetElectionTimer(now); }
                    break;
                case NodeRole.Follower:
                case NodeRole.Candidate:
                    if (!_electionRunning && now >= _electionDeadlineMs && CanParticipate) { _electionRunning = true; startElection = true; }
                    break;
                case NodeRole.Leader:
                    if (!StorageHealthy) StepDownInternal("storage non sano");
                    else if (!LeaseValid(now)) StepDownInternal("lease scaduto (REQ-LDR-4)");
                    break;
            }
        }
        if (startElection) _ = Task.Run(() => RunElectionAsync(ct), ct);
    }

    private void CheckStorage()
    {
        try
        {
            var root = Path.GetPathRoot(Path.GetFullPath(_cfg.Storage.DataDir))!;
            var di = new DriveInfo(root);
            long avail = di.AvailableFreeSpace, total = di.TotalSize;
            _metrics.Set("storageAvailableBytes", avail);
            double occ = total > 0 ? 1.0 - (double)avail / total : 0;                       // REQ-SNP-3
            if (occ >= 0.85) { if (!_alarm85) { _alarm85 = _alarm70 = true; _log.LogError("ALLARME: occupazione disco {Occ:P0} (>=85%)", occ); } }
            else if (occ >= 0.70) { _alarm85 = false; if (!_alarm70) { _alarm70 = true; _log.LogWarning("ALLARME: occupazione disco {Occ:P0} (>=70%)", occ); } }
            else { _alarm70 = false; _alarm85 = false; }
            bool low = avail < _cfg.Storage.MinFreeBytes;
            if (low && !_storageLow)
            {
                _storageLow = true; _unhealthyReason = $"Spazio libero {avail} < MinFreeBytes {_cfg.Storage.MinFreeBytes}";
                _log.LogCritical("EVENTO ALTA SEVERITÀ: {R}", _unhealthyReason);
                if (_role == NodeRole.Leader) StepDownInternal("spazio sotto MinFreeBytes (REQ-WAL-5)");
            }
            else if (!low && _storageLow) { _storageLow = false; _log.LogWarning("Spazio libero ripristinato"); }
        }
        catch { /* volume non interrogabile: nessuna decisione */ }
    }

    // ===================================================================== elezione (§6, §7)
    private async Task RunElectionAsync(CancellationToken ct)
    {
        try
        {
            long term0, startMs;
            lock (_mu)
            {
                if ((_role != NodeRole.Follower && _role != NodeRole.Candidate) || !CanParticipate) return;
                term0 = _term; startMs = _clock.MonotonicMs;
            }
            // --- PreVote (REQ-VOT-1): non modifica term né votedFor
            var pvGranted = await AskPeersAsync(async peer =>
            {
                var req = NewRpc<PreVoteRequest>(); req.Term = term0 + 1; req.CandidateId = _self;
                req.LastLogIndex = _wal.LastIndex; req.LastLogTerm = _wal.LastTerm;
                var r = await _tx.SendAsync<PreVoteRequest, PreVoteResponse>(peer, "/internal/v1/prevote", req, RpcTimeout, ct);
                if (r.Ok && r.Body!.Term > term0) ObserveHigherTerm(r.Body.Term);
                return r.Ok && r.Body!.VoteGranted;
            });
            if (1 + pvGranted.Count < _cfg.Quorum)
            {
                _log.LogDebug("PreVote non accolto: {N}/{Q} (term {T})", 1 + pvGranted.Count, _cfg.Quorum, term0 + 1);
                lock (_mu) ResetElectionTimer(_clock.MonotonicMs);
                return;
            }

            // --- incremento del term, voto per sé, persistenza (REQ-VOT-3)
            long term, sendMs;
            lock (_mu)
            {
                if ((_role != NodeRole.Follower && _role != NodeRole.Candidate) || _term != term0 || _lastAppendMs >= startMs || !CanParticipate)
                { ResetElectionTimer(_clock.MonotonicMs); return; }
                try { PersistTermVote(_term + 1, _self); } catch (IOException) { return; }
                SetRole(NodeRole.Candidate); _leaderId = null;
                _metrics.Inc("electionCount");
                ResetElectionTimer(_clock.MonotonicMs);
                term = _term; sendMs = _clock.MonotonicMs;
            }
            var voters = await AskPeersAsync(async peer =>
            {
                var req = NewRpc<VoteRequest>(); req.Term = term; req.CandidateId = _self;
                req.LastLogIndex = _wal.LastIndex; req.LastLogTerm = _wal.LastTerm;
                var r = await _tx.SendAsync<VoteRequest, VoteResponse>(peer, "/internal/v1/vote", req, RpcTimeout, ct);
                if (r.Ok && r.Body!.Term > term) ObserveHigherTerm(r.Body.Term);
                return r.Ok && r.Body!.VoteGranted;
            });
            lock (_mu)
            {
                if (_role == NodeRole.Candidate && _term == term && 1 + voters.Count >= _cfg.Quorum && StorageHealthy)
                    BecomeLeader(sendMs, new HashSet<string>(voters));
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) { _log.LogError(ex, "Errore durante l'elezione"); }
        finally { lock (_mu) _electionRunning = false; }
    }

    private TimeSpan RpcTimeout => TimeSpan.FromMilliseconds(Math.Max(100, Math.Min(_cfg.ReplicationTimeoutMs, _cfg.ElectionTimeoutMinMs / 2)));

    private async Task<List<string>> AskPeersAsync(Func<PeerEndpoint, Task<bool>> ask)
    {
        var tasks = new List<(string id, Task<bool> t)>();
        foreach (var id in _members.Others)
        {
            var url = _peers.GetUrl(id);
            if (url == null)
            {
                // Causa tipica di un nodo "bloccato in PreVote": non sa come raggiungere gli altri membri
                _metrics.Inc("peerUrlUnknown");
                if (_throttle.ShouldLog("nourl:" + id))
                    _log.LogWarning("Nessun URL noto per il membro {Peer}: impossibile chiedergli il voto. Verificare la discovery UDP (UdpMode) o configurare Peers.", id);
                continue;
            }
            tasks.Add((id, Task.Run(() => ask(new PeerEndpoint(id, url)))));
        }
        // Si procede appena si raggiunge il quorum, senza attendere i membri lenti o spenti: su Windows una connessione verso
        // una porta chiusa arriva al timeout invece di essere rifiutata subito, e attendere tutti allungava ogni round
        // (PreVote + voto) fino a provocare candidature simultanee e voti contesi in serie dopo la caduta del Leader.
        // Le risposte successive vengono comunque elaborate (term più alto osservato) dai task, che proseguono.
        int needed = _cfg.Quorum - 1;
        var granted = new List<string>();
        var pending = tasks.ToDictionary(x => (Task)x.t, x => x.id);
        while (pending.Count > 0 && granted.Count < needed)
        {
            var done = await Task.WhenAny(pending.Keys);
            var id = pending[done]; pending.Remove(done);
            try { if (await (Task<bool>)done) granted.Add(id); } catch { }
        }
        return granted;
    }

    private T NewRpc<T>() where T : BaseRpcRequest, new() =>
        new() { ClusterId = _cfg.ClusterId, ClusterSize = _cfg.ClusterSize, MembersHash = _members.Hash, SenderNodeId = _self, SenderUrl = _selfUrl };

    private void ObserveHigherTerm(long term)
    {
        lock (_mu) { try { AdoptHigherTerm(term); } catch (IOException) { } }
    }

    private void BecomeLeader(long sendMs, HashSet<string> granters)
    {
        SetRole(NodeRole.Leader); _leaderId = _self;
        _log.LogInformation("{Ctx} eletto Leader", Ctx);
        _progress.Clear(); _pending.Clear();
        long last = _wal.LastIndex;
        foreach (var id in _members.Others)
            _progress[id] = new PeerProgress { Next = last + 1, Match = 0, LastAckSendMs = granters.Contains(id) ? sendMs : long.MinValue / 2 };
        try
        {   // REQ-LDR-1: no-op nel term corrente
            _wal.Append(0, new[] { new WalInput(last + 1, _term, LogEntryType.NoOp, Guid.NewGuid().ToString(), "", _clock.UtcNow.Ticks, default) });
        }
        catch (IOException ex) { MarkIoFailure(ex); return; }
        _nextIndex = _wal.LastIndex + 1;
        _applyCursorKnown = false;
        _leaderCts = new CancellationTokenSource();
        var ct = _leaderCts.Token; long term = _term;
        foreach (var (id, prog) in _progress) { var i = id; var p = prog; _ = Task.Run(() => ReplicateLoopAsync(i, p, term, ct), ct); }
        if (_cfg.Quorum == 1) AdvanceCommit();
        _applySignal.Release();
        NotifyLeaderChange();
    }

    // ===================================================================== replica (§9) e InstallSnapshot
    private async Task ReplicateLoopAsync(string peerId, PeerProgress prog, long term, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            bool ok = false;
            try { ok = await SendAppendAsync(peerId, prog, term, ct); }
            catch (OperationCanceledException) { break; }
            catch (Exception ex) { _log.LogDebug("Replica verso {Peer}: {M}", peerId, ex.Message); }
            bool behind; lock (_mu) behind = _role == NodeRole.Leader && _term == term && prog.Next <= _wal.LastIndex;
            if (ok && behind) continue;
            try { await prog.Signal.WaitAsync(_cfg.HeartbeatIntervalMs, ct); } catch (OperationCanceledException) { break; }
        }
    }

    private void SignalReplicators() { foreach (var p in _progress.Values) if (p.Signal.CurrentCount == 0) try { p.Signal.Release(); } catch (SemaphoreFullException) { } }

    private async Task<bool> SendAppendAsync(string peerId, PeerProgress prog, long term, CancellationToken ct)
    {
        var url = _peers.GetUrl(peerId);
        if (url == null)
        {
            if (_throttle.ShouldLog("nourl-rep:" + peerId)) _log.LogWarning("Nessun URL noto per il Follower {Peer}: replica sospesa", peerId);
            return false;
        }
        long prev, prevTerm, last, commit, applied, sendMs, snapIndex, snapTerm;
        Dictionary<string, string> urls;
        lock (_mu)
        {
            if (_role != NodeRole.Leader || _term != term) return false;
            prev = prog.Next - 1; snapIndex = _wal.SnapshotIndex; snapTerm = _wal.SnapshotTerm;
            prevTerm = _wal.TermAt(prev); last = _wal.LastIndex; commit = _commit; applied = _applyCursorKnown ? _lastApplied : 0;
            sendMs = _clock.MonotonicMs;
            urls = _peers.KnownUrls(_members.Others); urls[_self] = _selfUrl;
        }
        if (prev < snapIndex) return await SendSnapshotAsync(peerId, url, prog, term, snapIndex, snapTerm, commit, sendMs, urls, ct);

        var entries = new List<LogEntry>(); long bytes = 0;
        for (long i = prev + 1; i <= last && entries.Count < 256 && bytes < 4 * 1024 * 1024; i++)
        {
            var e = _wal.ReadEntry(i); if (e == null) break;
            entries.Add(e); bytes += e.Payload.Length + 128;
        }
        var req = NewRpc<AppendEntriesRequest>();
        req.Term = term; req.LeaderId = _self; req.PrevLogIndex = prev; req.PrevLogTerm = prevTerm; req.Entries = entries;
        req.LeaderCommit = commit; req.LeaderApplied = applied; req.MemberUrls = urls;
        var res = await _tx.SendAsync<AppendEntriesRequest, AppendEntriesResponse>(new PeerEndpoint(peerId, url), "/internal/v1/append", req,
            TimeSpan.FromMilliseconds(_cfg.ReplicationTimeoutMs), ct);
        if (!res.Ok) return false;
        var r = res.Body!;
        lock (_mu)
        {
            if (r.Term > _term) { try { AdoptHigherTerm(r.Term); } catch (IOException) { } return false; }
            if (_role != NodeRole.Leader || _term != term) return false;
            if (sendMs > prog.LastAckSendMs) prog.LastAckSendMs = sendMs;             // REQ-LDR-3: istante di INVIO
            if (r.Success)
            {
                if (r.MatchIndex > prog.Match) prog.Match = r.MatchIndex;
                prog.Next = Math.Max(prog.Next, prog.Match + 1);
                AdvanceCommit();
                return true;
            }
            long next = r.ConflictIndex > 0 ? r.ConflictIndex : prog.Next - 1;
            if (r.ConflictTerm > 0) { long li = _wal.LastIndexOfTerm(r.ConflictTerm); if (li > 0) next = li + 1; }
            prog.Next = Math.Max(1, Math.Min(next, prog.Next - 1));
            return true;
        }
    }

    private async Task<bool> SendSnapshotAsync(string peerId, string url, PeerProgress prog, long term, long snapIndex, long snapTerm, long commit,
        long sendMs, Dictionary<string, string> urls, CancellationToken ct)
    {
        var req = NewRpc<InstallSnapshotRequest>();
        req.Term = term; req.LeaderId = _self; req.SnapshotIndex = snapIndex; req.SnapshotTerm = snapTerm; req.LeaderCommit = commit; req.MemberUrls = urls;
        var res = await _tx.SendAsync<InstallSnapshotRequest, InstallSnapshotResponse>(new PeerEndpoint(peerId, url), "/internal/v1/snapshot", req,
            TimeSpan.FromMilliseconds(_cfg.ReplicationTimeoutMs), ct);
        if (!res.Ok) return false;
        var r = res.Body!;
        lock (_mu)
        {
            if (r.Term > _term) { try { AdoptHigherTerm(r.Term); } catch (IOException) { } return false; }
            if (_role != NodeRole.Leader || _term != term) return false;
            if (sendMs > prog.LastAckSendMs) prog.LastAckSendMs = sendMs;
            if (!r.Success) return false;
            _metrics.Inc("snapshotsSent");
            _log.LogInformation("InstallSnapshot inviato a {Peer}: snapshot {Idx} (term {T})", peerId, snapIndex, snapTerm);
            if (r.MatchIndex > prog.Match) prog.Match = r.MatchIndex;
            prog.Next = Math.Max(prog.Next, prog.Match + 1);
            AdvanceCommit();
            return true;
        }
    }

    /// <summary>REQ-REP-5 / REQ-LDR-2: commit solo di voci del term corrente replicate (e durevoli) su un quorum.</summary>
    private void AdvanceCommit()
    {
        var m = new List<long> { _wal.LastIndex };
        foreach (var p in _progress.Values) m.Add(p.Match);
        m.Sort(); m.Reverse();
        long cand = m[_cfg.Quorum - 1];
        if (cand > _commit && _wal.TermAt(cand) == _term)
        {
            _commit = cand;
            PersistCommit(_clock.MonotonicMs);
            _applySignal.Release();
        }
    }

    // ===================================================================== group commit del Leader (fsync fuori da _mu)
    private async Task WriterLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try { await _writerSignal.WaitAsync(ct); } catch (OperationCanceledException) { return; }
            List<PendingWrite> batch; long gen;
            lock (_mu)
            {
                if (_pending.Count == 0) continue;
                batch = _pending.ToList(); _pending.Clear();
                gen = _wal.Generation;
                if (_role != NodeRole.Leader) { FailBatch(batch); continue; }
            }
            // eventuali segnali accumulati per lo stesso batch
            while (_writerSignal.CurrentCount > 0 && _writerSignal.Wait(0)) { }
            var inputs = batch.Select(p => new WalInput(p.Index, p.Term, LogEntryType.Data, p.RequestId, p.Hash, p.ReceivedTicks, p.Payload)).ToList();
            bool ok = false; Exception? err = null;
            try { ok = _wal.TryAppend(gen, inputs); }
            catch (Exception ex) { err = ex; }
            lock (_mu)
            {
                if (ok)
                {
                    _metrics.Inc("writeBatches");
                    if (_role == NodeRole.Leader) { if (_cfg.Quorum == 1) AdvanceCommit(); SignalReplicators(); }
                }
                else
                {
                    if (err is IOException io) MarkIoFailure(io);
                    else if (err != null) _log.LogError(err, "Scrittura del batch fallita");
                    FailBatch(batch);
                    // le richieste accodate dopo il batch hanno indici non più validi: falliscono anch'esse (il client ritenta)
                    FailBatch(_pending.ToList()); _pending.Clear();
                    if (_role == NodeRole.Leader) _nextIndex = _wal.LastIndex + 1;
                }
            }
        }
    }

    /// <summary>Sotto _mu: il batch non è stato scritto. Le richieste ricevono 503 e il client ritenta con lo stesso RequestId.</summary>
    private void FailBatch(List<PendingWrite> batch)
    {
        foreach (var p in batch)
        {
            if (_waiters.Remove(p.Index, out var w)) w.TrySetResult(null);
            if (_idem.TryGetValue(p.RequestId, out var rec) && rec.Index == p.Index && _wal.GetMeta(p.Index)?.RequestId != p.RequestId)
                _idem.Remove(p.RequestId);
        }
    }

    // ===================================================================== apply sul database condiviso (solo Leader)
    private ApplyContext NewApplyContext() =>
        new(_clock.UtcNow, TimeSpan.FromMinutes(_cfg.StateMachine.MaxPendingAgeMinutes), _cfg.ClusterId);

    private int Backoff(int attempt) =>
        Math.Min(_cfg.StateMachine.ApplyRetryMaxDelayMs, 100 * (1 << Math.Min(attempt, 10))) + _rnd.Next(0, 50);

    private void StateMachineError(Exception ex, string what)
    {
        lock (_mu) { _smHealthy = false; _smError = $"{what}: {ex.Message}"; }
        if (_throttle.ShouldLog("sm:" + what))
            _log.LogError("Database condiviso non disponibile ({What}): {Msg}. Le scritture restano in attesa.", what, ex.Message);
    }

    private async Task ApplyLoopAsync(CancellationToken ct)
    {
        long errIndex = -1; int attempts = 0, transient = 0;
        while (!ct.IsCancellationRequested)
        {
            try { await _applySignal.WaitAsync(200, ct); } catch (OperationCanceledException) { return; }
            long term; bool leader, cursorKnown;
            lock (_mu) { leader = _role == NodeRole.Leader && _recovered; term = _term; cursorKnown = _applyCursorKnown; }
            if (!leader) continue;
            try
            {
                if (!_repoInitialized) { await _repo.InitializeAsync(ct); _repoInitialized = true; }
                if (!cursorKnown)
                {
                    long db = await _repo.GetLastAppliedIndexAsync(ct);
                    lock (_mu)
                    {
                        if (_role != NodeRole.Leader || _term != term) continue;
                        _lastApplied = db; _applyCursorKnown = true; _smHealthy = true; _smError = null;
                        if (db < _wal.SnapshotIndex)
                            _log.LogCritical("EVENTO ALTA SEVERITÀ: LastApplied del database ({Db}) precede l'inizio del log compattato ({Snap}). " +
                                "Il database è stato ripristinato da un backup? Le voci mancanti non sono più nel log.", db, _wal.SnapshotIndex);
                    }
                    await ResolveAppliedWaitersAsync(ct);
                }
            }
            catch (OperationCanceledException) { return; }
            catch (Exception ex) { StateMachineError(ex, "lettura di LastApplied"); await DelaySafe(Backoff(transient++), ct); _applySignal.Release(); continue; }

            while (!ct.IsCancellationRequested)
            {
                LogEntry? e;
                lock (_mu)
                {
                    if (_role != NodeRole.Leader || _term != term || !_applyCursorKnown || _lastApplied >= _commit) break;
                    e = _wal.ReadEntry(_lastApplied + 1);
                }
                if (e == null) break;
                ApplyOutcome o;
                try
                {
                    o = await _repo.ApplyAsync(e, NewApplyContext(), ct);
                    transient = 0;
                    lock (_mu) { _smHealthy = true; _smError = null; }
                }
                catch (OperationCanceledException) { return; }
                catch (ApplyGapException g)
                {
                    _log.LogWarning("Cursore di apply riallineato al database: {Msg}", g.Message);
                    lock (_mu) _lastApplied = g.DatabaseLastApplied;
                    await ResolveAppliedWaitersAsync(ct);
                    continue;
                }
                catch (Exception ex)
                {
                    if (_repo.IsTransient(ex))
                    {
                        _metrics.Inc("applyTransientErrors");
                        StateMachineError(ex, $"apply della voce {e.Index}");
                        await DelaySafe(Backoff(transient++), ct);
                        continue;                                                    // errore transitorio: si ritenta senza limite
                    }
                    if (errIndex != e.Index) { errIndex = e.Index; attempts = 0; }
                    attempts++;
                    _metrics.Inc("applyErrors");
                    _log.LogError("Apply della voce {Idx} fallito (tentativo {N}/{Max}): {Msg}", e.Index, attempts, _cfg.StateMachine.MaxApplyAttempts, ex.Message);
                    if (attempts < _cfg.StateMachine.MaxApplyAttempts) { await DelaySafe(Backoff(attempts), ct); continue; }
                    try
                    {
                        // Poison entry: registrata come Quarantined nel database, LastApplied avanza, il cluster riparte
                        o = await _repo.QuarantineAsync(e, ex.GetType().Name + ": " + ex.Message, NewApplyContext(), ct);
                        _metrics.Inc("applyQuarantined");
                        _log.LogCritical("EVENTO ALTA SEVERITÀ: voce {Idx} (RequestId {Rid}) messa in quarantena dopo {N} tentativi: {Msg}",
                            e.Index, e.RequestId, attempts, ex.Message);
                    }
                    catch (OperationCanceledException) { return; }
                    catch (Exception qex) { StateMachineError(qex, "quarantena"); await DelaySafe(Backoff(transient++), ct); continue; }
                }
                errIndex = -1; attempts = 0;
                CountOutcome(o);
                lock (_mu)
                {
                    if (e.Index > _lastApplied) _lastApplied = e.Index;
                    if (_waiters.Remove(e.Index, out var w)) w.TrySetResult(o);
                }
            }
        }
    }

    private void CountOutcome(ApplyOutcome o)
    {
        switch (o.Status)
        {
            case ApplyStatus.Rejected: _metrics.Inc("applyRejected"); break;
            case ApplyStatus.Expired:
                _metrics.Inc("applyExpired");
                _log.LogWarning("Voce {Idx} scaduta (oltre MaxPendingAge): non scritta", o.LogIndex);
                break;
        }
    }

    /// <summary>Risolve le attese su voci già applicate da un Leader precedente, leggendo l'esito dal database.</summary>
    private async Task ResolveAppliedWaitersAsync(CancellationToken ct)
    {
        List<(long Index, TaskCompletionSource<ApplyOutcome?> W)> done;
        lock (_mu) done = _waiters.Where(kv => kv.Key <= _lastApplied).Select(kv => (kv.Key, kv.Value)).ToList();
        foreach (var (idx, w) in done)
        {
            var e = _wal.ReadEntry(idx);
            ApplyOutcome? o = null;
            try { if (e != null && e.EntryType == LogEntryType.Data) o = await _repo.GetOutcomeAsync(e.RequestId, ct); }
            catch (Exception ex) when (ex is not OperationCanceledException) { StateMachineError(ex, "lettura esito"); }
            lock (_mu) _waiters.Remove(idx);
            w.TrySetResult(o);
        }
    }

    private static async Task DelaySafe(int ms, CancellationToken ct)
    {
        try { await Task.Delay(ms, ct); } catch (OperationCanceledException) { }
    }

    // ===================================================================== compattazione (punto 7)
    private async Task MaintenanceLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await DelaySafe(_cfg.Log.CompactionIntervalMs, ct);
            try { CompactIfNeeded(); }
            catch (Exception ex) { _log.LogError(ex, "Compattazione fallita"); }
        }
    }

    internal void CompactIfNeeded()
    {
        long upTo;
        lock (_mu)
        {
            if (!_recovered || !StorageHealthy) return;
            long applied = _role == NodeRole.Leader ? (_applyCursorKnown ? _lastApplied : 0) : _lastApplied;
            upTo = Math.Min(applied, _commit) - _cfg.Log.RetainEntries;
            if (upTo - _wal.SnapshotIndex < _cfg.Log.CompactionBatch) return;
        }
        int removed = _wal.Compact(upTo);
        if (removed == 0) return;
        lock (_mu)
        {
            foreach (var k in _idem.Where(kv => kv.Value.Index <= upTo).Select(kv => kv.Key).ToList()) _idem.Remove(k);
        }
        _metrics.Inc("compactions");
        _log.LogInformation("Log compattato fino all'indice {Idx}: {N} voci rimosse, segmenti residui {S}", upTo, removed, _wal.SegmentCount);
    }

    // ===================================================================== scrittura client (REQ-PRX-9, §12)
    public async Task<WriteResult> WriteAsync(string requestId, string payloadHash, ReadOnlyMemory<byte> payload, long deadlineMs, CancellationToken ct)
    {
        TaskCompletionSource<ApplyOutcome?>? waiter = null; bool fromDb = false;
        lock (_mu)
        {
            long now = _clock.MonotonicMs;
            if (_role != NodeRole.Leader) return WriteResult.NotLeaderResult;
            if (_draining || !StorageHealthy || !LeaseValid(now)) return new WriteResult(503, Array.Empty<byte>());   // REQ-LDR-4, OPS-5
            if (_idem.TryGetValue(requestId, out var rec))
            {
                if (rec.Hash != payloadHash) return WriteResult.From(Outcomes.Conflict(rec.Index, requestId));
                if (_applyCursorKnown && rec.Index <= _lastApplied) fromDb = true;             // già applicata: esito dal database
                else waiter = GetWaiter(rec.Index);                                             // Pending: stessa voce, nessuna seconda voce
            }
            else
            {
                long idx = _nextIndex++;
                _pending.Add(new PendingWrite(idx, _term, requestId, payloadHash, payload.ToArray(), _clock.UtcNow.Ticks));
                _idem[requestId] = new IdemRec(payloadHash, idx);
                waiter = GetWaiter(idx);
                _writerSignal.Release();
            }
        }
        if (fromDb)
        {
            var stored = await _repo.GetOutcomeAsync(requestId, ct);
            if (stored == null) return new WriteResult(503, Array.Empty<byte>());
            _metrics.Inc("requestDuplicateCount");
            return WriteResult.From(stored.AsReplay());
        }
        int wait = (int)Math.Max(1, deadlineMs - _clock.MonotonicMs);
        var done = await Task.WhenAny(waiter!.Task, Task.Delay(wait, ct));
        if (done != waiter.Task) return new WriteResult(504, Array.Empty<byte>());   // esito non osservato entro il deadline: il client ritenta
        var o = await waiter.Task;
        if (o == null) return new WriteResult(503, Array.Empty<byte>());              // leadership persa prima dell'esito
        var r = WriteResult.From(o);
        if (r.Replay) _metrics.Inc("requestDuplicateCount");
        return r;
    }

    private TaskCompletionSource<ApplyOutcome?> GetWaiter(long idx)
    {
        if (!_waiters.TryGetValue(idx, out var w)) _waiters[idx] = w = new(TaskCreationOptions.RunContinuationsAsynchronously);
        return w;
    }

    private void IndexEntry(WalMeta m, long index)
    {
        if (m.Type != LogEntryType.Data) return;
        if (!_idem.ContainsKey(m.RequestId)) _idem[m.RequestId] = new IdemRec(m.PayloadHash, index);
    }

    private void RebuildIdem()
    {
        _idem.Clear();
        for (long i = _wal.FirstIndex; i <= _wal.LastIndex; i++) { var m = _wal.GetMeta(i); if (m != null) IndexEntry(m, i); }
    }

    // ===================================================================== RPC: PreVote / Vote / Append / Snapshot (lato ricevente)
    public PreVoteResponse HandlePreVote(PreVoteRequest r)
    {
        lock (_mu)
        {
            long now = _clock.MonotonicMs;
            bool grant = CanParticipate && _members.Contains(r.CandidateId)
                && ClusterConfig.Norm(r.CandidateId) == ClusterConfig.Norm(r.SenderNodeId)
                && r.Term > _term                                                            // (b)
                && LogUpToDate(r.LastLogTerm, r.LastLogIndex)                                // (c)
                && now - _lastAppendMs >= _cfg.ElectionTimeoutMinMs                          // (d) leader stickiness
                && !(_role == NodeRole.Leader && LeaseValid(now));
            if (!grant) _metrics.Inc("prevoteDenied");
            return new PreVoteResponse { Term = _term, VoteGranted = grant };
        }
    }

    public VoteResponse HandleVote(VoteRequest r)
    {
        lock (_mu)
        {
            long now = _clock.MonotonicMs;
            var deny = new Func<VoteResponse>(() => new VoteResponse { Term = _term, VoteGranted = false });
            if (!CanParticipate || !_members.Contains(r.CandidateId) || ClusterConfig.Norm(r.CandidateId) != ClusterConfig.Norm(r.SenderNodeId)) return deny();
            // REQ-VOT-5: ignora senza aggiornare il term se il Leader è vivo
            if (r.Term > _term && (now - _lastAppendMs < _cfg.ElectionTimeoutMinMs || (_role == NodeRole.Leader && LeaseValid(now)))) return deny();
            if (r.Term < _term) return deny();
            try
            {
                if (r.Term > _term) AdoptHigherTerm(r.Term);
                var cand = ClusterConfig.Norm(r.CandidateId);
                bool can = _votedFor == null || _votedFor == cand;
                if (can && LogUpToDate(r.LastLogTerm, r.LastLogIndex))
                {
                    PersistTermVote(_term, cand);                                            // voto valido solo dopo la persistenza
                    ResetElectionTimer(now);
                    return new VoteResponse { Term = _term, VoteGranted = true };
                }
            }
            catch (IOException) { }
            return deny();
        }
    }

    private bool LogUpToDate(long lastTerm, long lastIndex)
    {
        long myTerm = _wal.LastTerm, myIdx = _wal.LastIndex;
        return lastTerm > myTerm || (lastTerm == myTerm && lastIndex >= myIdx);
    }

    /// <summary>Verifiche comuni ad AppendEntries e InstallSnapshot: term, ruolo, Leader noto, timer. null = procedere.</summary>
    private long? AcceptLeader(long term, string leaderId, Dictionary<string, string> urls, long now)
    {
        if (term < _term) return _term;
        if (term > _term) AdoptHigherTerm(term);                                             // persiste prima di rispondere
        else if (_role == NodeRole.Leader) return _term;
        if (_role != NodeRole.Follower) { if (_role == NodeRole.Leader) StepDownInternal("AppendEntries valido"); SetRole(NodeRole.Follower); }
        var lid = ClusterConfig.Norm(leaderId);
        if (_leaderId != lid) { _leaderId = lid; NotifyLeaderChange(); _log.LogInformation("Leader corrente: {Leader} (term {Term})", lid, _term); }
        _lastAppendMs = now; ResetElectionTimer(now);                                        // REQ-REP-7
        _peers.NoteGossip(urls, _self);
        return null;
    }

    /// <summary>Status 200 con risposta; 503 se lo storage non è affidabile; 400 se malformata.</summary>
    public (int Status, AppendEntriesResponse? Resp) HandleAppend(AppendEntriesRequest r)
    {
        lock (_mu)
        {
            if (!_recovered || !StorageHealthy) return (503, null);                          // REQ-WAL-5: nessuna conferma
            if (ClusterConfig.Norm(r.LeaderId) != ClusterConfig.Norm(r.SenderNodeId) || !_members.Contains(r.LeaderId)) return (400, null);
            long now = _clock.MonotonicMs;
            try
            {
                var reject = AcceptLeader(r.Term, r.LeaderId, r.MemberUrls, now);
                if (reject != null) return (200, new AppendEntriesResponse { Term = reject.Value, Success = false });

                using var writeLock = _wal.AcquireWrite();                                   // stato del log stabile fino all'append
                long snap = _wal.SnapshotIndex, last = _wal.LastIndex;
                long prev = r.PrevLogIndex;
                var entries = r.Entries;
                for (int k = 0; k < entries.Count; k++) if (entries[k].Index != prev + 1 + k) return (400, null);
                long lastNew = prev + entries.Count;                                         // ultima voce verificata coerente

                if (prev < snap)
                {
                    // le voci fino allo snapshot sono committed e identiche a quelle del Leader: si salta il prefisso
                    int skip = (int)Math.Min(entries.Count, snap - prev);
                    entries = entries.Skip(skip).ToList();
                    prev = snap;
                }
                else if (prev > last)
                    return (200, new AppendEntriesResponse { Term = _term, Success = false, ConflictIndex = last + 1, ConflictTerm = 0 });
                else if (prev > 0)
                {
                    long pt = _wal.TermAt(prev);
                    if (pt != r.PrevLogTerm)
                        return (200, new AppendEntriesResponse { Term = _term, Success = false, ConflictTerm = pt, ConflictIndex = Math.Max(_wal.FirstIndexOfTerm(pt), snap + 1) });
                }

                long truncateFrom = 0; var toAppend = new List<WalInput>();
                foreach (var e in entries)
                {
                    if (truncateFrom == 0 && e.Index <= last)
                    {
                        if (_wal.TermAt(e.Index) == e.Term) continue;                        // già presente
                        if (e.Index <= _commit) { _log.LogCritical("{Ctx} tentativo di sovrascrivere la voce committed {I} (REQ-REP-4)", Ctx, e.Index); return (200, new AppendEntriesResponse { Term = _term, Success = false }); }
                        truncateFrom = e.Index;                                              // elimina la voce in conflitto e TUTTE le successive
                    }
                    toAppend.Add(new WalInput(e.Index, e.Term, e.EntryType, e.RequestId, e.PayloadHash, e.ReceivedUtc.Ticks, e.Payload));
                }
                if (truncateFrom > 0 || toAppend.Count > 0)
                {
                    _wal.AppendLocked(truncateFrom, toAppend);                               // fsync prima di rispondere (REQ-REP-3.6)
                    if (truncateFrom > 0) RebuildIdem();
                    else foreach (var w in toAppend) IndexEntry(_wal.GetMeta(w.Index)!, w.Index);
                }
                lastNew = Math.Max(lastNew, snap);
                long nc = Math.Min(r.LeaderCommit, lastNew);                                 // REQ-REP-3.8
                if (nc > _commit) { _commit = nc; PersistCommit(now); }
                _lastApplied = Math.Max(_lastApplied, Math.Min(r.LeaderApplied, _commit));
                return (200, new AppendEntriesResponse { Term = _term, Success = true, MatchIndex = lastNew });
            }
            catch (IOException ex) { MarkIoFailure(ex); return (503, null); }
        }
    }

    public (int Status, InstallSnapshotResponse? Resp) HandleInstallSnapshot(InstallSnapshotRequest r)
    {
        lock (_mu)
        {
            if (!_recovered || !StorageHealthy) return (503, null);
            if (ClusterConfig.Norm(r.LeaderId) != ClusterConfig.Norm(r.SenderNodeId) || !_members.Contains(r.LeaderId) || r.SnapshotIndex < 1) return (400, null);
            long now = _clock.MonotonicMs;
            try
            {
                var reject = AcceptLeader(r.Term, r.LeaderId, r.MemberUrls, now);
                if (reject != null) return (200, new InstallSnapshotResponse { Term = reject.Value, Success = false });
                if (r.SnapshotIndex > _wal.SnapshotIndex)
                {
                    if (_wal.TermAt(r.SnapshotIndex) == r.SnapshotTerm)
                        _wal.Compact(r.SnapshotIndex);                                       // il suffisso coerente resta
                    else
                    {
                        _wal.ResetToSnapshot(r.SnapshotIndex, r.SnapshotTerm);               // log non coerente: si riparte dallo snapshot
                        RebuildIdem();
                    }
                    foreach (var k in _idem.Where(kv => kv.Value.Index <= r.SnapshotIndex).Select(kv => kv.Key).ToList()) _idem.Remove(k);
                    _metrics.Inc("snapshotsInstalled");
                    _log.LogInformation("InstallSnapshot ricevuto: log riallineato allo snapshot {Idx} (term {T})", r.SnapshotIndex, r.SnapshotTerm);
                }
                long nc = Math.Min(r.LeaderCommit, Math.Max(r.SnapshotIndex, _wal.SnapshotIndex));
                if (nc > _commit) { _commit = nc; PersistCommit(now, force: true); }
                _lastApplied = Math.Max(_lastApplied, _wal.SnapshotIndex);
                return (200, new InstallSnapshotResponse { Term = _term, Success = true, MatchIndex = r.SnapshotIndex });
            }
            catch (IOException ex) { MarkIoFailure(ex); return (503, null); }
        }
    }

    // ===================================================================== osservabilità / stato
    public (NodeRole Role, long Term) GetRoleAndTerm() { lock (_mu) return (_role, _term); }

    public (string Id, string Url)? GetKnownLeader()
    {
        lock (_mu)
        {
            if (_role == NodeRole.Leader) return (_self, _selfUrl);
            if (_leaderId == null || _clock.MonotonicMs - _lastAppendMs > _cfg.ElectionTimeoutMaxMs) return null;
            var url = _peers.GetUrl(_leaderId);
            return url == null ? null : (_leaderId, url);
        }
    }

    public bool IsLeader { get { lock (_mu) return _role == NodeRole.Leader; } }
    public string SelfId => _self;
    public string SelfUrl { get { lock (_mu) return _selfUrl; } }

    public (bool Ready, string Reason) GetReadiness()
    {
        lock (_mu)
        {
            if (!_recovered) return (false, _unhealthyReason ?? "recovery non completato");
            if (!StorageHealthy) return (false, _unhealthyReason ?? "storage non sano");
            if (_draining) return (false, "shutdown in corso");
            long now = _clock.MonotonicMs;
            if (_role == NodeRole.Leader)
            {
                if (!LeaseValid(now)) return (false, "lease non valido");
                return _smHealthy ? (true, "leader con lease valido") : (false, "database condiviso non disponibile: " + _smError);
            }
            if (_role == NodeRole.Follower) return (_leaderId != null && now - _lastAppendMs <= _cfg.ElectionTimeoutMaxMs) ? (true, "follower con leader noto") : (false, "nessun leader noto");
            return (false, $"ruolo {_role}");
        }
    }

    public object GetStatus()
    {
        lock (_mu)
        {
            var g = _metrics;
            g.Set("currentTerm", _term); g.Set("role", (long)_role); g.Set("commitIndex", _commit); g.Set("lastApplied", _lastApplied);
            g.Set("logLength", _wal.LastIndex); g.Set("snapshotIndex", _wal.SnapshotIndex); g.Set("logWindowEntries", _wal.WindowCount);
            g.Set("walSegments", _wal.SegmentCount); g.Set("idempotencyWindowEntries", _idem.Count);
            g.Set("applyLag", _role == NodeRole.Leader && _applyCursorKnown ? Math.Max(0, _commit - _lastApplied) : 0);
            g.Set("replicationLag", _role == NodeRole.Leader && _progress.Count > 0 ? _progress.Values.Max(p => _wal.LastIndex - p.Match) : 0);
            return new
            {
                nodeId = _self, url = _selfUrl, role = _role.ToString(), term = _term, leaderId = _leaderId, commitIndex = _commit, lastApplied = _lastApplied,
                snapshotIndex = _wal.SnapshotIndex, lastLogIndex = _wal.LastIndex, membersHash = _members.Hash, healthy = StorageHealthy,
                unhealthyReason = _unhealthyReason, stateMachineHealthy = _smHealthy, stateMachineError = _smError,
                peers = _peers.GetVisiblePeers().Select(p => new { p.NodeId, Url = p.BestUrl, p.RpcUrl, p.UdpUrl, p.StaticUrl, p.GossipUrl, Role = p.Role.ToString(), p.Term }).ToList()
            };
        }
    }

    public Dictionary<string, long> GetMetrics() { GetStatus(); return _metrics.Snapshot(); }

    /// <summary>Per i test.</summary>
    internal (long Term, long Commit, long Applied, long Last, long Snapshot, bool Unhealthy, NodeRole Role) Probe()
    { lock (_mu) return (_term, _commit, _lastApplied, _wal.LastIndex, _wal.SnapshotIndex, !StorageHealthy, _role); }
    internal WalLog Wal => _wal;

    // ===================================================================== shutdown (REQ-OPS-5)
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        lock (_mu) _draining = true;                                                        // 1) nuove scritture: 503
        long end = _clock.MonotonicMs + _cfg.ShutdownGraceMs;                               // 2) completa le operazioni in corso
        while (_clock.MonotonicMs < end) { lock (_mu) { if (_waiters.Count == 0 && _pending.Count == 0) break; } await Task.Delay(25, CancellationToken.None); }
        lock (_mu)
        {
            // smette di inviare heartbeat: i Follower eleggono subito un nuovo Leader
            _leaderCts?.Cancel(); _leaderCts = null;
            PersistCommit(_clock.MonotonicMs, force: true);
        }
        await base.StopAsync(cancellationToken);
        _wal.Dispose();
        _commitFile.Dispose();
    }
}
