using System.Collections.Concurrent;
using RaftCore.Abstractions;
using RaftCore.Configuration;
using RaftCore.Models;
using RaftCore.Services.Common;
using RaftCore.Services.Network;
using RaftCore.Services.Storage;

namespace RaftCore.Services.Core;

public sealed record ApplyOutcome(int Status, byte[] Body, bool Replay);
public sealed record WriteResult(int Status, byte[] Body, bool Replay = false, bool NotLeader = false)
{
    public static readonly WriteResult NotLeaderResult = new(0, Array.Empty<byte>(), false, true);
}
public sealed class StartupBlockedException : Exception { public StartupBlockedException(string m) : base(m) { } }
public sealed class PeerProgress
{
    public long Next = 1, Match = 0, LastAckSendMs = long.MinValue / 2;
    public readonly SemaphoreSlim Signal = new(0, 1);
}
public sealed record IdemRec(string Hash, long FirstIndex, long ReceivedTicks);

/// <summary>
/// Sottoinsieme rigoroso di Raft: elezione (PreVote obbligatorio, leader stickiness), replica del log, commit, apply,
/// lease del Leader, idempotenza derivata dal log. Tutti i timer usano IClock monotono (REQ-LDR-5).
/// Lo stato è protetto da un unico lock (_mu) che non viene mai tenuto durante una chiamata di rete.
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
    private readonly string _walPath;
    private readonly Random _rnd = new();
    private readonly object _mu = new();
    private readonly string _self;
    private string _selfUrl = "";

    // stato Raft
    private NodeRole _role = NodeRole.Discovery;
    private long _term;
    private string? _votedFor;
    private long _commit, _lastApplied;
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
    // indice idempotenza derivato dal log (REQ-IDM-2)
    private readonly Dictionary<string, IdemRec> _idem = new();

    private TaskCompletionSource _leaderTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly SemaphoreSlim _applySignal = new(0, int.MaxValue);
    public SemaphoreSlim AnnounceNow { get; } = new(0, 1);

    public RaftNode(ClusterConfig cfg, Membership members, PeerRegistry peers, IPeerTransport tx, IClock clock, Metrics metrics,
        IDataRepository repo, ILogger<RaftNode> log)
    {
        _cfg = cfg; _members = members; _peers = peers; _tx = tx; _clock = clock; _metrics = metrics; _repo = repo; _log = log;
        _self = members.SelfId;
        var dir = cfg.Storage.DataDir;
        _store = new RaftStateStore(dir);
        _walPath = Path.Combine(Path.GetFullPath(dir), "raft.wal");
        var inject = Path.Combine(Path.GetFullPath(dir), ".inject_io_error");
        _wal = new WalLog(_walPath, cfg.Storage.FsyncOnCommit, cfg.MaxBodyBytes, () => cfg.TestMode && File.Exists(inject));
        if (repo is FileDataRepository f) f.EntryResolver = i => _wal.ReadEntry(i);
        members.Persist = _ => { lock (_mu) { try { SaveState(true); } catch (Exception ex) { _log.LogError("Persistenza members fallita: {M}", ex.Message); } } };
    }

    // ===================================================================== recovery (REQ-WAL-3, REQ-ID-3)
    public void Initialize()
    {
        lock (_mu)
        {
            _selfUrl = NetworkHelper.ResolveAdvertiseUrl(_cfg);
            bool stateExists = _store.Exists;
            bool walHadData = File.Exists(_walPath) && new FileInfo(_walPath).Length > 0;
            var open = _wal.Open();                                           // 1-2) validazione e troncamento del record finale incompleto
            if (open.TruncatedBytes > 0) _log.LogWarning("Recovery: troncati {B} byte di record finale incompleto", open.TruncatedBytes);
            if (open.CorruptNonFinal) { SetRecoveryRequired(open.Message!); return; }   // 3)

            long persistedCommit = 0;
            if (!stateExists && !walHadData)
            {
                if (!_cfg.Storage.AllowEmptyState)
                {
                    throw new StartupBlockedException("REQ-ID-3: stato persistente assente (WAL e stato Raft) e Storage.AllowEmptyState=false: il nodo NON parte. " +
                        "Se è il primo deploy o un nuovo NodeId impostare AllowEmptyState=true e rimuoverlo subito dopo.");
                }
                SaveState(true);
            }
            else if (!stateExists) { SetRecoveryRequired("Stato Raft (term/voto) assente ma WAL presente: richiesta recovery esplicita."); return; }
            else
            {
                PersistentRaftState st;
                try { st = _store.Load(); }
                catch (Exception ex) { SetRecoveryRequired("raft_state.json illeggibile: " + ex.Message); return; }
                _term = st.CurrentTerm; _votedFor = st.VotedFor; persistedCommit = st.CommitIndex;
                if (persistedCommit > _wal.LastIndex)
                { SetRecoveryRequired($"commitIndex persistito ({persistedCommit}) oltre la fine del WAL ({_wal.LastIndex}): WAL perso o troncato."); return; }
            }

            RebuildIdem();                                                    // 4) indice delle richieste
            long applied = _repo.GetLastAppliedIndexAsync(default).GetAwaiter().GetResult();
            if (applied > _wal.LastIndex) { SetRecoveryRequired($"lastApplied ({applied}) oltre la fine del WAL ({_wal.LastIndex})."); return; }
            _commit = Math.Max(persistedCommit, applied);                     // 5) max(persistito, lastApplied) - REQ-WAL-3 rev.5
            _lastApplied = applied;
            if (_commit != persistedCommit) SaveState(false);
            for (long i = applied + 1; i <= _commit; i++)                     // 6-7) riallineamento della state machine
            {
                var e = _wal.ReadEntry(i)!;
                _repo.ApplyAsync(e, default).GetAwaiter().GetResult();
                _lastApplied = i;
            }
            long now = _clock.MonotonicMs;
            _discoveryEndMs = now + _cfg.DiscoveryPhaseMs;
            _recovered = true;                                                // REQ-WAL-4
            _log.LogInformation("Recovery completato: term={Term} lastIndex={Last} commitIndex={Commit} lastApplied={Applied}", _term, _wal.LastIndex, _commit, _lastApplied);
        }
    }

    private void SetRecoveryRequired(string why)
    {
        _recoveryRequired = true; _unhealthyReason = why;
        _log.LogCritical("EVENTO ALTA SEVERITÀ: {Why} Il nodo non vota e non accetta voci (unhealthy).", why);
    }

    // ===================================================================== helper di stato (sotto _mu)
    private string Ctx => $"[Node={_self} Term={_term} Role={_role}]";

    private PersistentRaftState BuildState(long term, string? vote) => new()
    { CurrentTerm = term, VotedFor = vote, CommitIndex = _commit, Members = _members.Members.ToList(), NodeId = _self };

    private void SaveState(bool durable) => _store.Save(BuildState(_term, _votedFor), durable);

    /// <summary>REQ-ID-5: lo stato è durevole PRIMA di aggiornare la memoria e quindi di rispondere.</summary>
    private void PersistTermVote(long term, string? vote)
    {
        try { _store.Save(BuildState(term, vote), true); }
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
        _role = r;
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
            foreach (var w in _waiters.Values) w.TrySetResult(null);       // REQ-STA-1: 503 al client
            _waiters.Clear();
            _progress.Clear();
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

    // ===================================================================== tick: timer, lease, storage
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _ = Task.Run(() => ApplyLoopAsync(ct), ct);
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
            if (occ >= 0.85 && !_alarm85) { _alarm85 = true; _log.LogError("ALLARME: occupazione disco {Occ:P0} (>=85%)", occ); }
            else if (occ >= 0.70 && !_alarm70) { _alarm70 = true; _log.LogWarning("ALLARME: occupazione disco {Occ:P0} (>=70%)", occ); }
            if (occ < 0.70) { _alarm70 = false; _alarm85 = false; }
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
                lock (_mu) { req.LastLogIndex = _wal.LastIndex; req.LastLogTerm = _wal.LastTerm; }
                var r = await _tx.SendAsync<PreVoteRequest, PreVoteResponse>(peer, "/internal/v1/prevote", req, RpcTimeout, ct);
                if (r.Ok && r.Body!.Term > term0) ObserveHigherTerm(r.Body.Term);
                return r.Ok && r.Body!.VoteGranted;
            });
            if (1 + pvGranted.Count < _cfg.Quorum) { lock (_mu) ResetElectionTimer(_clock.MonotonicMs); return; }

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
                lock (_mu) { req.LastLogIndex = _wal.LastIndex; req.LastLogTerm = _wal.LastTerm; }
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
            if (url == null) continue;
            tasks.Add((id, Task.Run(() => ask(new PeerEndpoint(id, url)))));
        }
        var granted = new List<string>();
        foreach (var (id, t) in tasks) { try { if (await t) granted.Add(id); } catch { } }
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
        _progress.Clear();
        long last = _wal.LastIndex;
        foreach (var id in _members.Others)
            _progress[id] = new PeerProgress { Next = last + 1, Match = 0, LastAckSendMs = granters.Contains(id) ? sendMs : long.MinValue / 2 };
        try
        {   // REQ-LDR-1: no-op nel term corrente
            _wal.Append(0, new[] { new WalInput(last + 1, _term, LogEntryType.NoOp, Guid.NewGuid().ToString(), "", _clock.UtcNow.Ticks, default) });
        }
        catch (IOException ex) { MarkIoFailure(ex); return; }
        _leaderCts = new CancellationTokenSource();
        var ct = _leaderCts.Token; long term = _term;
        foreach (var (id, prog) in _progress) { var i = id; var p = prog; _ = Task.Run(() => ReplicateLoopAsync(i, p, term, ct), ct); }
        if (_cfg.Quorum == 1) AdvanceCommit();
        NotifyLeaderChange();
    }

    // ===================================================================== replica (§9)
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
        if (url == null) return false;
        long prev, prevTerm, last, commit, sendMs;
        lock (_mu)
        {
            if (_role != NodeRole.Leader || _term != term) return false;
            prev = prog.Next - 1; prevTerm = _wal.TermAt(prev); last = _wal.LastIndex; commit = _commit; sendMs = _clock.MonotonicMs;
        }
        var entries = new List<LogEntry>(); long bytes = 0;
        for (long i = prev + 1; i <= last && entries.Count < 256 && bytes < 4 * 1024 * 1024; i++)
        {
            var e = _wal.ReadEntry(i); if (e == null) break;
            entries.Add(e); bytes += e.Payload.Length + 128;
        }
        var req = NewRpc<AppendEntriesRequest>();
        req.Term = term; req.LeaderId = _self; req.PrevLogIndex = prev; req.PrevLogTerm = prevTerm; req.Entries = entries; req.LeaderCommit = commit;
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
            long now = _clock.MonotonicMs;
            if (now - _lastCommitPersistMs > 100) { _lastCommitPersistMs = now; try { SaveState(false); } catch { } }
            _applySignal.Release();
        }
    }

    // ===================================================================== apply (REQ-REP-6)
    private async Task ApplyLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try { await _applySignal.WaitAsync(100, ct); } catch (OperationCanceledException) { break; }
            while (!ct.IsCancellationRequested)
            {
                LogEntry? e;
                lock (_mu) { if (!_recovered || _lastApplied >= _commit) break; e = _wal.ReadEntry(_lastApplied + 1); }
                if (e == null) break;
                ApplyOutcome outcome;
                try
                {
                    await _repo.ApplyAsync(e, ct);
                    if (e.EntryType == LogEntryType.NoOp) outcome = new ApplyOutcome(200, Array.Empty<byte>(), false);
                    else if (e.IdConflict) outcome = new ApplyOutcome(422, System.Text.Encoding.UTF8.GetBytes("{\"error\":\"RequestId già associato a un payload diverso\"}"), false);
                    else if (e.DuplicateOf > 0) outcome = new ApplyOutcome(200, await _repo.GetResultAsync(e.DuplicateOf, ct), true);
                    else outcome = new ApplyOutcome(200, await _repo.GetResultAsync(e.Index, ct), false);
                }
                catch (OperationCanceledException) { return; }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Apply fallito all'indice {Idx}: ritento", e.Index);
                    try { await Task.Delay(500, ct); } catch (OperationCanceledException) { return; }
                    break;
                }
                lock (_mu)
                {
                    _lastApplied = e.Index;                                         // avanza solo dopo apply riuscito
                    if (_waiters.Remove(e.Index, out var w)) w.TrySetResult(outcome);
                }
            }
        }
    }

    // ===================================================================== scrittura client (REQ-PRX-9, §12)
    public async Task<WriteResult> WriteAsync(string requestId, string payloadHash, ReadOnlyMemory<byte> payload, long deadlineMs, CancellationToken ct)
    {
        TaskCompletionSource<ApplyOutcome?> waiter; long replayIndex = 0;
        lock (_mu)
        {
            long now = _clock.MonotonicMs;
            if (_role != NodeRole.Leader) return WriteResult.NotLeaderResult;
            if (_draining || !StorageHealthy || !LeaseValid(now)) return new WriteResult(503, Array.Empty<byte>());   // REQ-LDR-4, OPS-5
            long refTicks = _lastApplied > 0 ? (_wal.GetMeta(_lastApplied)?.ReceivedTicks ?? 0) : 0;
            if (_idem.TryGetValue(requestId, out var rec) && !Expired(rec, refTicks))
            {
                if (rec.Hash != payloadHash) return new WriteResult(422, System.Text.Encoding.UTF8.GetBytes("{\"error\":\"RequestId già associato a un payload diverso\"}"));
                if (rec.FirstIndex <= _lastApplied) { replayIndex = rec.FirstIndex; waiter = null!; }
                else waiter = GetWaiter(rec.FirstIndex);                            // Pending: stessa voce, nessuna seconda voce
            }
            else
            {
                long idx = _wal.LastIndex + 1;
                try
                {
                    _wal.Append(0, new[] { new WalInput(idx, _term, LogEntryType.Data, requestId, payloadHash, _clock.UtcNow.Ticks, payload) });
                }
                catch (IOException ex) { MarkIoFailure(ex); return new WriteResult(503, Array.Empty<byte>()); }
                IndexEntry(_wal.GetMeta(idx)!, idx);
                waiter = GetWaiter(idx);
                SignalReplicators();
                if (_cfg.Quorum == 1) AdvanceCommit();
            }
        }
        if (replayIndex > 0)
        {
            _metrics.Inc("requestDuplicateCount");
            return new WriteResult(200, await _repo.GetResultAsync(replayIndex, ct), true);
        }
        int wait = (int)Math.Max(1, Math.Min(_cfg.ReplicationTimeoutMs, deadlineMs - _clock.MonotonicMs));
        var done = await Task.WhenAny(waiter.Task, Task.Delay(wait, ct));
        if (done != waiter.Task) return new WriteResult(504, Array.Empty<byte>());   // esito non osservato: il client ritenta
        var o = await waiter.Task;
        if (o == null) return new WriteResult(503, Array.Empty<byte>());              // leadership persa prima del commit
        if (o.Replay) _metrics.Inc("requestDuplicateCount");
        return new WriteResult(o.Status, o.Body, o.Replay);
    }

    private TaskCompletionSource<ApplyOutcome?> GetWaiter(long idx)
    {
        if (!_waiters.TryGetValue(idx, out var w)) _waiters[idx] = w = new(TaskCreationOptions.RunContinuationsAsynchronously);
        return w;
    }

    // ===================================================================== idempotenza derivata dal log (REQ-IDM-2..6)
    private bool Expired(IdemRec rec, long refTicks) =>
        _cfg.Storage.IdempotencyRetentionHours > 0 && refTicks - rec.ReceivedTicks > _cfg.Storage.IdempotencyRetentionHours * TimeSpan.TicksPerHour;

    private void IndexEntry(WalMeta m, long index)
    {
        m.DupOf = 0; m.Conflict = false;
        if (m.Type != LogEntryType.Data) return;
        if (_idem.TryGetValue(m.RequestId, out var rec) && !Expired(rec, m.ReceivedTicks))
        {
            if (rec.Hash == m.PayloadHash) m.DupOf = rec.FirstIndex; else { m.Conflict = true; m.DupOf = rec.FirstIndex; }  // REQ-IDM-4
        }
        else _idem[m.RequestId] = new IdemRec(m.PayloadHash, index, m.ReceivedTicks);
    }

    private void RebuildIdem()
    {
        _idem.Clear();
        for (long i = 1; i <= _wal.LastIndex; i++) IndexEntry(_wal.GetMeta(i)!, i);
    }

    // ===================================================================== RPC: PreVote / Vote / Append (lato ricevente)
    public PreVoteResponse HandlePreVote(PreVoteRequest r)
    {
        lock (_mu)
        {
            long now = _clock.MonotonicMs;
            bool grant = CanParticipate && _members.Contains(r.CandidateId)
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
            if (!CanParticipate || !_members.Contains(r.CandidateId)) return deny();
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

    /// <summary>Status 200 con risposta; 503 se lo storage non è affidabile; 400 se malformata.</summary>
    public (int Status, AppendEntriesResponse? Resp) HandleAppend(AppendEntriesRequest r)
    {
        lock (_mu)
        {
            if (!_recovered || !StorageHealthy) return (503, null);                          // REQ-WAL-5: nessuna conferma
            if (ClusterConfig.Norm(r.LeaderId) != ClusterConfig.Norm(r.SenderNodeId) || !_members.Contains(r.LeaderId)) return (400, null);
            if (r.Term < _term) return (200, new AppendEntriesResponse { Term = _term, Success = false });
            long now = _clock.MonotonicMs;
            try
            {
                if (r.Term > _term) AdoptHigherTerm(r.Term);                                 // persiste prima di rispondere
                else if (_role == NodeRole.Leader) return (200, new AppendEntriesResponse { Term = _term, Success = false });
                if (_role != NodeRole.Follower) { if (_role == NodeRole.Leader) StepDownInternal("AppendEntries valido"); SetRole(NodeRole.Follower); }
                var lid = ClusterConfig.Norm(r.LeaderId);
                if (_leaderId != lid) { _leaderId = lid; NotifyLeaderChange(); }
                _lastAppendMs = now; ResetElectionTimer(now);                                // REQ-REP-7

                long last = _wal.LastIndex;
                if (r.PrevLogIndex > last)
                    return (200, new AppendEntriesResponse { Term = _term, Success = false, ConflictIndex = last + 1, ConflictTerm = 0 });
                if (r.PrevLogIndex > 0)
                {
                    long pt = _wal.TermAt(r.PrevLogIndex);
                    if (pt != r.PrevLogTerm)
                        return (200, new AppendEntriesResponse { Term = _term, Success = false, ConflictTerm = pt, ConflictIndex = _wal.FirstIndexOfTerm(pt) });
                }

                long truncateFrom = 0; var toAppend = new List<WalInput>();
                for (int k = 0; k < r.Entries.Count; k++)
                {
                    var e = r.Entries[k];
                    if (e.Index != r.PrevLogIndex + 1 + k) return (400, null);
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
                    _wal.Append(truncateFrom, toAppend);                                     // fsync prima di rispondere (REQ-REP-3.6)
                    if (truncateFrom > 0) RebuildIdem();
                    else foreach (var w in toAppend) IndexEntry(_wal.GetMeta(w.Index)!, w.Index);
                }
                long lastNew = r.PrevLogIndex + r.Entries.Count;                             // ultima voce verificata coerente
                long nc = Math.Min(r.LeaderCommit, lastNew);                                 // REQ-REP-3.8
                if (nc > _commit)
                {
                    _commit = nc;
                    if (now - _lastCommitPersistMs > 100) { _lastCommitPersistMs = now; try { SaveState(false); } catch { } }
                    _applySignal.Release();
                }
                return (200, new AppendEntriesResponse { Term = _term, Success = true, MatchIndex = lastNew });
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

    public (bool Ready, string Reason) GetReadiness()
    {
        lock (_mu)
        {
            if (!_recovered) return (false, _unhealthyReason ?? "recovery non completato");
            if (!StorageHealthy) return (false, _unhealthyReason ?? "storage non sano");
            if (_draining) return (false, "shutdown in corso");
            long now = _clock.MonotonicMs;
            if (_role == NodeRole.Leader) return LeaseValid(now) ? (true, "leader con lease valido") : (false, "lease non valido");
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
            g.Set("logLength", _wal.LastIndex);
            g.Set("replicationLag", _role == NodeRole.Leader && _progress.Count > 0 ? _progress.Values.Max(p => _wal.LastIndex - p.Match) : 0);
            return new
            {
                nodeId = _self, role = _role.ToString(), term = _term, leaderId = _leaderId, commitIndex = _commit, lastApplied = _lastApplied,
                lastLogIndex = _wal.LastIndex, membersHash = _members.Hash, healthy = StorageHealthy, unhealthyReason = _unhealthyReason,
                peers = _peers.GetVisiblePeers().Select(p => new { p.NodeId, Url = p.RpcUrl ?? p.RestUrl, Role = p.Role.ToString(), p.Term }).ToList()
            };
        }
    }

    public Dictionary<string, long> GetMetrics() { GetStatus(); return _metrics.Snapshot(); }

    /// <summary>Per i test.</summary>
    public (long Term, long Commit, long Applied, long Last, bool Unhealthy) Probe() { lock (_mu) return (_term, _commit, _lastApplied, _wal.LastIndex, !StorageHealthy); }

    // ===================================================================== shutdown (REQ-OPS-5)
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        lock (_mu) _draining = true;                                                        // 1) nuove scritture: 503
        long end = _clock.MonotonicMs + _cfg.ShutdownGraceMs;                               // 2) completa le operazioni in corso
        while (_clock.MonotonicMs < end) { lock (_mu) { if (_waiters.Count == 0) break; } await Task.Delay(25); }
        lock (_mu) { try { if (_recovered) SaveState(true); } catch { } }                    // le voci committed sono già durevoli nel WAL
        await base.StopAsync(cancellationToken);
        _wal.Dispose();
    }
}
