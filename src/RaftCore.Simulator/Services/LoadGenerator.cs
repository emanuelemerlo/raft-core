using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace RaftCore.Simulator.Services;

public enum OpOutcome { InCorso, Ok, OkDopoRetry, Rifiutata, Abbandonata }

public sealed record Attempt(int Port, int Status, long Ms, string? Error);

/// <summary>Un'operazione logica: un RequestId, un body, uno o più tentativi (REQ-CLI-1).</summary>
public sealed class OpRecord
{
    public long Seq { get; init; }
    public string RequestId { get; init; } = "";
    public DateTimeOffset StartedAt { get; init; }
    public DateTimeOffset? CompletedAt { get; set; }
    public OpOutcome Outcome { get; set; } = OpOutcome.InCorso;
    public int FinalStatus { get; set; }
    public bool Replay { get; set; }
    public long LatencyMs { get; set; }
    public string? Error { get; set; }
    public List<Attempt> Attempts { get; } = new();
    public string Path => string.Join(" → ", Attempts.Select(a => a.Status == 0 ? $"{a.Port}✕" : $"{a.Port}:{a.Status}"));
}

public sealed class GeneratorSettings
{
    public int IntervalMs { get; set; } = 200;
    /// <summary>RoundRobin, Leader, Follower oppure il NodeId di un nodo.</summary>
    public string Target { get; set; } = "RoundRobin";
    /// <summary>Byte di riempimento aggiunti al JSON (campo "pad") per simulare payload più grandi.</summary>
    public int PadBytes { get; set; }
}

/// <summary>
/// Invio ciclico di JSON con timestamp e numero progressivo. Ogni operazione si comporta come un client conforme
/// al contratto: RequestId generato una volta, stesso body byte per byte a ogni ritento, retry con backoff e jitter
/// su errore di rete, 503 e 504, mai su 4xx; su errore di rete si prova un altro nodo.
/// </summary>
public sealed class LoadGenerator : BackgroundService
{
    private const int Bucket = 1, BucketsKept = 180;

    private readonly SimulatorOptions _opt;
    private readonly DataClient _client;
    private readonly ClusterMonitor _monitor;
    private readonly EventLog _events;
    private readonly object _l = new();
    private readonly ConcurrentDictionary<long, OpRecord> _ops = new();
    private readonly LinkedList<OpRecord> _recent = new();
    private readonly SortedDictionary<long, int[]> _buckets = new();     // secondo -> [ok, okDopoRetry, rifiutate, abbandonate, ritenti]
    private readonly Queue<long> _latencies = new();
    private long _seq, _inFlight, _skipped;
    private long _cntOk, _cntOkRetry, _cntRejected, _cntGaveUp, _cntRetries, _cntReplay;
    private volatile bool _running;
    private PendingFault? _fault;
    private readonly List<object> _faultResults = new();

    public string RunId { get; } = Guid.NewGuid().ToString("D");
    public GeneratorSettings Settings { get; private set; }
    public bool Running => _running;

    public LoadGenerator(SimulatorOptions opt, DataClient client, ClusterMonitor monitor, EventLog events)
    {
        _opt = opt; _client = client; _monitor = monitor; _events = events;
        Settings = new GeneratorSettings { IntervalMs = opt.DefaultIntervalMs, Target = opt.DefaultTarget };
    }

    public IReadOnlyCollection<OpRecord> AllOps => (IReadOnlyCollection<OpRecord>)_ops.Values;

    // ------------------------------------------------------------------ comandi
    public void Configure(GeneratorSettings s)
    {
        s.IntervalMs = Math.Clamp(s.IntervalMs, 5, 60_000);
        s.PadBytes = Math.Clamp(s.PadBytes, 0, _opt.MaxBodyBytes);
        if (string.IsNullOrWhiteSpace(s.Target)) s.Target = "RoundRobin";
        Settings = s;
    }

    public void Start()
    {
        if (_running) return;
        _running = true;
        _events.Info($"Invio ciclico avviato: ogni {Settings.IntervalMs} ms, destinazione {TargetLabel(Settings.Target)}");
    }

    public void Stop()
    {
        if (!_running) return;
        _running = false;
        _events.Info($"Invio ciclico fermato dopo {Interlocked.Read(ref _seq)} operazioni");
    }

    public void ResetStats()
    {
        lock (_l)
        {
            _recent.Clear(); _buckets.Clear(); _latencies.Clear(); _faultResults.Clear();
            _cntOk = _cntOkRetry = _cntRejected = _cntGaveUp = _cntRetries = _cntReplay = _skipped = 0;
        }
        _events.Info("Statistiche azzerate (numerazione e storico per la verifica invariati)");
    }

    /// <summary>Invia N operazioni contemporaneamente (raffica).</summary>
    public int Burst(int count)
    {
        count = Math.Clamp(count, 1, 2000);
        for (int i = 0; i < count; i++) _ = RunOperationAsync(CancellationToken.None);
        _events.Info($"Raffica di {count} operazioni inviata");
        return count;
    }

    public Task<OpRecord> SendOneAsync() => RunOperationAsync(CancellationToken.None);

    /// <summary>Dopo un guasto iniettato: misura il tempo fino al primo 200 di un'operazione iniziata dopo il guasto (REQ-FOV-1).</summary>
    public void MarkFault(string description)
    {
        lock (_l) _fault = new PendingFault(description, DateTimeOffset.Now);
        if (!_running) _events.Info("Suggerimento: avviare l'invio ciclico per misurare il tempo di failover");
    }

    private sealed record PendingFault(string Description, DateTimeOffset At);

    // ------------------------------------------------------------------ ciclo
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        double next = 0;
        while (!ct.IsCancellationRequested)
        {
            if (!_running) { await Task.Delay(50, ct); next = sw.ElapsedMilliseconds; continue; }
            if (Interlocked.Read(ref _inFlight) >= _opt.MaxInFlight) Interlocked.Increment(ref _skipped);
            else _ = RunOperationAsync(ct);
            next += Settings.IntervalMs;
            double wait = next - sw.ElapsedMilliseconds;
            if (wait < -Settings.IntervalMs * 5) { next = sw.ElapsedMilliseconds; wait = 0; }   // in ritardo: non recuperare a raffica
            if (wait > 0) await Task.Delay(TimeSpan.FromMilliseconds(wait), ct);
        }
    }

    private async Task<OpRecord> RunOperationAsync(CancellationToken ct)
    {
        long seq = Interlocked.Increment(ref _seq);
        var op = new OpRecord { Seq = seq, RequestId = Guid.NewGuid().ToString("D"), StartedAt = DateTimeOffset.Now };
        _ops[seq] = op;
        if (_ops.Count > 1_000_000 && _ops.TryRemove(_ops.Keys.Min(), out _)) { }
        byte[] body = BuildPayload(op);                          // serializzato una sola volta: retry con gli stessi byte
        var settings = Settings;
        Interlocked.Increment(ref _inFlight);
        var sw = Stopwatch.StartNew();
        try
        {
            int lastNetErrPort = -1;
            for (int attempt = 0; ; attempt++)
            {
                var node = Pick(settings.Target, seq, attempt, lastNetErrPort);
                if (node == null)
                {
                    lock (op) op.Attempts.Add(new Attempt(0, 0, 0, "nessun nodo raggiungibile"));
                }
                else
                {
                    var r = await _client.PostDataAsync(node.Url, op.RequestId, body, _opt.RequestTimeoutMs, ct: ct);
                    lock (op) op.Attempts.Add(new Attempt(node.Port, r.Status, r.ElapsedMs, r.Error));
                    if (r.Status is >= 200 and < 500)
                    {
                        op.FinalStatus = r.Status; op.Replay = r.Replay;
                        op.Outcome = r.Status < 300 ? (attempt == 0 ? OpOutcome.Ok : OpOutcome.OkDopoRetry) : OpOutcome.Rifiutata;
                        if (r.Status >= 300) op.Error = r.Body;
                        break;
                    }
                    op.FinalStatus = r.Status; op.Error = r.Error ?? r.Body;
                    lastNetErrPort = r.NetworkError ? node.Port : -1;
                }
                // backoff esponenziale con jitter (REQ-CLI-1), entro il budget complessivo dell'operazione
                int backoff = (int)Math.Min(2000, 100 * Math.Pow(2, Math.Min(attempt, 5))) + Random.Shared.Next(0, 100);
                if (sw.ElapsedMilliseconds + backoff > _opt.RetryBudgetMs) { op.Outcome = OpOutcome.Abbandonata; break; }
                Interlocked.Increment(ref _cntRetries);
                Count(4);
                await Task.Delay(backoff, ct);
            }
        }
        catch (OperationCanceledException) { op.Outcome = OpOutcome.Abbandonata; op.Error = "simulatore in chiusura"; }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
            op.LatencyMs = sw.ElapsedMilliseconds;
            op.CompletedAt = DateTimeOffset.Now;
            Complete(op);
        }
        return op;
    }

    private byte[] BuildPayload(OpRecord op)
    {
        using var ms = new MemoryStream();
        using (var w = new Utf8JsonWriter(ms))
        {
            w.WriteStartObject();
            w.WriteNumber("seq", op.Seq);
            w.WriteString("timestamp", op.StartedAt);
            w.WriteString("source", _opt.Source);
            w.WriteString("runId", RunId);
            if (Settings.PadBytes > 0) w.WriteString("pad", new string('x', Settings.PadBytes));
            w.WriteEndObject();
        }
        return ms.ToArray();
    }

    private NodeView? Pick(string target, long seq, int attempt, int avoidPort)
    {
        var nodes = _monitor.Reachable();
        if (avoidPort > 0 && nodes.Count > 1) nodes = nodes.Where(n => n.Port != avoidPort).ToList();
        if (nodes.Count == 0) return null;
        NodeView RoundRobin(IReadOnlyList<NodeView> list) => list[(int)((seq + attempt) % list.Count)];
        switch (target)
        {
            case "RoundRobin": return RoundRobin(nodes);
            case "Leader": return nodes.FirstOrDefault(n => n.Role == "Leader") ?? RoundRobin(nodes);
            case "Follower":
                var followers = nodes.Where(n => n.Role != "Leader").ToList();
                return followers.Count > 0 ? RoundRobin(followers) : RoundRobin(nodes);
            default:
                // nodo specifico: su errore di rete si prova un altro nodo (REQ-CLI-1)
                return nodes.FirstOrDefault(n => n.NodeId.Equals(target, StringComparison.OrdinalIgnoreCase)) ?? RoundRobin(nodes);
        }
    }

    private void Count(int index)
    {
        long sec = DateTimeOffset.Now.ToUnixTimeSeconds() / Bucket;
        lock (_l)
        {
            if (!_buckets.TryGetValue(sec, out var b)) _buckets[sec] = b = new int[5];
            b[index]++;
            while (_buckets.Count > BucketsKept) _buckets.Remove(_buckets.Keys.First());
        }
    }

    private void Complete(OpRecord op)
    {
        switch (op.Outcome)
        {
            case OpOutcome.Ok: Interlocked.Increment(ref _cntOk); Count(0); break;
            case OpOutcome.OkDopoRetry: Interlocked.Increment(ref _cntOkRetry); Count(1); break;
            case OpOutcome.Rifiutata: Interlocked.Increment(ref _cntRejected); Count(2); break;
            default: Interlocked.Increment(ref _cntGaveUp); Count(3); break;
        }
        if (op.Replay) Interlocked.Increment(ref _cntReplay);
        if (op.Outcome == OpOutcome.Abbandonata)
            _events.Error($"Operazione #{op.Seq} abbandonata dopo {op.Attempts.Count} tentativi ({op.Error}). Può essere stata scritta comunque: vedi Verifica");
        else if (op.Outcome == OpOutcome.Rifiutata)
            _events.Warn($"Operazione #{op.Seq} rifiutata: {op.FinalStatus} {op.Error}");
        lock (_l)
        {
            _recent.AddFirst(op);
            while (_recent.Count > 200) _recent.RemoveLast();
            if (op.Outcome is OpOutcome.Ok or OpOutcome.OkDopoRetry)
            {
                _latencies.Enqueue(op.LatencyMs);
                while (_latencies.Count > 2000) _latencies.Dequeue();
                if (_fault != null && op.StartedAt >= _fault.At)
                {
                    var ms = (long)((op.CompletedAt ?? DateTimeOffset.Now) - _fault.At).TotalMilliseconds;
                    _faultResults.Insert(0, new { fault = _fault.Description, at = _fault.At, firstOkAfterMs = ms, seq = op.Seq });
                    if (_faultResults.Count > 20) _faultResults.RemoveAt(_faultResults.Count - 1);
                    _events.Ok($"Failover: primo 200 dopo {ms} ms da «{_fault.Description}» (operazione #{op.Seq})");
                    _fault = null;
                }
            }
        }
    }

    // ------------------------------------------------------------------ stato per l'interfaccia
    public object State()
    {
        lock (_l)
        {
            var lat = _latencies.OrderBy(x => x).ToArray();
            long P(double q) => lat.Length == 0 ? 0 : lat[Math.Min(lat.Length - 1, (int)Math.Ceiling(q * lat.Length) - 1)];
            long nowSec = DateTimeOffset.Now.ToUnixTimeSeconds();
            var series = Enumerable.Range(0, 120).Select(i =>
            {
                long s = nowSec - 119 + i;
                var b = _buckets.TryGetValue(s, out var v) ? v : new int[5];
                return new[] { b[0], b[1], b[2], b[3], b[4] };
            }).ToList();
            return new
            {
                running = _running,
                runId = RunId,
                settings = Settings,
                targetLabel = TargetLabel(Settings.Target),
                counters = new
                {
                    sent = Interlocked.Read(ref _seq),
                    inFlight = Interlocked.Read(ref _inFlight),
                    ok = _cntOk, okAfterRetry = _cntOkRetry, rejected = _cntRejected, gaveUp = _cntGaveUp,
                    retries = _cntRetries, replays = _cntReplay, skipped = _skipped
                },
                latency = new { p50 = P(0.5), p95 = P(0.95), p99 = P(0.99), max = lat.Length == 0 ? 0 : lat[^1], samples = lat.Length },
                series,
                pendingFault = _fault == null ? null : new { _fault.Description, _fault.At },
                failovers = _faultResults.ToList(),
                recent = _recent.Take(60).Select(o => new
                {
                    o.Seq, id = o.RequestId[..8], requestId = o.RequestId, startedAt = o.StartedAt, o.LatencyMs,
                    outcome = o.Outcome.ToString(), o.FinalStatus, o.Replay, attempts = o.Attempts.Count, path = o.Path,
                    error = o.Error == null ? null : DataClient.Truncate(o.Error, 160)
                }).ToList()
            };
        }
    }

    private string TargetLabel(string target) => target switch
    {
        "RoundRobin" => "tutti i nodi a rotazione",
        "Leader" => "solo il Leader",
        "Follower" => "solo i Follower (inoltro)",
        _ => "nodo " + (target.Length >= 8 ? target[..8] : target)
    };

    public string OpsCsv()
    {
        var sb = new StringBuilder("seq;requestId;startedAt;completedAt;outcome;finalStatus;replay;latencyMs;attempts;path;error\n");
        foreach (var o in _ops.Values.OrderBy(o => o.Seq))
            sb.Append($"{o.Seq};{o.RequestId};{o.StartedAt:O};{o.CompletedAt:O};{o.Outcome};{o.FinalStatus};{o.Replay};{o.LatencyMs};{o.Attempts.Count};{o.Path};{o.Error?.Replace(';', ',').Replace('\n', ' ')}\n");
        return sb.ToString();
    }
}
