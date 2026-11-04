using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;

namespace RaftCore.Simulator.Services;

public enum OpOutcome { InCorso, Ok, OkDopoRetry, Rifiutata, Abbandonata }

public sealed record Attempt(int Port, int Status, long Ms, string? Error);

/// <summary>Un'operazione logica: un RequestId, un endpoint, un body, uno o più tentativi (REQ-CLI-1).</summary>
public sealed class OpRecord
{
    public long Seq { get; init; }
    public string RequestId { get; init; } = "";
    /// <summary>Endpoint scelto alla prima risoluzione e mantenuto in tutti i ritenti (contratto client).</summary>
    public string Endpoint { get; set; } = "";
    public string EndpointPath { get; set; } = "";
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
    /// <summary>Peso di ogni endpoint (0 = escluso, massimo 100). Un endpoint non elencato ha peso 1.</summary>
    public Dictionary<string, int> EndpointWeights { get; set; } = new();
}

/// <summary>Contatori, latenze e serie per secondo: uno per il totale e uno per ogni endpoint.</summary>
internal sealed class OpStats
{
    public const int BucketsKept = 180;
    public long Ok, OkRetry, Rejected, GaveUp, Retries, Replays;
    public readonly SortedDictionary<long, int[]> Buckets = new();   // secondo -> [ok, okDopoRetry, rifiutate, abbandonate, ritenti]
    public readonly Queue<long> Latencies = new();

    public void Bucket(long sec, int index)
    {
        if (!Buckets.TryGetValue(sec, out var b)) Buckets[sec] = b = new int[5];
        b[index]++;
        while (Buckets.Count > BucketsKept) Buckets.Remove(Buckets.Keys.First());
    }

    public void Latency(long ms)
    {
        Latencies.Enqueue(ms);
        while (Latencies.Count > 2000) Latencies.Dequeue();
    }

    public object Counters(long? sent = null) => new { sent, ok = Ok, okAfterRetry = OkRetry, rejected = Rejected, gaveUp = GaveUp, retries = Retries, replays = Replays };

    public object LatencySummary()
    {
        var lat = Latencies.OrderBy(x => x).ToArray();
        long P(double q) => lat.Length == 0 ? 0 : lat[Math.Min(lat.Length - 1, (int)Math.Ceiling(q * lat.Length) - 1)];
        return new { p50 = P(0.5), p95 = P(0.95), p99 = P(0.99), max = lat.Length == 0 ? 0 : lat[^1], samples = lat.Length };
    }

    public List<int[]> Series(long nowSec) => Enumerable.Range(0, 120).Select(i =>
    {
        var b = Buckets.TryGetValue(nowSec - 119 + i, out var v) ? v : new int[5];
        return new[] { b[0], b[1], b[2], b[3], b[4] };
    }).ToList();
}

/// <summary>
/// Invio ciclico su N endpoint. Ogni operazione sceglie un endpoint secondo i pesi configurati e invia il JSON del suo
/// modello (<see cref="PayloadFactory"/>), con numero progressivo e timestamp. Si comporta come un client conforme al
/// contratto: RequestId generato una volta, stesso endpoint e stesso body byte per byte a ogni ritento, retry con backoff e
/// jitter su errore di rete, 503 e 504, mai su 4xx; su errore di rete si prova un altro nodo.
/// </summary>
public sealed class LoadGenerator : BackgroundService
{
    private readonly SimulatorOptions _opt;
    private readonly DataClient _client;
    private readonly ClusterMonitor _monitor;
    private readonly PayloadFactory _payloads;
    private readonly EventLog _events;
    private readonly object _l = new();
    private readonly ConcurrentDictionary<long, OpRecord> _ops = new();
    private readonly LinkedList<OpRecord> _recent = new();
    private OpStats _total = new();
    private readonly Dictionary<string, OpStats> _byEndpoint = new(StringComparer.Ordinal);
    private long _seq, _inFlight, _skipped;
    private volatile bool _running;
    private PendingFault? _fault;
    private readonly List<object> _faultResults = new();

    public string RunId { get; } = Guid.NewGuid().ToString("D");
    public GeneratorSettings Settings { get; private set; }
    public bool Running => _running;

    public LoadGenerator(SimulatorOptions opt, DataClient client, ClusterMonitor monitor, PayloadFactory payloads, EventLog events)
    {
        _opt = opt; _client = client; _monitor = monitor; _payloads = payloads; _events = events;
        Settings = new GeneratorSettings
        {
            IntervalMs = opt.DefaultIntervalMs, Target = opt.DefaultTarget,
            EndpointWeights = new Dictionary<string, int>(opt.DefaultEndpointWeights, StringComparer.Ordinal)
        };
    }

    public IReadOnlyCollection<OpRecord> AllOps => (IReadOnlyCollection<OpRecord>)_ops.Values;

    // ------------------------------------------------------------------ comandi
    public void Configure(GeneratorSettings s)
    {
        s.IntervalMs = Math.Clamp(s.IntervalMs, 5, 60_000);
        s.PadBytes = Math.Clamp(s.PadBytes, 0, _opt.MaxBodyBytes);
        if (string.IsNullOrWhiteSpace(s.Target)) s.Target = "RoundRobin";
        s.EndpointWeights = (s.EndpointWeights ?? new()).Where(kv => !string.IsNullOrWhiteSpace(kv.Key))
            .ToDictionary(kv => kv.Key.Trim(), kv => Math.Clamp(kv.Value, 0, 100), StringComparer.Ordinal);
        Settings = s;
    }

    public void Start()
    {
        if (_running) return;
        _running = true;
        _events.Info($"Invio ciclico avviato: ogni {Settings.IntervalMs} ms, destinazione {TargetLabel(Settings.Target)}, {EndpointLabel()}");
    }

    public void Stop()
    {
        if (!_running) return;
        _running = false;
        _events.Info($"Invio ciclico fermato dopo {Interlocked.Read(ref _seq)} operazioni");
    }

    public void ResetStats()
    {
        lock (_l) { _recent.Clear(); _total = new OpStats(); _byEndpoint.Clear(); _faultResults.Clear(); _skipped = 0; }
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
        var settings = Settings;
        byte[]? body = null;                                     // costruito una sola volta, appena l'endpoint è noto
        Interlocked.Increment(ref _inFlight);
        var sw = Stopwatch.StartNew();
        try
        {
            int lastNetErrPort = -1;
            for (int attempt = 0; ; attempt++)
            {
                if (body == null && PickEndpoint(settings, seq) is { } ep)
                {
                    op.Endpoint = ep.Name; op.EndpointPath = ep.Path;
                    body = _payloads.Build(ep.Name, seq, op.StartedAt, RunId, settings.PadBytes);   // retry con gli stessi byte
                }
                var node = Pick(settings.Target, seq, attempt, lastNetErrPort);
                if (node == null || body == null)
                {
                    string why = node == null ? "nessun nodo raggiungibile"
                        : _monitor.KnownEndpoints().Count == 0 ? "nessun endpoint noto" : "nessun endpoint abilitato (tutti i pesi a 0)";
                    lock (op) op.Attempts.Add(new Attempt(node?.Port ?? 0, 0, 0, why));
                    op.Error = why;
                }
                else
                {
                    var r = await _client.PostDataAsync(node.Url, op.EndpointPath, op.RequestId, body, _opt.RequestTimeoutMs, ct: ct);
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
                CountRetry(op.Endpoint);
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

    /// <summary>Peso effettivo di un endpoint: quello configurato (0-100), altrimenti 1.</summary>
    private static int WeightOf(GeneratorSettings s, string name) => s.EndpointWeights.TryGetValue(name, out var w) ? w : 1;

    /// <summary>
    /// Endpoint dell'operazione, in proporzione ai pesi. La scelta dipende solo da seq: con pesi 3 e 1 su 4 operazioni
    /// consecutive tre vanno al primo endpoint e una al secondo.
    /// </summary>
    private SimEndpoint? PickEndpoint(GeneratorSettings s, long seq)
    {
        var eps = _monitor.KnownEndpoints().Select(e => (e, w: WeightOf(s, e.Name))).Where(x => x.w > 0).ToList();
        int total = eps.Sum(x => x.w);
        if (total == 0) return null;
        long k = seq % total;
        foreach (var (e, w) in eps) { if (k < w) return e; k -= w; }
        return eps[^1].e;
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

    // ------------------------------------------------------------------ statistiche
    private IEnumerable<OpStats> StatsFor(string endpoint)
    {
        yield return _total;
        if (endpoint.Length == 0) yield break;
        if (!_byEndpoint.TryGetValue(endpoint, out var s)) _byEndpoint[endpoint] = s = new OpStats();
        yield return s;
    }

    private void CountRetry(string endpoint)
    {
        long sec = DateTimeOffset.Now.ToUnixTimeSeconds();
        lock (_l) foreach (var s in StatsFor(endpoint)) { s.Retries++; s.Bucket(sec, 4); }
    }

    private void Complete(OpRecord op)
    {
        int index = op.Outcome switch { OpOutcome.Ok => 0, OpOutcome.OkDopoRetry => 1, OpOutcome.Rifiutata => 2, _ => 3 };
        long sec = DateTimeOffset.Now.ToUnixTimeSeconds();
        if (op.Outcome == OpOutcome.Abbandonata)
            _events.Error($"Operazione #{op.Seq}{EndpointSuffix(op)} abbandonata dopo {op.Attempts.Count} tentativi ({op.Error}). Può essere stata scritta comunque: vedi Verifica");
        else if (op.Outcome == OpOutcome.Rifiutata)
            _events.Warn($"Operazione #{op.Seq}{EndpointSuffix(op)} rifiutata: {op.FinalStatus} {op.Error}");
        lock (_l)
        {
            foreach (var s in StatsFor(op.Endpoint))
            {
                switch (index) { case 0: s.Ok++; break; case 1: s.OkRetry++; break; case 2: s.Rejected++; break; default: s.GaveUp++; break; }
                if (op.Replay) s.Replays++;
                s.Bucket(sec, index);
                if (index <= 1) s.Latency(op.LatencyMs);
            }
            _recent.AddFirst(op);
            while (_recent.Count > 200) _recent.RemoveLast();
            if (index <= 1 && _fault != null && op.StartedAt >= _fault.At)
            {
                var ms = (long)((op.CompletedAt ?? DateTimeOffset.Now) - _fault.At).TotalMilliseconds;
                _faultResults.Insert(0, new { fault = _fault.Description, at = _fault.At, firstOkAfterMs = ms, seq = op.Seq });
                if (_faultResults.Count > 20) _faultResults.RemoveAt(_faultResults.Count - 1);
                _events.Ok($"Failover: primo 200 dopo {ms} ms da «{_fault.Description}» (operazione #{op.Seq})");
                _fault = null;
            }
        }
    }

    private static string EndpointSuffix(OpRecord op) => op.Endpoint.Length > 0 ? $" ({op.Endpoint})" : "";

    // ------------------------------------------------------------------ stato per l'interfaccia
    public object State()
    {
        var endpoints = _monitor.KnownEndpoints();
        var settings = Settings;
        lock (_l)
        {
            long nowSec = DateTimeOffset.Now.ToUnixTimeSeconds();
            int totalWeight = endpoints.Sum(e => WeightOf(settings, e.Name));
            // endpoint esposti dal cluster, più quelli che hanno già statistiche (es. un endpoint rimosso nel frattempo)
            var names = endpoints.Select(e => e.Name).Concat(_byEndpoint.Keys).Distinct().ToList();
            return new
            {
                running = _running,
                runId = RunId,
                settings,
                targetLabel = TargetLabel(settings.Target),
                endpointLabel = EndpointLabel(),
                counters = new
                {
                    sent = Interlocked.Read(ref _seq),
                    inFlight = Interlocked.Read(ref _inFlight),
                    ok = _total.Ok, okAfterRetry = _total.OkRetry, rejected = _total.Rejected, gaveUp = _total.GaveUp,
                    retries = _total.Retries, replays = _total.Replays, skipped = _skipped
                },
                latency = _total.LatencySummary(),
                series = _total.Series(nowSec),
                byEndpoint = names.Select(name =>
                {
                    var ep = endpoints.FirstOrDefault(e => e.Name == name);
                    var s = _byEndpoint.TryGetValue(name, out var v) ? v : new OpStats();
                    int w = ep == null ? 0 : WeightOf(settings, name);
                    return new
                    {
                        name, path = ep?.Path, exposed = ep != null, weight = w,
                        share = totalWeight == 0 || ep == null ? 0 : Math.Round(100.0 * w / totalWeight, 1),
                        counters = s.Counters(), latency = s.LatencySummary(), series = s.Series(nowSec)
                    };
                }).ToList(),
                payloadTemplates = _payloads.Templates(endpoints.Select(e => e.Name)),
                pendingFault = _fault == null ? null : new { _fault.Description, _fault.At },
                failovers = _faultResults.ToList(),
                recent = _recent.Take(60).Select(o => new
                {
                    o.Seq, id = o.RequestId[..8], requestId = o.RequestId, endpoint = o.Endpoint, startedAt = o.StartedAt, o.LatencyMs,
                    outcome = o.Outcome.ToString(), o.FinalStatus, o.Replay, attempts = o.Attempts.Count, path = o.Path,
                    error = o.Error == null ? null : DataClient.Truncate(o.Error, 160)
                }).ToList()
            };
        }
    }

    private string EndpointLabel()
    {
        var eps = _monitor.KnownEndpoints();
        var active = eps.Select(e => (e.Name, w: WeightOf(Settings, e.Name))).Where(x => x.w > 0).ToList();
        if (eps.Count == 0) return "endpoint non ancora noti";
        if (active.Count == 0) return "nessun endpoint abilitato";
        if (active.Count == eps.Count && active.All(x => x.w == active[0].w)) return "tutti gli endpoint a rotazione";
        return "endpoint " + string.Join(", ", active.Select(x => x.w == 1 ? x.Name : $"{x.Name}×{x.w}"));
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
        var sb = new StringBuilder("seq;requestId;endpoint;startedAt;completedAt;outcome;finalStatus;replay;latencyMs;attempts;path;error\n");
        foreach (var o in _ops.Values.OrderBy(o => o.Seq))
            sb.Append($"{o.Seq};{o.RequestId};{o.Endpoint};{o.StartedAt:O};{o.CompletedAt:O};{o.Outcome};{o.FinalStatus};{o.Replay};{o.LatencyMs};{o.Attempts.Count};{o.Path};{o.Error?.Replace(';', ',').Replace('\n', ' ')}\n");
        return sb.ToString();
    }
}
