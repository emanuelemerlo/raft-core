using System.Text;

namespace RaftCore.Simulator.Services;

/// <param name="PerEndpoint">false se lo scenario non dipende dall'endpoint (es. path non configurato): con «tutti» si esegue una volta.</param>
public sealed record ScenarioInfo(string Id, string Title, string Expected, string Spec, bool PerEndpoint = true);

public sealed record ScenarioStep(string Label, int Port, int Status, bool Replay, long Ms, string? Note);

public sealed record ScenarioResult(string Id, string Title, string Expected, string? Endpoint, bool Passed, string Summary,
    IReadOnlyList<ScenarioStep> Steps, DateTimeOffset At);

/// <summary>
/// Casi del contratto client e dell'idempotenza (§12, §14, §15 della specifica, docs/specifica.md), eseguiti su richiesta
/// su un endpoint scelto oppure su tutti. I body seguono il modello dell'endpoint (<see cref="PayloadFactory"/>), così un
/// writer che valida il proprio schema non rifiuta le richieste degli scenari.
/// </summary>
public sealed class ScenarioRunner
{
    /// <summary>Valore del parametro endpoint che esegue lo scenario su ogni endpoint esposto dal cluster.</summary>
    public const string AllEndpoints = "*";

    private readonly SimulatorOptions _opt;
    private readonly DataClient _client;
    private readonly ClusterMonitor _monitor;
    private readonly AppliedLogVerifier _verifier;
    private readonly LoadGenerator _gen;
    private readonly PayloadFactory _payloads;
    private readonly EventLog _events;
    private readonly List<ScenarioResult> _history = new();
    private long _seq;

    public static readonly IReadOnlyList<ScenarioInfo> All = new[]
    {
        new ScenarioInfo("replay", "Retry con stesso ID e stesso body", "200, poi 200 con X-Idempotent-Replay: true; una sola scrittura", "T7 · REQ-IDM-3"),
        new ScenarioInfo("conflict", "Stesso ID, body diverso solo per uno spazio", "200, poi 422; nessuna seconda scrittura", "T9/T42 · REQ-IDM-8"),
        new ScenarioInfo("parallel", "10 richieste identiche in parallelo", "10 × 200; una sola voce applicata", "T33 · REQ-IDM-3"),
        new ScenarioInfo("via-follower", "Scrittura tramite un Follower", "200 come dal Leader; nessun header interno nella risposta", "T4 · REQ-PRX-1/3/7"),
        new ScenarioInfo("missing-id", "Senza X-Request-ID", "400, nessuna scrittura", "T34 · REQ-IDM-1"),
        new ScenarioInfo("invalid-id", "X-Request-ID non UUID", "400", "REQ-IDM-1"),
        new ScenarioInfo("invalid-json", "Body JSON non valido", "400", "REQ-API-2"),
        new ScenarioInfo("too-large", "Body oltre MaxBodyBytes", "413, senza inoltro né scrittura", "T34 · REQ-PRX-2"),
        new ScenarioInfo("spoofed-headers", "Client che invia X-Forward-Hops e X-Forwarded-By", "200: header di inoltro non firmati ignorati (mai 421 al client)", "REQ-PRX-3/6"),
        new ScenarioInfo("no-content-type", "Body JSON senza Content-Type", "200: conta il body, non l'header", "REQ-API-1"),
        new ScenarioInfo("cross-endpoint", "Stesso ID su un altro endpoint", "200, poi 422 sull'endpoint successivo; una sola scrittura", "T59 · REQ-EPT-3"),
        new ScenarioInfo("unknown-path", "Path non configurato", "404, nessuna scrittura", "T60 · REQ-EPT-1", PerEndpoint: false),
    };

    public ScenarioRunner(SimulatorOptions opt, DataClient client, ClusterMonitor monitor, AppliedLogVerifier verifier, LoadGenerator gen,
        PayloadFactory payloads, EventLog events)
    { _opt = opt; _client = client; _monitor = monitor; _verifier = verifier; _gen = gen; _payloads = payloads; _events = events; }

    public IReadOnlyList<ScenarioResult> History() { lock (_history) return _history.ToList(); }

    /// <summary>Esegue lo scenario sull'endpoint indicato (vuoto = il primo esposto) oppure, con «*», su ogni endpoint.</summary>
    public async Task<IReadOnlyList<ScenarioResult>> RunAsync(string id, string? endpoint, CancellationToken ct)
    {
        var info = All.FirstOrDefault(s => s.Id == id) ?? throw new SimulatorException($"Scenario '{id}' sconosciuto");
        var eps = _monitor.KnownEndpoints();
        if (eps.Count == 0) throw new SimulatorException("Nessun endpoint noto: i nodi non hanno ancora risposto con il loro stato");
        IReadOnlyList<SimEndpoint> targets = endpoint == AllEndpoints
            ? (info.PerEndpoint ? eps : eps.Take(1).ToList())
            : new[] { string.IsNullOrEmpty(endpoint) ? eps[0]
                      : eps.FirstOrDefault(e => e.Name == endpoint) ?? throw new SimulatorException($"Endpoint '{endpoint}' non esposto dal cluster") };
        var results = new List<ScenarioResult>();
        foreach (var ep in targets) results.Add(await RunOneAsync(info, ep, ct));
        return results;
    }

    private async Task<ScenarioResult> RunOneAsync(ScenarioInfo info, SimEndpoint ep, CancellationToken ct)
    {
        var steps = new List<ScenarioStep>();
        bool passed; string summary;
        try { (passed, summary) = await RunCoreAsync(info.Id, ep, steps, ct); }
        catch (SimulatorException ex) { passed = false; summary = ex.Message; }
        var label = info.PerEndpoint ? ep.Name : null;
        var result = new ScenarioResult(info.Id, info.Title, info.Expected, label, passed, summary, steps, DateTimeOffset.Now);
        lock (_history) { _history.Insert(0, result); if (_history.Count > 100) _history.RemoveAt(_history.Count - 1); }
        var where = label == null ? "" : $" su {label}";
        if (passed) _events.Ok($"Scenario «{info.Title}»{where}: superato. {summary}");
        else _events.Error($"Scenario «{info.Title}»{where}: NON superato. {summary}");
        return result;
    }

    private NodeView Entry(bool preferFollower = true)
    {
        var nodes = _monitor.Reachable();
        if (nodes.Count == 0) throw new SimulatorException("Nessun nodo raggiungibile: avviare il cluster");
        return (preferFollower ? nodes.FirstOrDefault(n => n.Role != "Leader") : null) ?? nodes[0];
    }

    /// <summary>Body valido per l'endpoint (modello dell'endpoint), marcato con lo scenario. Un runId distinto da quello
    /// dell'invio ciclico tiene queste scritture fuori dalla verifica della sessione.</summary>
    private byte[] Body(string scenario, SimEndpoint ep, int padBytes = 0) =>
        _payloads.Build(ep.Name, Interlocked.Increment(ref _seq), DateTimeOffset.Now, _gen.RunId + "-scenari", padBytes, scenario);

    /// <summary>Stesso JSON con uno spazio in più dopo il primo ':': semanticamente uguale, byte diversi.</summary>
    private static byte[] Respaced(byte[] body)
    {
        var s = Encoding.UTF8.GetString(body);
        int i = s.IndexOf(':');
        return Encoding.UTF8.GetBytes(s.Insert(i + 1, " "));
    }

    private async Task<PostResult> Post(List<ScenarioStep> steps, string label, NodeView node, string path, string? id, byte[] body,
        IEnumerable<KeyValuePair<string, string>>? headers, CancellationToken ct, string? note = null)
    {
        var r = await _client.PostDataAsync(node.Url, path, id, body, _opt.RequestTimeoutMs, headers, ct);
        steps.Add(new ScenarioStep(label, node.Port, r.Status, r.Replay, r.ElapsedMs, note ?? r.Error ?? (r.Status >= 300 ? r.Body : null)));
        return r;
    }

    private async Task<(bool, string)> RunCoreAsync(string id, SimEndpoint ep, List<ScenarioStep> steps, CancellationToken ct)
    {
        var node = Entry();
        string rid = Guid.NewGuid().ToString("D");
        switch (id)
        {
            case "replay":
            {
                var body = Body(id, ep);
                var a = await Post(steps, "Prima richiesta", node, ep.Path, rid, body, null, ct);
                var b = await Post(steps, "Retry: stesso ID, stessi byte", node, ep.Path, rid, body, null, ct);
                var applied = await _verifier.AppliedRowsForRequestAsync(rid, ct);
                bool ok = a.Status == 200 && b.Status == 200 && b.Replay && applied is null or 1;
                return (ok, $"Risposte {a.Status} e {b.Status}{(b.Replay ? " (replay)" : " (senza header di replay)")}; {AppliedText(applied)}");
            }
            case "conflict":
            {
                var body = Body(id, ep);
                var a = await Post(steps, "Prima richiesta", node, ep.Path, rid, body, null, ct);
                var b = await Post(steps, "Stesso ID, uno spazio in più nel JSON", node, ep.Path, rid, Respaced(body), null, ct);
                var applied = await _verifier.AppliedRowsForRequestAsync(rid, ct);
                return (a.Status == 200 && b.Status == 422 && applied is null or 1, $"Risposte {a.Status} e {b.Status}; {AppliedText(applied)}");
            }
            case "parallel":
            {
                var body = Body(id, ep);
                var nodes = _monitor.Reachable();
                var tasks = Enumerable.Range(0, 10).Select(i => _client.PostDataAsync(nodes[i % nodes.Count].Url, ep.Path, rid, body, _opt.RequestTimeoutMs, ct: ct)).ToList();
                var res = await Task.WhenAll(tasks);
                for (int i = 0; i < res.Length; i++)
                    steps.Add(new ScenarioStep($"Richiesta {i + 1}", nodes[i % nodes.Count].Port, res[i].Status, res[i].Replay, res[i].ElapsedMs, res[i].Error));
                var applied = await _verifier.AppliedRowsForRequestAsync(rid, ct);
                int ok200 = res.Count(r => r.Status == 200);
                return (ok200 == 10 && applied is null or 1, $"{ok200}/10 risposte 200 ({res.Count(r => r.Replay)} replay); {AppliedText(applied)}");
            }
            case "via-follower":
            {
                var follower = _monitor.Reachable().FirstOrDefault(n => n.Role != "Leader")
                               ?? throw new SimulatorException("Nessun Follower raggiungibile");
                var r = await Post(steps, "POST al Follower", follower, ep.Path, rid, Body(id, ep), null, ct);
                var leaked = r.Headers.Keys.Where(k => k.StartsWith("X-Forward", StringComparison.OrdinalIgnoreCase)
                                                    || k.StartsWith("X-Raft", StringComparison.OrdinalIgnoreCase)
                                                    || k.StartsWith("X-Cluster", StringComparison.OrdinalIgnoreCase)).ToList();
                return (r.Status == 200 && leaked.Count == 0,
                    $"Risposta {r.Status}; {(leaked.Count == 0 ? "nessun header interno" : "header interni esposti: " + string.Join(", ", leaked))}");
            }
            case "missing-id":
            {
                var r = await Post(steps, "POST senza X-Request-ID", node, ep.Path, null, Body(id, ep), null, ct);
                return (r.Status == 400, $"Risposta {r.Status}");
            }
            case "invalid-id":
            {
                var r = await Post(steps, "X-Request-ID: non-un-uuid", node, ep.Path, "non-un-uuid", Body(id, ep), null, ct);
                return (r.Status == 400, $"Risposta {r.Status}");
            }
            case "invalid-json":
            {
                var r = await Post(steps, "Body {\"a\":", node, ep.Path, rid, Encoding.UTF8.GetBytes("{\"a\":"), null, ct);
                return (r.Status == 400, $"Risposta {r.Status}");
            }
            case "too-large":
            {
                var big = Body(id, ep, _opt.MaxBodyBytes);
                var r = await Post(steps, $"Body da {big.Length:N0} byte", node, ep.Path, rid, big, null, ct);
                var applied = await _verifier.AppliedRowsForRequestAsync(rid, ct);
                return (r.Status == 413 && applied is null or 0, $"Risposta {r.Status}; {AppliedText(applied)}");
            }
            case "spoofed-headers":
            {
                var headers = new Dictionary<string, string> { ["X-Forward-Hops"] = "5", ["X-Forwarded-By"] = Guid.NewGuid().ToString("D") };
                var r = await Post(steps, "POST con X-Forward-Hops: 5", node, ep.Path, rid, Body(id, ep), headers, ct);
                return (r.Status == 200, $"Risposta {r.Status}{(r.Status == 421 ? ": il client vede 421, contrario a REQ-API-2" : "")}");
            }
            case "no-content-type":
            {
                // DataClient imposta sempre Content-Type: qui si usa una richiesta costruita a mano
                using var req = new HttpRequestMessage(HttpMethod.Post, node.Url + ep.Path) { Content = new ByteArrayContent(Body(id, ep)) };
                req.Headers.TryAddWithoutValidation("X-Request-ID", rid);
                var sw = System.Diagnostics.Stopwatch.StartNew();
                int status;
                try { using var res = await _client.Http.SendAsync(req, ct); status = (int)res.StatusCode; }
                catch (HttpRequestException ex) { steps.Add(new ScenarioStep("POST senza Content-Type", node.Port, 0, false, sw.ElapsedMilliseconds, ex.Message)); return (false, "Errore di rete"); }
                steps.Add(new ScenarioStep("POST senza Content-Type", node.Port, status, false, sw.ElapsedMilliseconds, null));
                return (status == 200, $"Risposta {status}");
            }
            case "cross-endpoint":
            {
                // il secondo endpoint è il successivo nell'elenco, così con «tutti» ogni endpoint fa da primo una volta
                var eps = _monitor.KnownEndpoints();
                if (eps.Count < 2) throw new SimulatorException("Servono almeno due endpoint configurati");
                var other = eps[(eps.ToList().FindIndex(e => e.Name == ep.Name) + 1) % eps.Count];
                var body = Body(id, ep);
                var a = await Post(steps, $"POST {ep.Path}", node, ep.Path, rid, body, null, ct);
                var b = await Post(steps, $"Stesso ID e stessi byte su {other.Path}", node, other.Path, rid, body, null, ct);
                var applied = await _verifier.AppliedRowsForRequestAsync(rid, ct);
                return (a.Status == 200 && b.Status == 422 && applied is null or 1, $"Risposte {a.Status} e {b.Status}; {AppliedText(applied)}");
            }
            case "unknown-path":
            {
                var path = "/api/v1/non-configurato-" + rid[..8];
                var r = await Post(steps, $"POST {path}", node, path, rid, Body(id, ep), null, ct);
                var applied = await _verifier.AppliedRowsForRequestAsync(rid, ct);
                return (r.Status == 404 && applied is null or 0, $"Risposta {r.Status}; {AppliedText(applied)}");
            }
            default: throw new SimulatorException($"Scenario '{id}' non implementato");
        }
    }

    private static string AppliedText(int? applied) => applied switch
    {
        null => "applied.log non disponibile: conteggio delle scritture non verificato",
        0 => "nessuna voce applicata",
        1 => "una sola voce applicata",
        _ => $"{applied} voci applicate (DUPLICATO)"
    };
}
