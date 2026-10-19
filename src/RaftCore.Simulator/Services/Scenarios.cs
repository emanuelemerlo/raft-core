using System.Text;

namespace RaftCore.Simulator.Services;

public sealed record ScenarioInfo(string Id, string Title, string Expected, string Spec);

public sealed record ScenarioStep(string Label, int Port, int Status, bool Replay, long Ms, string? Note);

public sealed record ScenarioResult(string Id, string Title, string Expected, bool Passed, string Summary, IReadOnlyList<ScenarioStep> Steps, DateTimeOffset At);

/// <summary>Casi del contratto client e dell'idempotenza (§12, §14, §15 della specifica), eseguiti su richiesta.</summary>
public sealed class ScenarioRunner
{
    private readonly SimulatorOptions _opt;
    private readonly DataClient _client;
    private readonly ClusterMonitor _monitor;
    private readonly AppliedLogVerifier _verifier;
    private readonly LoadGenerator _gen;
    private readonly EventLog _events;
    private readonly List<ScenarioResult> _history = new();

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
        new ScenarioInfo("spoofed-headers", "Client che invia X-Forward-Hops e X-Forwarded-By", "200: header di inoltro non firmati ignorati (mai 421 al client)", "REQ-API-2 · rev. 6 PRX-3/6"),
        new ScenarioInfo("no-content-type", "Body JSON senza Content-Type", "200: conta il body, non l'header", "REQ-API-1"),
    };

    public ScenarioRunner(SimulatorOptions opt, DataClient client, ClusterMonitor monitor, AppliedLogVerifier verifier, LoadGenerator gen, EventLog events)
    { _opt = opt; _client = client; _monitor = monitor; _verifier = verifier; _gen = gen; _events = events; }

    public IReadOnlyList<ScenarioResult> History() { lock (_history) return _history.ToList(); }

    public async Task<ScenarioResult> RunAsync(string id, CancellationToken ct)
    {
        var info = All.FirstOrDefault(s => s.Id == id) ?? throw new SimulatorException($"Scenario '{id}' sconosciuto");
        var steps = new List<ScenarioStep>();
        bool passed; string summary;
        try { (passed, summary) = await RunCoreAsync(id, steps, ct); }
        catch (SimulatorException ex) { passed = false; summary = ex.Message; }
        var result = new ScenarioResult(info.Id, info.Title, info.Expected, passed, summary, steps, DateTimeOffset.Now);
        lock (_history) { _history.Insert(0, result); if (_history.Count > 50) _history.RemoveAt(_history.Count - 1); }
        if (passed) _events.Ok($"Scenario «{info.Title}»: superato. {summary}");
        else _events.Error($"Scenario «{info.Title}»: NON superato. {summary}");
        return result;
    }

    private NodeView Entry(bool preferFollower = true)
    {
        var nodes = _monitor.Reachable();
        if (nodes.Count == 0) throw new SimulatorException("Nessun nodo raggiungibile: avviare il cluster");
        return (preferFollower ? nodes.FirstOrDefault(n => n.Role != "Leader") : null) ?? nodes[0];
    }

    private byte[] Body(string scenario, string extra = "") =>
        Encoding.UTF8.GetBytes($"{{\"scenario\":\"{scenario}\",\"timestamp\":\"{DateTimeOffset.Now:O}\",\"runId\":\"{_gen.RunId}\"{extra}}}");

    private async Task<PostResult> Post(List<ScenarioStep> steps, string label, NodeView node, string? id, byte[] body,
        IEnumerable<KeyValuePair<string, string>>? headers, CancellationToken ct, string? note = null)
    {
        var r = await _client.PostDataAsync(node.Url, id, body, _opt.RequestTimeoutMs, headers, ct);
        steps.Add(new ScenarioStep(label, node.Port, r.Status, r.Replay, r.ElapsedMs, note ?? r.Error ?? (r.Status >= 300 ? r.Body : null)));
        return r;
    }

    private async Task<(bool, string)> RunCoreAsync(string id, List<ScenarioStep> steps, CancellationToken ct)
    {
        var node = Entry();
        string rid = Guid.NewGuid().ToString("D");
        switch (id)
        {
            case "replay":
            {
                var body = Body(id);
                var a = await Post(steps, "Prima richiesta", node, rid, body, null, ct);
                var b = await Post(steps, "Retry: stesso ID, stessi byte", node, rid, body, null, ct);
                var applied = await _verifier.AppliedRowsForRequestAsync(rid, ct);
                bool ok = a.Status == 200 && b.Status == 200 && b.Replay && applied is null or 1;
                return (ok, $"Risposte {a.Status} e {b.Status}{(b.Replay ? " (replay)" : " (senza header di replay)")}; {AppliedText(applied)}");
            }
            case "conflict":
            {
                var a = await Post(steps, "Prima richiesta", node, rid, Encoding.UTF8.GetBytes("{\"q\":1}"), null, ct);
                var b = await Post(steps, "Stesso ID, body {\"q\": 1}", node, rid, Encoding.UTF8.GetBytes("{\"q\": 1}"), null, ct);
                var applied = await _verifier.AppliedRowsForRequestAsync(rid, ct);
                return (a.Status == 200 && b.Status == 422 && applied is null or 1, $"Risposte {a.Status} e {b.Status}; {AppliedText(applied)}");
            }
            case "parallel":
            {
                var body = Body(id);
                var nodes = _monitor.Reachable();
                var tasks = Enumerable.Range(0, 10).Select(i => _client.PostDataAsync(nodes[i % nodes.Count].Url, rid, body, _opt.RequestTimeoutMs, ct: ct)).ToList();
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
                var r = await Post(steps, "POST al Follower", follower, rid, Body(id), null, ct);
                var leaked = r.Headers.Keys.Where(k => k.StartsWith("X-Forward", StringComparison.OrdinalIgnoreCase)
                                                    || k.StartsWith("X-Raft", StringComparison.OrdinalIgnoreCase)).ToList();
                return (r.Status == 200 && leaked.Count == 0,
                    $"Risposta {r.Status}; {(leaked.Count == 0 ? "nessun header interno" : "header interni esposti: " + string.Join(", ", leaked))}");
            }
            case "missing-id":
            {
                var r = await Post(steps, "POST senza X-Request-ID", node, null, Body(id), null, ct);
                return (r.Status == 400, $"Risposta {r.Status}");
            }
            case "invalid-id":
            {
                var r = await Post(steps, "X-Request-ID: non-un-uuid", node, "non-un-uuid", Body(id), null, ct);
                return (r.Status == 400, $"Risposta {r.Status}");
            }
            case "invalid-json":
            {
                var r = await Post(steps, "Body {\"a\":", node, rid, Encoding.UTF8.GetBytes("{\"a\":"), null, ct);
                return (r.Status == 400, $"Risposta {r.Status}");
            }
            case "too-large":
            {
                var big = Body(id, ",\"pad\":\"" + new string('x', _opt.MaxBodyBytes) + "\"");
                var r = await Post(steps, $"Body da {big.Length:N0} byte", node, rid, big, null, ct);
                var applied = await _verifier.AppliedRowsForRequestAsync(rid, ct);
                return (r.Status == 413 && applied is null or 0, $"Risposta {r.Status}; {AppliedText(applied)}");
            }
            case "spoofed-headers":
            {
                var headers = new Dictionary<string, string> { ["X-Forward-Hops"] = "5", ["X-Forwarded-By"] = Guid.NewGuid().ToString("D") };
                var r = await Post(steps, "POST con X-Forward-Hops: 5", node, rid, Body(id), headers, ct);
                return (r.Status == 200, $"Risposta {r.Status}{(r.Status == 421 ? ": il client vede 421, contrario a REQ-API-2" : "")}");
            }
            case "no-content-type":
            {
                // DataClient imposta sempre Content-Type: qui si usa una richiesta costruita a mano
                var body = Body(id);
                using var req = new HttpRequestMessage(HttpMethod.Post, node.Url + "/api/v1/data") { Content = new ByteArrayContent(body) };
                req.Headers.TryAddWithoutValidation("X-Request-ID", rid);
                var sw = System.Diagnostics.Stopwatch.StartNew();
                int status;
                try { using var res = await _client.Http.SendAsync(req, ct); status = (int)res.StatusCode; }
                catch (HttpRequestException ex) { status = 0; steps.Add(new ScenarioStep("POST senza Content-Type", node.Port, 0, false, sw.ElapsedMilliseconds, ex.Message)); return (false, "Errore di rete"); }
                steps.Add(new ScenarioStep("POST senza Content-Type", node.Port, status, false, sw.ElapsedMilliseconds, null));
                return (status == 200, $"Risposta {status}");
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
