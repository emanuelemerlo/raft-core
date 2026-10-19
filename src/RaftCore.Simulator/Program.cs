using RaftCore.Simulator.Services;

// Simulatore di client per il cluster RaftCore di sviluppo: invio ciclico, scenari del contratto client,
// guasti a livello di processo e verifica delle scritture. Interfaccia su http://localhost:5100.
var builder = WebApplication.CreateBuilder(args);
builder.Logging.AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss.fff "; });

var options = builder.Configuration.GetSection("Simulator").Get<SimulatorOptions>() ?? new SimulatorOptions();
builder.Services.AddSingleton(options);
builder.Services.AddSingleton<PathResolver>();
builder.Services.AddSingleton<EventLog>();
builder.Services.AddSingleton<DataClient>();
builder.Services.AddSingleton<ProcessControl>();
builder.Services.AddSingleton<ClusterMonitor>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<ClusterMonitor>());
builder.Services.AddSingleton<LoadGenerator>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<LoadGenerator>());
builder.Services.AddSingleton<AppliedLogVerifier>();
builder.Services.AddSingleton<ScenarioRunner>();

var app = builder.Build();
app.UseDefaultFiles();
app.UseStaticFiles();

// Errori come JSON con un messaggio leggibile dall'interfaccia: 409 per quelli di dominio (nodo sconosciuto, PID non
// trovato, ...), 500 per gli altri (es. accesso negato a un processo avviato da un Visual Studio in esecuzione come amministratore)
app.Use(async (ctx, next) =>
{
    try { await next(); }
    catch (SimulatorException ex) when (!ctx.Response.HasStarted)
    {
        ctx.Response.StatusCode = 409;
        await ctx.Response.WriteAsJsonAsync(new { error = ex.Message });
    }
    catch (Exception ex) when (!ctx.Response.HasStarted && !ctx.RequestAborted.IsCancellationRequested)
    {
        app.Logger.LogError(ex, "Errore in {Path}", ctx.Request.Path);
        ctx.Response.StatusCode = 500;
        await ctx.Response.WriteAsJsonAsync(new { error = $"{ex.GetType().Name}: {ex.Message}" });
    }
});

var api = app.MapGroup("/sim");

api.MapGet("/state", (ClusterMonitor m, LoadGenerator g, EventLog e, AppliedLogVerifier v, ScenarioRunner s) => Results.Json(new
{
    now = DateTimeOffset.Now,
    nodes = m.Snapshot(),
    leaderId = m.Leader()?.NodeId,
    scanned = m.ScannedTargets,
    generator = g.State(),
    events = e.Recent(120),
    appliedLog = new { path = v.FilePath, available = v.Available },
    scenarios = ScenarioRunner.All,
    scenarioHistory = s.History().Take(20)
}));

// ---- invio ciclico
api.MapPost("/generator/start", (GeneratorSettings? s, LoadGenerator g) => { if (s != null) g.Configure(s); g.Start(); return Results.Ok(); });
api.MapPost("/generator/stop", (LoadGenerator g) => { g.Stop(); return Results.Ok(); });
api.MapPost("/generator/settings", (GeneratorSettings s, LoadGenerator g) => { g.Configure(s); return Results.Ok(g.Settings); });
api.MapPost("/generator/one", async (LoadGenerator g) =>
{
    var op = await g.SendOneAsync();
    return Results.Ok(new { op.Seq, op.RequestId, outcome = op.Outcome.ToString(), op.FinalStatus, op.LatencyMs, path = op.Path });
});
api.MapPost("/generator/burst", (int? count, LoadGenerator g) => Results.Ok(new { sent = g.Burst(count ?? 50) }));
api.MapPost("/generator/reset", (LoadGenerator g) => { g.ResetStats(); return Results.Ok(); });
api.MapGet("/ops.csv", (LoadGenerator g) => Results.Text(g.OpsCsv(), "text/csv; charset=utf-8"));

// ---- scenari
api.MapPost("/scenarios/{id}", async (string id, ScenarioRunner s, CancellationToken ct) => Results.Json(await s.RunAsync(id, ct)));

// ---- guasti sui processi dei nodi
api.MapPost("/nodes/{nodeId}/kill", (string nodeId, ClusterMonitor m, LoadGenerator g) =>
{
    var msg = m.Kill(nodeId); g.MarkFault(msg); return Results.Ok(new { message = msg });
});
api.MapPost("/nodes/{nodeId}/pause", (string nodeId, int? seconds, ClusterMonitor m, LoadGenerator g) =>
{
    var msg = m.Pause(nodeId, seconds ?? 5); g.MarkFault(msg); return Results.Ok(new { message = msg });
});
api.MapPost("/nodes/{nodeId}/resume", (string nodeId, ClusterMonitor m) => Results.Ok(new { message = m.Resume(nodeId) }));
api.MapPost("/nodes/{nodeId}/restart", (string nodeId, ClusterMonitor m) => Results.Ok(new { message = m.Restart(nodeId) }));
api.MapPost("/leader/{action}", (string action, int? seconds, ClusterMonitor m, LoadGenerator g) =>
{
    var leader = m.Leader() ?? throw new SimulatorException("Nessun Leader visibile in questo momento");
    var msg = action switch
    {
        "kill" => m.Kill(leader.NodeId),
        "pause" => m.Pause(leader.NodeId, seconds ?? 5),
        _ => throw new SimulatorException($"Azione '{action}' sconosciuta")
    };
    g.MarkFault(msg);
    return Results.Ok(new { message = msg });
});

// ---- verifica delle scritture
api.MapPost("/verify", async (AppliedLogVerifier v, LoadGenerator g, ClusterMonitor m, CancellationToken ct) =>
    Results.Json(await v.VerifyAsync(g, m, ct)));

app.Lifetime.ApplicationStopping.Register(() => app.Services.GetRequiredService<ProcessControl>().Dispose());
app.Logger.LogInformation("Simulatore pronto: {Targets} indirizzi sondati, applied.log in {Path}",
    app.Services.GetRequiredService<ClusterMonitor>().ScannedTargets.Count, app.Services.GetRequiredService<AppliedLogVerifier>().FilePath);
app.Run();
