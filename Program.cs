using System.Text.Json;
using RaftCore.Abstractions;
using RaftCore.Configuration;
using RaftCore.Models;
using RaftCore.Services.Common;
using RaftCore.Services.Core;
using RaftCore.Services.Http;
using RaftCore.Services.Network;
using RaftCore.Services.Storage;

var builder = WebApplication.CreateBuilder(args);
builder.Logging.AddSimpleConsole(o => { o.IncludeScopes = true; o.SingleLine = true; o.TimestampFormat = "HH:mm:ss.fff "; });

// ---- Configurazione (REQ-CFG-1, REQ-SEC-1)
var config = builder.Configuration.GetSection("RaftCluster").Get<ClusterConfig>() ?? new ClusterConfig();
config.ApplyEnvironment();
if (config.MembershipMode == MembershipMode.Bootstrap && string.IsNullOrWhiteSpace(config.NodeId))
{
    // REQ-ID-1: in Bootstrap il NodeId è generato al primo avvio e salvato in NodeIdFile
    var f = Path.GetFullPath(config.NodeIdFile);
    Directory.CreateDirectory(Path.GetDirectoryName(f)!);
    if (File.Exists(f)) config.NodeId = File.ReadAllText(f).Trim();
    else { config.NodeId = Guid.NewGuid().ToString("D"); File.WriteAllText(f, config.NodeId); }
}
try { config.Validate(); }
catch (ConfigurationValidationException ex)
{
    Console.Error.WriteLine("ERRORE DI CONFIGURAZIONE - il nodo non parte (REQ-CFG-1):");
    foreach (var v in ex.Violations) Console.Error.WriteLine(" - " + v);
    return 1;
}

builder.WebHost.UseUrls($"http://0.0.0.0:{config.RestPort}");
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromMilliseconds(config.ShutdownGraceMs + 3000));

// ---- DI
IEnumerable<string>? bootstrapMembers = null;
if (config.MembershipMode == MembershipMode.Bootstrap)
{
    var st = new RaftStateStore(config.Storage.DataDir);
    if (st.Exists) bootstrapMembers = st.Load().Members;
}
builder.Services.AddSingleton(config);
builder.Services.AddSingleton(new Membership(config, config.NodeId, bootstrapMembers));
builder.Services.AddSingleton<IClock, SystemClock>();
builder.Services.AddSingleton<Metrics>();
builder.Services.AddSingleton<PeerRegistry>();
builder.Services.AddSingleton<IDataRepository>(_ => new FileDataRepository(config.Storage.ResolvedOutputPath, config.Storage.OutputMaxFileBytes, config.Storage.FsyncOnCommit));
builder.Services.AddHttpClient("raft-rpc").ConfigureHttpClient(c => c.Timeout = Timeout.InfiniteTimeSpan);
builder.Services.AddHttpClient("forward").ConfigureHttpClient(c => c.Timeout = Timeout.InfiniteTimeSpan);
builder.Services.AddSingleton<IPeerTransport, HttpPeerTransport>();
builder.Services.AddSingleton<IUdpChannel>(_ => new UdpChannel(config));
builder.Services.AddSingleton<RaftNode>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<RaftNode>());
builder.Services.AddSingleton<UdpDiscoveryService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<UdpDiscoveryService>());
builder.Services.AddSingleton<ClientApi>();

var app = builder.Build();
var node = app.Services.GetRequiredService<RaftNode>();
try { node.Initialize(); }                                    // REQ-WAL-3/4: recovery PRIMA di servire traffico
catch (StartupBlockedException ex)
{
    app.Logger.LogCritical("EVENTO ALTA SEVERITÀ: {Msg}", ex.Message);
    return 2;
}

app.UseMiddleware<SecurityMiddleware>();

// ---- Health (REQ-OPS-1/2)
app.MapGet("/health/live", () => Results.Ok(new { status = "ALIVE" }));
app.MapGet("/health/ready", (RaftNode n) =>
{
    var (ready, reason) = n.GetReadiness();
    return Results.Json(new { ready, reason }, statusCode: ready ? 200 : 503);
});
app.MapGet("/api/v1/cluster/status", (RaftNode n) => Results.Json(n.GetStatus()));
app.MapGet("/api/v1/metrics", (RaftNode n) => Results.Json(n.GetMetrics()));

// ---- RPC interne (autenticate dal SecurityMiddleware)
app.MapPost("/internal/v1/prevote", (PreVoteRequest r, RaftNode n) => Results.Json(n.HandlePreVote(r)));
app.MapPost("/internal/v1/vote", (VoteRequest r, RaftNode n) => Results.Json(n.HandleVote(r)));
app.MapPost("/internal/v1/append", (AppendEntriesRequest r, RaftNode n) =>
{
    var (status, resp) = n.HandleAppend(r);
    return status == 200 ? Results.Json(resp) : Results.StatusCode(status);
});

// ---- API pubblica
app.MapPost("/api/v1/data", (HttpContext ctx, ClientApi api) => api.HandleAsync(ctx));

await app.RunAsync();
return 0;
