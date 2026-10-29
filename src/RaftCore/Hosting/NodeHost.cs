using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Hosting.Systemd;
using Microsoft.Extensions.Hosting.WindowsServices;
using RaftCore.Abstractions;
using RaftCore.Configuration;
using RaftCore.Models;
using RaftCore.Services.Common;
using RaftCore.Services.Core;
using RaftCore.Services.Http;
using RaftCore.Services.Network;
using RaftCore.Services.Storage;

namespace RaftCore.Hosting;

public enum ExitCodes { Ok = 0, Configuration = 1, StartupBlocked = 2, DataDirInUse = 3, NoFreePort = 4 }

public sealed class NodeStartException : Exception
{
    public ExitCodes Code { get; }
    public NodeStartException(ExitCodes code, string message, Exception? inner = null) : base(message, inner) => Code = code;
}

/// <summary>Un nodo avviato: applicazione web, configurazione effettiva e lock della DataDir.</summary>
public sealed class RunningNode : IAsyncDisposable
{
    public WebApplication App { get; }
    public ClusterConfig Config { get; }
    public RaftNode Raft => App.Services.GetRequiredService<RaftNode>();
    public string Url => $"http://127.0.0.1:{Config.RestPort}";
    private readonly DataDirLock _lock;
    private bool _stopped;

    internal RunningNode(WebApplication app, ClusterConfig cfg, DataDirLock l) { App = app; Config = cfg; _lock = l; }

    public async Task StopAsync()
    {
        if (_stopped) return;
        _stopped = true;
        try { await App.StopAsync(); } catch { }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        await App.DisposeAsync();
        _lock.Dispose();
    }
}

/// <summary>
/// Costruzione e avvio di un nodo: usata da Program (processo/servizio) e dai test di integrazione (più nodi in-process).
/// </summary>
public static class NodeHost
{
    public static bool IsService => WindowsServiceHelpers.IsWindowsService() || SystemdHelpers.IsSystemdService();

    /// <param name="configure">Override dei servizi (test: trasporto con fault injection, repository, orologio).</param>
    /// <param name="settings">Override della configurazione (test).</param>
    public static async Task<RunningNode> StartAsync(string[] args, Action<IServiceCollection, ClusterConfig>? configure = null,
        IDictionary<string, string?>? settings = null, CancellationToken ct = default, string? contentRoot = null)
    {
        _contentRoot.Value = contentRoot;
        // fsync e lock sincroni occupano thread del pool: con pochi core la crescita lenta del pool introdurrebbe
        // pause di centinaia di ms, sufficienti a far scadere il lease del Leader (elezioni spurie).
        ThreadPool.GetMinThreads(out var minWorker, out var minIo);
        ThreadPool.SetMinThreads(Math.Max(minWorker, Math.Max(32, 4 * Environment.ProcessorCount)), Math.Max(minIo, 32));
        var probe = BuildConfig(args, settings);
        var ports = probe.CandidatePorts();
        var errors = new List<string>();
        foreach (var port in ports)
        {
            ct.ThrowIfCancellationRequested();
            if (!NetworkHelper.IsTcpPortFree(port)) { errors.Add($"{port}: occupata"); continue; }
            try { return await TryStartOnPortAsync(args, settings, port, configure, ct); }
            catch (PortInUseException ex) { errors.Add($"{port}: {ex.Message}"); }
        }
        throw new NodeStartException(ExitCodes.NoFreePort,
            "Nessuna porta REST disponibile nell'intervallo configurato (" + string.Join(", ", errors) + ").");
    }

    private sealed class PortInUseException : Exception { public PortInUseException(string m, Exception inner) : base(m, inner) { } }

    private static ClusterConfig BuildConfig(string[] args, IDictionary<string, string?>? settings)
    {
        var b = WebApplication.CreateBuilder(Options(args));
        if (settings != null) b.Configuration.AddInMemoryCollection(settings);
        var cfg = b.Configuration.GetSection("RaftCluster").Get<ClusterConfig>() ?? new ClusterConfig();
        cfg.ApplyEnvironment();
        return cfg;
    }

    private static readonly AsyncLocal<string?> _contentRoot = new();

    private static WebApplicationOptions Options(string[] args) => new()
    {
        Args = args,
        // appsettings.json accanto all'eseguibile, anche quando avviato come servizio o da un'altra directory
        ContentRootPath = _contentRoot.Value ?? AppContext.BaseDirectory
    };

    private static async Task<RunningNode> TryStartOnPortAsync(string[] args, IDictionary<string, string?>? settings, int port,
        Action<IServiceCollection, ClusterConfig>? configure, CancellationToken ct)
    {
        var builder = WebApplication.CreateBuilder(Options(args));
        if (settings != null) builder.Configuration.AddInMemoryCollection(settings);
        builder.Logging.AddSimpleConsole(o => { o.IncludeScopes = false; o.SingleLine = true; o.TimestampFormat = "HH:mm:ss.fff "; });
        // Le RPC interne sono decine al secondo: i log Information di HttpClient renderebbero illeggibile l'output
        builder.Logging.AddFilter("System.Net.Http.HttpClient", LogLevel.Warning);
        builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);
        builder.Services.AddWindowsService(o => o.ServiceName = "RaftCore");
        builder.Services.AddSystemd();

        // ---- Configurazione (REQ-CFG-1, REQ-SEC-1)
        var config = builder.Configuration.GetSection("RaftCluster").Get<ClusterConfig>() ?? new ClusterConfig();
        config.ApplyEnvironment();
        string baseDir = !string.IsNullOrWhiteSpace(config.BaseDirectory) ? config.BaseDirectory!
            : IsService ? AppContext.BaseDirectory : Directory.GetCurrentDirectory();
        config.ResolvePlaceholdersAndPaths(port, baseDir);
        if (config.MembershipMode == MembershipMode.Bootstrap && string.IsNullOrWhiteSpace(config.NodeId))
        {
            // REQ-ID-1: in Bootstrap il NodeId è generato al primo avvio e salvato in NodeIdFile
            Directory.CreateDirectory(Path.GetDirectoryName(config.NodeIdFile)!);
            if (File.Exists(config.NodeIdFile)) config.NodeId = File.ReadAllText(config.NodeIdFile).Trim();
            else { config.NodeId = Guid.NewGuid().ToString("D"); File.WriteAllText(config.NodeIdFile, config.NodeId); }
        }
        try
        {
            config.Validate();
            if (!builder.Environment.IsDevelopment() && config.ClusterSecret.StartsWith("dev-only-", StringComparison.OrdinalIgnoreCase))
                throw new ConfigurationValidationException(new[] { "ClusterSecret di sviluppo (dev-only-...) non ammesso fuori dall'ambiente Development (REQ-SEC-1)." });
        }
        catch (ConfigurationValidationException ex) { throw new NodeStartException(ExitCodes.Configuration, ex.Message, ex); }

        DataDirLock dataLock;
        // nodo figlio del cluster di sviluppo: l'istanza della sessione precedente può impiegare un attimo a chiudersi
        var lockDeadline = DateTime.UtcNow.AddSeconds(config.DevCluster.ParentPid > 0 ? 10 : 0);
        while (true)
        {
            try { dataLock = DataDirLock.Acquire(config.Storage.DataDir); break; }
            catch (IOException) when (DateTime.UtcNow < lockDeadline) { await Task.Delay(250, ct); }
            catch (IOException ex) { throw new NodeStartException(ExitCodes.DataDirInUse, ex.Message, ex); }
        }

        try
        {
            builder.WebHost.UseUrls($"http://0.0.0.0:{config.RestPort}");
            builder.WebHost.ConfigureKestrel(k => k.Limits.MaxRequestBodySize = 100L * 1024 * 1024);
            builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromMilliseconds(config.ShutdownGraceMs + 3000));

            // ---- DI
            IEnumerable<string>? bootstrapMembers = null;
            if (config.MembershipMode == MembershipMode.Bootstrap)
            {
                var st = new RaftStateStore(config.Storage.DataDir);
                if (st.Exists) bootstrapMembers = st.Load().Members;
            }
            var membership = new Membership(config, config.NodeId, bootstrapMembers);
            builder.Services.AddSingleton(config);
            builder.Services.AddSingleton(membership);
            builder.Services.AddSingleton<IClock, SystemClock>();
            builder.Services.AddSingleton<Metrics>();
            builder.Services.AddSingleton<PeerRegistry>();
            builder.Services.AddSingleton<RpcVerifier>();
            builder.Services.AddSingleton<IDataRepository>(sp => config.StateMachine.Provider switch
            {
                StateMachineProvider.SqlServer => new SqlServerDataRepository(config.StateMachine.SqlServer, config.ClusterId, membership.SelfId,
                    sp.GetService<ISqlPayloadWriter>()),
                _ => new SharedFileDataRepository(config.StateMachine.SharedFile, config.Storage.FsyncOnCommit)
            });
            builder.Services.AddHttpClient("raft-rpc").ConfigureHttpClient(c => c.Timeout = Timeout.InfiniteTimeSpan);
            builder.Services.AddHttpClient("forward").ConfigureHttpClient(c => c.Timeout = Timeout.InfiniteTimeSpan);
            builder.Services.AddSingleton<IPeerTransport, HttpPeerTransport>();
            builder.Services.AddSingleton<IUdpChannel>(_ => config.UdpMode == UdpMode.Disabled ? new NullUdpChannel() : new UdpChannel(config));
            builder.Services.AddSingleton<RaftNode>();
            builder.Services.AddHostedService(sp => sp.GetRequiredService<RaftNode>());
            builder.Services.AddSingleton<UdpDiscoveryService>();
            builder.Services.AddHostedService(sp => sp.GetRequiredService<UdpDiscoveryService>());
            builder.Services.AddHttpClient("probe").ConfigureHttpClient(c => c.Timeout = Timeout.InfiniteTimeSpan);
            builder.Services.AddSingleton<PeerProbeService>();
            builder.Services.AddHostedService(sp => sp.GetRequiredService<PeerProbeService>());
            builder.Services.AddSingleton<ClientApi>();
            configure?.Invoke(builder.Services, config);

            var app = builder.Build();
            var node = app.Services.GetRequiredService<RaftNode>();
            try { node.Initialize(); }                                    // REQ-WAL-3/4: recovery PRIMA di servire traffico
            catch (StartupBlockedException ex)
            {
                await app.DisposeAsync();
                throw new NodeStartException(ExitCodes.StartupBlocked, ex.Message, ex);
            }
            MapEndpoints(app);

            try { await app.StartAsync(ct); }
            catch (Exception ex) when (IsAddressInUse(ex))
            {
                try { await app.StopAsync(CancellationToken.None); } catch { }
                await app.DisposeAsync();
                throw new PortInUseException(ex.Message, ex);
            }
            app.Logger.LogInformation("Nodo {Node} avviato su {Url} (cluster {Cluster}, {Size} membri, state machine {Sm}, dati in {Dir})",
                membership.SelfId, NetworkHelper.ResolveAdvertiseUrl(config), config.ClusterId, config.ClusterSize, config.StateMachine.Provider, config.Storage.DataDir);
            return new RunningNode(app, config, dataLock);
        }
        catch
        {
            dataLock.Dispose();
            throw;
        }
    }

    private static bool IsAddressInUse(Exception? ex)
    {
        for (; ex != null; ex = ex.InnerException)
            if (ex is Microsoft.AspNetCore.Connections.AddressInUseException
                || ex is System.Net.Sockets.SocketException { SocketErrorCode: System.Net.Sockets.SocketError.AddressAlreadyInUse }) return true;
        return false;
    }

    /// <summary>
    /// true se il chiamante può vedere gli endpoint informativi: o sono esposti esplicitamente in configurazione,
    /// o la richiesta proviene da loopback. Gestisce anche gli indirizzi IPv4 mappati su IPv6 (::ffff:127.0.0.1).
    /// </summary>
    private static bool ManagementAllowed(HttpContext ctx, ClusterConfig cfg)
    {
        if (cfg.Management.ExposeStatusEndpoints) return true;
        var ip = ctx.Connection.RemoteIpAddress;
        if (ip is null) return false;
        if (ip.IsIPv4MappedToIPv6) ip = ip.MapToIPv4();
        return System.Net.IPAddress.IsLoopback(ip);
    }

    private static void MapEndpoints(WebApplication app)
    {
        app.UseMiddleware<SecurityMiddleware>();

        // ---- Health (REQ-OPS-1/2)
        app.MapGet("/health/live", () => Results.Ok(new { status = "ALIVE" }));
        app.MapGet("/health/ready", (RaftNode n) =>
        {
            var (ready, reason) = n.GetReadiness();
            return Results.Json(new { ready, reason }, statusCode: ready ? 200 : 503);
        });
        // Endpoint informativi: espongono topologia e metriche. Per default raggiungibili solo da loopback
        // (Management:ExposeStatusEndpoints per aprirli quando l'accesso è già ristretto a monte).
        app.MapGet("/api/v1/cluster/status", (HttpContext ctx, RaftNode n, ClusterConfig cfg) =>
            ManagementAllowed(ctx, cfg) ? Results.Json(n.GetStatus()) : Results.StatusCode(403));
        app.MapGet("/api/v1/metrics", (HttpContext ctx, RaftNode n, ClusterConfig cfg) =>
            ManagementAllowed(ctx, cfg) ? Results.Json(n.GetMetrics()) : Results.StatusCode(403));

        // ---- RPC interne (autenticate e firmate dal SecurityMiddleware)
        app.MapPost("/internal/v1/prevote", (PreVoteRequest r, RaftNode n) => Results.Json(n.HandlePreVote(r)));
        app.MapPost("/internal/v1/vote", (VoteRequest r, RaftNode n) => Results.Json(n.HandleVote(r)));
        app.MapPost("/internal/v1/append", (AppendEntriesRequest r, RaftNode n) =>
        {
            var (status, resp) = n.HandleAppend(r);
            return status == 200 ? Results.Json(resp) : Results.StatusCode(status);
        });
        app.MapPost(RpcAuth.HelloPath, (HelloRequest r, RaftNode n) =>
        {
            var (role, term) = n.GetRoleAndTerm();
            return Results.Json(new HelloResponse { NodeId = n.SelfId, Url = n.SelfUrl, Role = role.ToString(), Term = term });
        });
        app.MapPost("/internal/v1/snapshot", (InstallSnapshotRequest r, RaftNode n) =>
        {
            var (status, resp) = n.HandleInstallSnapshot(r);
            return status == 200 ? Results.Json(resp) : Results.StatusCode(status);
        });

        // ---- API pubblica
        app.MapPost("/api/v1/data", (HttpContext ctx, ClientApi api) => api.HandleAsync(ctx));
    }
}
