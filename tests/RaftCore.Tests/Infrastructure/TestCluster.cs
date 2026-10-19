using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RaftCore.Abstractions;
using RaftCore.Configuration;
using RaftCore.Hosting;
using RaftCore.Models;
using RaftCore.Services.Core;
using RaftCore.Services.Network;
using RaftCore.Services.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace RaftCore.Tests.Infrastructure;

public sealed class TempDir : IDisposable
{
    public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "raftcore-tests", Guid.NewGuid().ToString("N")[..12]);
    public TempDir() => Directory.CreateDirectory(Path);
    public void Dispose() { try { Directory.Delete(Path, true); } catch { } }
}

public static class Ports
{
    private static int _next = 20000 + Random.Shared.Next(0, 20000);
    public static int Free()
    {
        while (true)
        {
            int p = Interlocked.Increment(ref _next);
            if (p > 60000) { Interlocked.Exchange(ref _next, 20000); continue; }
            try { var l = new TcpListener(IPAddress.Loopback, p); l.Start(); l.Stop(); return p; }
            catch (SocketException) { }
        }
    }
}

/// <summary>Partizioni di rete tra nodi: blocca le RPC Raft (non il traffico client).</summary>
public sealed class NetworkFaults
{
    private readonly ConcurrentDictionary<(string From, string To), bool> _blocked = new();
    public void Block(string a, string b) { _blocked[(a, b)] = true; _blocked[(b, a)] = true; }
    public void Isolate(string node, IEnumerable<string> others) { foreach (var o in others) Block(node, o); }
    public void HealAll() => _blocked.Clear();
    public bool IsBlocked(string from, string to) => _blocked.ContainsKey((from, to));
}

public sealed class FaultyTransport : IPeerTransport
{
    private readonly IPeerTransport _inner;
    private readonly NetworkFaults _faults;
    private readonly string _self;
    public FaultyTransport(IPeerTransport inner, NetworkFaults faults, string self) { _inner = inner; _faults = faults; _self = self; }
    public async Task<RpcResult<TResp>> SendAsync<TReq, TResp>(PeerEndpoint peer, string path, TReq request, TimeSpan timeout, CancellationToken ct) where TReq : BaseRpcRequest
    {
        if (_faults.IsBlocked(_self, peer.NodeId)) { await Task.Delay(Math.Min(50, (int)timeout.TotalMilliseconds), ct); return new RpcResult<TResp>(0, default); }
        return await _inner.SendAsync<TReq, TResp>(peer, path, request, timeout, ct);
    }
}

/// <summary>Orologio con UtcNow spostabile (il tempo monotono resta reale): simula un cluster riavviato dopo settimane.</summary>
public sealed class ShiftableClock : IClock
{
    private readonly SystemClock _sys = new();
    public TimeSpan Offset { get; set; }
    public long MonotonicMs => _sys.MonotonicMs;
    public DateTime UtcNow => _sys.UtcNow + Offset;
    public Task Delay(int milliseconds, CancellationToken ct) => _sys.Delay(milliseconds, ct);
}

public sealed class NodeSpec
{
    public required string Id { get; init; }
    public required int Port { get; init; }
    public string DataDir { get; init; } = "";
    public RunningNode? Running { get; set; }
    public ShiftableClock Clock { get; } = new();
    public string Url => $"http://127.0.0.1:{Port}";
}

/// <summary>Cluster di nodi completi (HTTP reale su localhost) nello stesso processo, con timer accelerati.</summary>
public sealed class TestCluster : IAsyncDisposable
{
    public const string Secret = "test-secret-0123456789abcdef";
    public TempDir Dir { get; } = new();
    public List<NodeSpec> Nodes { get; } = new();
    public NetworkFaults Faults { get; } = new();
    public string SharedDir => Path.Combine(Dir.Path, "shared");
    public string ClusterId { get; } = "test-" + Guid.NewGuid().ToString("N")[..8];
    public Dictionary<string, string?> Extra { get; } = new();
    /// <summary>Hook per sostituire il repository (es. fault injection); riceve l'indice del nodo.</summary>
    public Func<int, ClusterConfig, IDataRepository>? RepositoryFactory { get; set; }
    /// <summary>Per nodo: NodeId dei peer da NON configurare come Peers statici (simula discovery incompleta).</summary>
    public Func<int, string, bool> PeerVisible { get; set; } = (_, _) => true;
    /// <summary>Override di URL statico: (nodo, peerId) -> URL (simula URL stantii o errati).</summary>
    public Func<int, string, string?>? PeerUrlOverride { get; set; }
    public readonly HttpClient Http = new() { Timeout = TimeSpan.FromSeconds(30) };

    static TestCluster()
    {
        // Più nodi nello stesso processo condividono un solo thread pool: senza un minimo adeguato la crescita lenta
        // del pool introduce pause di centinaia di ms (lease scaduti) che con processi separati non esistono.
        ThreadPool.GetMinThreads(out var w, out var io);
        ThreadPool.SetMinThreads(Math.Max(w, 64), Math.Max(io, 64));
    }

    /// <summary>Nome del test (nome dei file di log copiati in RAFTCORE_TEST_LOGS).</summary>
    public string TestName { get; private set; } = "";

    public static TestCluster Create(int n, [System.Runtime.CompilerServices.CallerMemberName] string test = "")
    {
        var c = new TestCluster { TestName = test };
        for (int i = 0; i < n; i++)
        {
            var id = Guid.NewGuid().ToString("D");
            c.Nodes.Add(new NodeSpec { Id = id, Port = Ports.Free(), DataDir = Path.Combine(c.Dir.Path, "node" + i) });
        }
        return c;
    }

    public Dictionary<string, string?> Settings(int i, bool allowEmpty)
    {
        var n = Nodes[i];
        var s = new Dictionary<string, string?>
        {
            ["RaftCluster:ClusterId"] = ClusterId,
            ["RaftCluster:ClusterSize"] = Nodes.Count.ToString(),
            ["RaftCluster:ClusterSecret"] = Secret,
            ["RaftCluster:MembershipMode"] = "Static",
            ["RaftCluster:NodeId"] = n.Id,
            ["RaftCluster:RestPort"] = n.Port.ToString(),
            ["RaftCluster:RestPortRange"] = "",
            ["RaftCluster:AdvertiseUrl"] = "http://127.0.0.1:{port}",
            ["RaftCluster:UdpMode"] = "Disabled",
            ["RaftCluster:DiscoveryPhaseMs"] = "300",
            ["RaftCluster:HeartbeatIntervalMs"] = "100",
            ["RaftCluster:ElectionTimeoutMinMs"] = "600",
            ["RaftCluster:ElectionTimeoutMaxMs"] = "1000",
            ["RaftCluster:LeaderLeaseMs"] = "400",
            ["RaftCluster:ReplicationTimeoutMs"] = "800",
            ["RaftCluster:FailoverWaitMs"] = "6000",
            ["RaftCluster:RequestDeadlineMs"] = "8000",
            ["RaftCluster:ShutdownGraceMs"] = "200",
            ["RaftCluster:TestMode"] = "true",
            ["RaftCluster:Storage:DataDir"] = n.DataDir,
            ["RaftCluster:Storage:MinFreeBytes"] = "1024",
            ["RaftCluster:Storage:AllowEmptyState"] = allowEmpty ? "true" : "false",
            ["RaftCluster:StateMachine:Provider"] = "SharedFile",
            ["RaftCluster:StateMachine:SharedFile:Directory"] = SharedDir,
            ["RaftCluster:StateMachine:MaxApplyAttempts"] = "3",
            ["RaftCluster:StateMachine:ApplyRetryMaxDelayMs"] = "200",
        };
        for (int k = 0; k < Nodes.Count; k++)
        {
            s[$"RaftCluster:Members:{k}"] = Nodes[k].Id;
            if (k == i || !PeerVisible(i, Nodes[k].Id)) continue;
            s[$"RaftCluster:Peers:{Nodes[k].Id}"] = PeerUrlOverride?.Invoke(i, Nodes[k].Id) ?? Nodes[k].Url;
        }
        foreach (var kv in Extra) s[kv.Key] = kv.Value;
        return s;
    }

    public async Task StartAsync(int i, bool allowEmpty = true)
    {
        var n = Nodes[i];
        n.Running = await NodeHost.StartAsync(Array.Empty<string>(), (services, cfg) =>
        {
            services.AddSingleton<IClock>(n.Clock);
            services.AddLogging(b => { b.ClearProviders(); b.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Debug); b.AddProvider(new FileLoggerProvider(Path.Combine(Dir.Path, $"node{i}.log"))); });
            services.AddSingleton<IPeerTransport>(sp => new FaultyTransport(
                ActivatorUtilities.CreateInstance<HttpPeerTransport>(sp), Faults, ClusterConfig.Norm(n.Id)));
            if (RepositoryFactory != null) services.AddSingleton(RepositoryFactory(i, cfg));
        }, Settings(i, allowEmpty), contentRoot: Dir.Path);
    }

    public async Task StartAllAsync()
    {
        for (int i = 0; i < Nodes.Count; i++) await StartAsync(i);
    }

    public async Task StopAsync(int i)
    {
        var r = Nodes[i].Running;
        Nodes[i].Running = null;
        if (r != null) await r.DisposeAsync();
    }

    public IEnumerable<int> Running => Enumerable.Range(0, Nodes.Count).Where(i => Nodes[i].Running != null);

    public RaftNode Raft(int i) => Nodes[i].Running!.Raft;

    public int? LeaderIndex()
    {
        var leaders = Running.Where(i => Raft(i).IsLeader).ToList();
        return leaders.Count == 1 ? leaders[0] : null;
    }

    public async Task<int> WaitForLeaderAsync(TimeSpan? timeout = null, int? exclude = null)
    {
        var end = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(15));
        while (DateTime.UtcNow < end)
        {
            var l = LeaderIndex();
            if (l != null && l != exclude && Raft(l.Value).GetReadiness().Ready) return l.Value;
            await Task.Delay(50);
        }
        throw new TimeoutException("Nessun Leader eletto. Stato: " + string.Join(" | ", Running.Select(i => $"{i}:{Raft(i).Probe()}")));
    }

    public async Task WaitUntilAsync(Func<bool> cond, TimeSpan timeout, string what)
    {
        var end = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < end) { if (cond()) return; await Task.Delay(50); }
        throw new TimeoutException("Condizione non raggiunta: " + what);
    }

    public async Task<(int Status, string Body, HttpResponseMessage Resp)> PostAsync(int node, string requestId, string json, Dictionary<string, string>? headers = null)
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, Nodes[node].Url + "/api/v1/data") { Content = new StringContent(json, Encoding.UTF8, "application/json") };
        if (requestId.Length > 0) req.Headers.Add("X-Request-ID", requestId);
        if (headers != null) foreach (var (k, v) in headers) req.Headers.TryAddWithoutValidation(k, v);
        var resp = await Http.SendAsync(req);
        return ((int)resp.StatusCode, await resp.Content.ReadAsStringAsync(), resp);
    }

    /// <summary>Righe del database condiviso su file: (indice, stato, requestId, payload base64).</summary>
    public List<(long Index, ApplyStatus Status, string RequestId, string Payload)> AppliedRows()
    {
        var f = Path.Combine(SharedDir, "applied.log");
        if (!File.Exists(f)) return new();
        using var fs = new FileStream(f, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        using var r = new StreamReader(fs);
        return r.ReadToEnd().Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(l => l.Split('\t')).Select(x => (long.Parse(x[0]), (ApplyStatus)int.Parse(x[1]), x[2], x[6])).ToList();
    }

    /// <summary>Invarianti (§21): indici applicati contigui, nessun RequestId applicato due volte, log coerenti tra i nodi.</summary>
    public void AssertInvariants()
    {
        var rows = AppliedRows();
        for (int k = 0; k < rows.Count; k++)
            if (rows[k].Index != k + 1) throw new Xunit.Sdk.XunitException($"applied.log: indice {rows[k].Index} in posizione {k + 1}");
        var dupApplied = rows.Where(r => r.Status == ApplyStatus.Applied).GroupBy(r => r.RequestId).Where(g => g.Count() > 1).Select(g => g.Key).ToList();
        if (dupApplied.Count > 0) throw new Xunit.Sdk.XunitException("RequestId applicati più volte: " + string.Join(",", dupApplied));

        var running = Running.ToList();
        long minCommit = running.Min(i => Raft(i).Probe().Commit);
        long from = running.Max(i => Raft(i).Wal.FirstIndex);
        for (long idx = from; idx <= minCommit; idx++)
        {
            var terms = running.Select(i => Raft(i).Wal.TermAt(idx)).Distinct().ToList();
            if (terms.Count != 1) throw new Xunit.Sdk.XunitException($"Log divergenti all'indice committed {idx}: term {string.Join(",", terms)}");
        }
    }

    /// <summary>Se impostato, i log dei nodi vengono copiati qui alla fine del test (diagnostica).</summary>
    public string? KeepLogsIn { get; set; }

    public async ValueTask DisposeAsync()
    {
        foreach (var i in Running.ToList()) { try { await StopAsync(i); } catch { } }
        Http.Dispose();
        var keep = KeepLogsIn ?? Environment.GetEnvironmentVariable("RAFTCORE_TEST_LOGS");
        if (keep != null)
        {
            Directory.CreateDirectory(keep);
            foreach (var f in Directory.GetFiles(Dir.Path, "node*.log")) File.Copy(f, Path.Combine(keep, (TestName.Length > 0 ? TestName : ClusterId) + "-" + Path.GetFileName(f)), true);
        }
        Dir.Dispose();
    }
}
