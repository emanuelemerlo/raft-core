using System.Diagnostics;
using System.Text;
using System.Text.Json;
using RaftCore.Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RaftCore.Tests.E2E;

/// <summary>
/// Cluster di 3 PROCESSI reali (RaftCore.dll) con crash brutale del Leader (Process.Kill = SIGKILL / TerminateProcess):
/// misura il failover come in REQ-FOV-1 e verifica che nessuna scrittura vada persa o duplicata.
/// </summary>
[Trait("Category", "E2E")]
public sealed class ProcessClusterTests : IDisposable
{
    private readonly ITestOutputHelper _out;
    private readonly TempDir _dir = new();
    private readonly List<(string Id, int Port, string Data)> _nodes = new();
    private readonly Process?[] _procs = new Process?[3];
    private readonly List<StreamWriter> _logs = new();
    private int _starts;
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(20) };
    private readonly string _cluster = "e2e-" + Guid.NewGuid().ToString("N")[..8];

    public ProcessClusterTests(ITestOutputHelper o)
    {
        _out = o;
        for (int i = 0; i < 3; i++) _nodes.Add((Guid.NewGuid().ToString(), Ports.Free(), Path.Combine(_dir.Path, "n" + i)));
    }

    private void Start(int i, bool allowEmpty)
    {
        var dll = Path.Combine(AppContext.BaseDirectory, "RaftCore.dll");
        var args = new List<string>
        {
            dll,
            "--RaftCluster:ClusterId=" + _cluster, "--RaftCluster:ClusterSize=3", "--RaftCluster:NodeId=" + _nodes[i].Id,
            "--RaftCluster:RestPort=" + _nodes[i].Port, "--RaftCluster:AdvertiseUrl=http://127.0.0.1:{port}", "--RaftCluster:UdpMode=Disabled",
            "--RaftCluster:Storage:DataDir=" + _nodes[i].Data, "--RaftCluster:Storage:MinFreeBytes=1024",
            "--RaftCluster:Storage:AllowEmptyState=" + (allowEmpty ? "true" : "false"),
            "--RaftCluster:StateMachine:SharedFile:Directory=" + Path.Combine(_dir.Path, "shared"),
        };
        for (int k = 0; k < 3; k++)
        {
            args.Add($"--RaftCluster:Members:{k}=" + _nodes[k].Id);
            if (k != i) args.Add($"--RaftCluster:Peers:{_nodes[k].Id}=http://127.0.0.1:{_nodes[k].Port}");
        }
        var psi = new ProcessStartInfo("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        foreach (var a in args) psi.ArgumentList.Add(a);
        psi.Environment["CLUSTER_SECRET"] = TestCluster.Secret;
        var p = Process.Start(psi)!;
        var log = new StreamWriter(Path.Combine(_dir.Path, $"proc{i}-start{++_starts}.log")) { AutoFlush = true };
        _logs.Add(log);
        p.OutputDataReceived += (_, e) => { if (e.Data != null) lock (log) { try { log.WriteLine(e.Data); } catch (ObjectDisposedException) { } } };
        p.ErrorDataReceived += (_, e) => { if (e.Data != null) lock (log) { try { log.WriteLine(e.Data); } catch (ObjectDisposedException) { } } };
        p.BeginOutputReadLine(); p.BeginErrorReadLine();
        _procs[i] = p;
    }

    private async Task<JsonElement?> Status(int i)
    {
        try { return JsonDocument.Parse(await _http.GetStringAsync($"http://127.0.0.1:{_nodes[i].Port}/api/v1/cluster/status")).RootElement; }
        catch { return null; }
    }

    private async Task<int> WaitLeader(int? exclude = null, int seconds = 30)
    {
        var end = DateTime.UtcNow.AddSeconds(seconds);
        while (DateTime.UtcNow < end)
        {
            for (int i = 0; i < 3; i++)
            {
                if (i == exclude || _procs[i] == null) continue;
                var s = await Status(i);
                if (s?.GetProperty("role").GetString() == "Leader") return i;
            }
            await Task.Delay(100);
        }
        throw new TimeoutException("nessun Leader");
    }

    private async Task<int> Post(int node, string id, string body)
    {
        try
        {
            using var req = new HttpRequestMessage(HttpMethod.Post, $"http://127.0.0.1:{_nodes[node].Port}/api/v1/data") { Content = new StringContent(body, Encoding.UTF8, "application/json") };
            req.Headers.Add("X-Request-ID", id);
            using var r = await _http.SendAsync(req);
            return (int)r.StatusCode;
        }
        catch { return 0; }
    }

    [Fact]
    public async Task Kill9_of_the_leader_fails_over_within_limits_without_loss_or_duplicates()
    {
        for (int i = 0; i < 3; i++) Start(i, true);
        int leader = await WaitLeader();
        int entry = (leader + 1) % 3;
        var sent = new List<(string Id, string Body, DateTime At, int Status, DateTime Done)>();
        var killAt = DateTime.MinValue;
        var tasks = new List<Task>();
        var lockObj = new object();
        var start = DateTime.UtcNow;
        for (int k = 0; DateTime.UtcNow - start < TimeSpan.FromSeconds(7); k++)
        {
            var id = Guid.NewGuid().ToString(); var body = $"{{\"k\":{k}}}"; var at = DateTime.UtcNow;
            tasks.Add(Task.Run(async () => { int s = await Post(entry, id, body); lock (lockObj) sent.Add((id, body, at, s, DateTime.UtcNow)); }));
            if (killAt == DateTime.MinValue && DateTime.UtcNow - start > TimeSpan.FromSeconds(2))
            {
                killAt = DateTime.UtcNow;
                _procs[leader]!.Kill(entireProcessTree: true);                       // crash: nessuno shutdown ordinato
            }
            await Task.Delay(50);
        }
        await Task.WhenAll(tasks);
        var firstOk = sent.Where(s => s.At > killAt && s.Status == 200).Min(s => s.Done);
        double failover = (firstOk - killAt).TotalSeconds;
        _out.WriteLine($"failover: {failover:F2} s; esiti: " + string.Join(", ", sent.GroupBy(s => s.Status).Select(g => $"{g.Key}x{g.Count()}")));
        Assert.True(failover <= 5.0, $"failover {failover:F2} s oltre il limite di 5 s (REQ-FOV-2)");

        // retry del client con stesso ID e body: tutto deve risultare scritto una sola volta
        foreach (var s in sent.Where(s => s.Status != 200))
        {
            int st = 0;
            for (int a = 0; a < 10 && st != 200; a++) { st = await Post(entry, s.Id, s.Body); if (st != 200) await Task.Delay(300); }
            Assert.Equal(200, st);
        }
        var rows = File.ReadAllLines(Path.Combine(_dir.Path, "shared", "applied.log")).Select(l => l.Split('\t')).ToList();
        var applied = rows.Where(r => r[1] == "1").Select(r => r[2]).ToList();
        Assert.Equal(applied.Count, applied.Distinct().Count());
        foreach (var s in sent) Assert.Contains(s.Id, applied);

        // il nodo ucciso riparte con il proprio stato e si riallinea
        Start(leader, false);
        var end = DateTime.UtcNow.AddSeconds(20);
        while (DateTime.UtcNow < end)
        {
            var s = await Status(leader); var e = await Status(entry);
            if (s != null && e != null && s.Value.GetProperty("commitIndex").GetInt64() == e.Value.GetProperty("commitIndex").GetInt64()) return;
            await Task.Delay(200);
        }
        throw new TimeoutException("il nodo riavviato non si è riallineato");
    }

    public void Dispose()
    {
        foreach (var p in _procs) { try { if (p != null && !p.HasExited) p.Kill(true); p?.WaitForExit(5000); } catch { } }
        foreach (var l in _logs) { lock (l) { try { l.Dispose(); } catch { } } }
        var keep = Environment.GetEnvironmentVariable("RAFTCORE_TEST_LOGS");
        if (keep != null)
        {
            Directory.CreateDirectory(keep);
            foreach (var f in Directory.GetFiles(_dir.Path, "proc*.log")) File.Copy(f, Path.Combine(keep, "Kill9_of_the_leader_fails_over_within_limits_without_loss_or_duplicates-" + Path.GetFileName(f)), true);
        }
        _http.Dispose();
        _dir.Dispose();
    }
}
