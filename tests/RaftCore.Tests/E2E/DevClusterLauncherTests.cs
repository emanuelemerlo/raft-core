using System.Diagnostics;
using System.Text;
using System.Text.Json;
using RaftCore.Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RaftCore.Tests.E2E;

/// <summary>
/// Come F5 in Visual Studio: UN processo in ambiente Development avvia il cluster di 3 nodi;
/// uccidendo il processo (stop del debugger) anche i nodi figli si chiudono.
/// </summary>
[Trait("Category", "E2E")]
public sealed class DevClusterLauncherTests : IDisposable
{
    private readonly ITestOutputHelper _out;
    private readonly TempDir _dir = new();
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(5) };
    private Process? _parent;
    private readonly StringBuilder _log = new();

    public DevClusterLauncherTests(ITestOutputHelper o) => _out = o;

    private async Task<JsonElement?> Status(int port)
    {
        try { return JsonDocument.Parse(await _http.GetStringAsync($"http://127.0.0.1:{port}/api/v1/cluster/status")).RootElement; }
        catch { return null; }
    }

    [Fact]
    public async Task One_launch_starts_three_nodes_and_children_die_with_the_parent()
    {
        var ports = Enumerable.Range(0, 5).Select(_ => Ports.Free()).ToList();
        var psi = new ProcessStartInfo("dotnet") { WorkingDirectory = _dir.Path, UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
        psi.ArgumentList.Add(Path.Combine(AppContext.BaseDirectory, "RaftCore.dll"));
        psi.ArgumentList.Add("--RaftCluster:RestPorts=" + string.Join(",", ports));
        psi.ArgumentList.Add("--RaftCluster:ClusterId=launcher-" + Guid.NewGuid().ToString("N")[..6]);
        psi.ArgumentList.Add("--RaftCluster:Storage:MinFreeBytes=1024");
        psi.ArgumentList.Add("--RaftCluster:DevCluster:ChildOutput=Prefix");                // nessuna finestra nei test
        psi.Environment["ASPNETCORE_ENVIRONMENT"] = "Development";
        psi.Environment["CLUSTER_SECRET"] = "dev-only-secret-not-for-production-0123456789";
        _parent = Process.Start(psi)!;
        _parent.OutputDataReceived += (_, e) => { lock (_log) _log.AppendLine(e.Data); };
        _parent.ErrorDataReceived += (_, e) => { lock (_log) _log.AppendLine(e.Data); };
        _parent.BeginOutputReadLine(); _parent.BeginErrorReadLine();

        // tre nodi attivi, un Leader, tutti si conoscono
        var end = DateTime.UtcNow.AddSeconds(40);
        List<int> live = new();
        while (DateTime.UtcNow < end)
        {
            live.Clear();
            bool leader = false;
            foreach (var p in ports)
            {
                var s = await Status(p);
                if (s == null) continue;
                live.Add(p);
                leader |= s.Value.GetProperty("role").GetString() == "Leader";
            }
            if (live.Count == 3 && leader) break;
            await Task.Delay(300);
        }
        lock (_log) _out.WriteLine(_log.ToString());
        Assert.Equal(3, live.Count);
        using (var req = new HttpRequestMessage(HttpMethod.Post, $"http://127.0.0.1:{live[2]}/api/v1/data") { Content = new StringContent("{\"f5\":1}", Encoding.UTF8, "application/json") })
        {
            req.Headers.Add("X-Request-ID", Guid.NewGuid().ToString());
            using var r = await _http.SendAsync(req);
            Assert.Equal(200, (int)r.StatusCode);
        }

        // stop del debugger: il processo padre viene ucciso, i figli devono chiudersi da soli
        _parent.Kill();
        end = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < end && (await Task.WhenAll(live.Select(Status))).Any(s => s != null)) await Task.Delay(200);
        Assert.All(await Task.WhenAll(live.Select(Status)), s => Assert.Null(s));
    }

    public void Dispose()
    {
        try { if (_parent != null && !_parent.HasExited) _parent.Kill(true); } catch { }
        _http.Dispose();
        Thread.Sleep(1500);                                                            // i figli rilasciano i file
        _dir.Dispose();
    }
}
