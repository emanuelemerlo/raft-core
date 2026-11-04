using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Data.SqlClient;
using RaftCore.Configuration;
using RaftCore.Hosting;
using RaftCore.Models;
using RaftCore.Services.Storage;
using RaftCore.Tests.Infrastructure;
using RaftCore.Tests.Unit;
using Xunit;

namespace RaftCore.Tests.Cluster;

/// <summary>Endpoint di ingestione multipli (REQ-EPT-*) e versioni (REQ-VER-*), su un cluster reale in-process.</summary>
[Trait("Category", "Cluster")]
public class EndpointTests
{
    private static string Id() => Guid.NewGuid().ToString();

    [Fact]
    public async Task T58_each_endpoint_writes_with_its_own_type_also_through_a_follower()
    {
        await using var c = TestCluster.Create(3);
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync();
        int f = c.Running.First(i => i != l);
        var m1 = Id(); var e1 = Id(); var m2 = Id(); var e2 = Id();
        Assert.Equal(200, (await c.PostAsync(f, m1, "{\"temperatura\":21.5}")).Status);
        Assert.Equal(200, (await c.PostAsync(f, e1, "{\"evento\":\"avvio\"}", path: TestCluster.EventiPath)).Status);
        Assert.Equal(200, (await c.PostAsync(l, m2, "{\"temperatura\":22}")).Status);
        Assert.Equal(200, (await c.PostAsync(l, e2, "{\"evento\":\"stop\"}", path: "/API/V1/EVENTI")).Status);   // path senza distinzione di maiuscole
        var rows = c.AppliedRows().Where(r => r.Status == ApplyStatus.Applied).ToDictionary(r => r.RequestId, r => r.PayloadType);
        Assert.Equal("misure", rows[m1]); Assert.Equal("eventi", rows[e1]);
        Assert.Equal("misure", rows[m2]); Assert.Equal("eventi", rows[e2]);
        foreach (var i in c.Running)
            Assert.Equal("eventi", c.Raft(i).Wal.ReadEntry(c.AppliedRows().Single(r => r.RequestId == e1).Index)!.PayloadType);   // replicato
        c.AssertInvariants();
    }

    [Fact]
    public async Task T59_same_request_id_on_another_endpoint_is_rejected_with_422()
    {
        await using var c = TestCluster.Create(3);
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync();
        int f = c.Running.First(i => i != l);
        var rid = Id();
        Assert.Equal(200, (await c.PostAsync(f, rid, "{\"v\":1}")).Status);
        var other = await c.PostAsync(f, rid, "{\"v\":1}", path: TestCluster.EventiPath);       // stesso body, altro endpoint
        Assert.Equal(422, other.Status);
        Assert.Contains("payload diverso", other.Body);
        var replay = await c.PostAsync(l, rid, "{\"v\":1}");                                   // sull'endpoint originale resta un replay
        Assert.Equal(200, replay.Status);
        Assert.Single(c.AppliedRows(), r => r.RequestId == rid && r.Status == ApplyStatus.Applied);
        Assert.DoesNotContain(c.AppliedRows(), r => r.RequestId == rid && r.PayloadType == "eventi" && r.Status == ApplyStatus.Applied);
    }

    [Fact]
    public async Task T60_unconfigured_paths_return_404_without_log_entries()
    {
        await using var c = TestCluster.Create(3);
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync();
        int f = c.Running.First(i => i != l);
        Assert.Equal(200, (await c.PostAsync(l, Id(), "{}")).Status);
        long last = c.Raft(l).Wal.LastIndex;
        Assert.Equal(404, (await c.PostAsync(f, Id(), "{}", path: "/api/v1/data")).Status);    // endpoint della v0.2: non esiste più
        Assert.Equal(404, (await c.PostAsync(l, Id(), "{}", path: "/api/v1/ordini")).Status);
        Assert.Equal(404, (await c.PostAsync(f, Id(), "{}", path: TestCluster.MisurePath + "/x")).Status);
        Assert.Equal(last, c.Raft(l).Wal.LastIndex);
    }

    [Fact]
    public async Task T63_different_endpoint_configuration_is_reported_but_cluster_keeps_working()
    {
        await using var c = TestCluster.Create(3);
        c.NodeExtra = i => i != 2 ? new Dictionary<string, string?>() : new Dictionary<string, string?>
        {
            ["RaftCluster:Endpoints:2:Name"] = "ordini", ["RaftCluster:Endpoints:2:Path"] = "/api/v1/ordini", ["RaftCluster:Endpoints:2:Table"] = "dbo.Ordini"
        };
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync();
        Assert.Equal(200, (await c.PostAsync(c.Running.First(i => i != l), Id(), "{\"a\":1}")).Status);
        // Le impronte si osservano solo tra nodi che si scambiano RPC: il Leader le vede tutte, un Follower vede il Leader.
        // Quindi la discrepanza è sempre visibile almeno sul Leader e sul nodo discordante.
        string NodeOf(int i) => ClusterConfig.Norm(c.Nodes[i].Id);
        if (l != 2)
        {
            await c.WaitUntilAsync(() => Mismatch(c, l).SequenceEqual(new[] { NodeOf(2) }), TimeSpan.FromSeconds(10), "il Leader segnala il nodo 2");
            await c.WaitUntilAsync(() => Mismatch(c, 2).Contains(NodeOf(l)), TimeSpan.FromSeconds(10), "il nodo 2 segnala il Leader");
            Assert.Equal(1, c.Raft(l).GetMetrics()["endpointsHashMismatch"]);
            int other = 3 - l - 2;                                                     // l'altro Follower, allineato al Leader
            Assert.DoesNotContain(NodeOf(l), Mismatch(c, other));
        }
        else
        {
            await c.WaitUntilAsync(() => Mismatch(c, 2).Count == 2, TimeSpan.FromSeconds(10), "il Leader (nodo 2) segnala gli altri due");
            await c.WaitUntilAsync(() => Mismatch(c, 0).Contains(NodeOf(2)) && Mismatch(c, 1).Contains(NodeOf(2)), TimeSpan.FromSeconds(10),
                "i Follower segnalano il Leader");
            Assert.Equal(2, c.Raft(2).GetMetrics()["endpointsHashMismatch"]);
        }
    }

    private static List<string> Mismatch(TestCluster c, int i)
    {
        var json = JsonSerializer.SerializeToElement(c.Raft(i).GetStatus());
        return json.GetProperty("endpointsMismatch").EnumerateArray().Select(x => x.GetString()!).ToList();
    }

    [Fact]
    public async Task Status_lists_endpoints_and_protocol_version()
    {
        await using var c = TestCluster.Create(1);
        await c.StartAllAsync();
        await c.WaitForLeaderAsync();
        var json = JsonSerializer.SerializeToElement(c.Raft(0).GetStatus());
        Assert.Equal(ProtocolInfo.Version, json.GetProperty("protocolVersion").GetInt32());
        var eps = json.GetProperty("endpoints").EnumerateArray().Select(e => (e.GetProperty("name").GetString(), e.GetProperty("path").GetString())).ToList();
        Assert.Equal(new[] { ("misure", TestCluster.MisurePath), ("eventi", TestCluster.EventiPath) }, eps.Select(e => (e.Item1!, e.Item2!)));
    }

    [Fact]
    public async Task T64_wal_of_the_previous_format_blocks_startup()
    {
        await using var c = TestCluster.Create(1);
        await c.StartAllAsync();
        await c.WaitForLeaderAsync();
        Assert.Equal(200, (await c.PostAsync(0, Id(), "{}")).Status);
        await c.StopAsync(0);
        var seg = Directory.GetFiles(Path.Combine(c.Nodes[0].DataDir, "wal"), "*.wal").OrderBy(x => x).First();
        var bytes = await File.ReadAllBytesAsync(seg);
        bytes[4] = 1;                                                                          // formato della v0.2
        await File.WriteAllBytesAsync(seg, bytes);
        var ex = await Assert.ThrowsAsync<NodeStartException>(() => c.StartAsync(0, allowEmpty: false));
        Assert.Equal(ExitCodes.StartupBlocked, ex.Code);
        Assert.Contains("formato 1", ex.Message);
    }

    [Fact]
    public async Task T65_sql_endpoint_without_table_or_writer_blocks_startup()
    {
        await using var c = TestCluster.Create(1);
        c.Extra["RaftCluster:StateMachine:Provider"] = "SqlServer";
        c.Extra["RaftCluster:StateMachine:SqlServer:ConnectionString"] = "Server=127.0.0.1,1;Connect Timeout=1";
        c.Extra["RaftCluster:Endpoints:1:Table"] = "";
        var ex = await Assert.ThrowsAsync<NodeStartException>(() => c.StartAsync(0));
        Assert.Equal(ExitCodes.Configuration, ex.Code);
        Assert.Contains("Endpoints (eventi): Table obbligatoria", ex.Message);
    }
}

public class WriterRegistrationTests
{
    private sealed class W : ISqlPayloadWriter
    {
        public Task WriteAsync(SqlConnection c, SqlTransaction tx, LogEntry e, string clusterId, CancellationToken ct) => Task.CompletedTask;
    }

    private static ClusterConfig SqlCfg()
    {
        var c = ConfigTests.Valid();
        c.StateMachine.Provider = StateMachineProvider.SqlServer;
        c.Endpoints[1].Table = null;
        return c;
    }

    [Fact]
    public void Endpoint_without_table_needs_a_named_writer()
    {
        var ex = Assert.Throws<ConfigurationValidationException>(() => NodeHost.ValidateWriters(new ServiceCollection(), SqlCfg()));
        Assert.Contains(ex.Violations, v => v.Contains("Endpoints (eventi): Table obbligatoria"));
        NodeHost.ValidateWriters(new ServiceCollection().AddPayloadWriter<W>("eventi"), SqlCfg());
    }

    [Fact]
    public void Unnamed_writers_and_writers_for_unknown_endpoints_are_rejected()
    {
        var s = new ServiceCollection().AddPayloadWriter<W>("eventi").AddPayloadWriter("ordini", new W());
        s.AddSingleton<ISqlPayloadWriter, W>();                                                // registrazione della v0.2
        var ex = Assert.Throws<ConfigurationValidationException>(() => NodeHost.ValidateWriters(s, SqlCfg()));
        Assert.Contains(ex.Violations, v => v.Contains("senza nome di endpoint"));
        Assert.Contains(ex.Violations, v => v.Contains("'ordini'"));
    }
}
