using System.Text.Json;
using RaftCore.Abstractions;
using RaftCore.Configuration;
using RaftCore.Hosting;
using RaftCore.Models;
using RaftCore.Services.Storage;
using RaftCore.Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace RaftCore.Tests.Cluster;

[Trait("Category", "Cluster")]
public class ClusterTests
{
    private readonly ITestOutputHelper _out;
    public ClusterTests(ITestOutputHelper o) => _out = o;

    private static string Id() => Guid.NewGuid().ToString();
    private static string Others(TestCluster c, int leader) => string.Join(",", Enumerable.Range(0, c.Nodes.Count).Where(i => i != leader));
    private static int AFollower(TestCluster c, int leader) => c.Running.First(i => i != leader);

    [Fact]
    public async Task T1_T3_T4_boot_elects_one_leader_and_follower_forwards_transparently()
    {
        await using var c = TestCluster.Create(3);
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync();
        Assert.Single(c.Running, i => c.Raft(i).IsLeader);

        var rid = Id();
        var viaFollower = await c.PostAsync(AFollower(c, l), rid, "{\"a\":1}");
        Assert.Equal(200, viaFollower.Status);
        var replayViaLeader = await c.PostAsync(l, rid, "{\"a\":1}");
        Assert.Equal(200, replayViaLeader.Status);
        Assert.Equal(viaFollower.Body, replayViaLeader.Body);
        Assert.Equal("true", replayViaLeader.Resp.Headers.GetValues("X-Idempotent-Replay").Single());
        Assert.Single(c.AppliedRows(), r => r.RequestId == rid && r.Status == ApplyStatus.Applied);
        c.AssertInvariants();
    }

    [Fact]
    public async Task T9_T34_T42_validation_and_conflicts()
    {
        await using var c = TestCluster.Create(3);
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync();
        int f = AFollower(c, l);
        Assert.Equal(400, (await c.PostAsync(f, "", "{}")).Status);
        Assert.Equal(400, (await c.PostAsync(f, "non-uuid", "{}")).Status);
        Assert.Equal(400, (await c.PostAsync(f, Id(), "{not json")).Status);
        Assert.Equal(413, (await c.PostAsync(f, Id(), "{\"p\":\"" + new string('x', 1_100_000) + "\"}")).Status);
        var rid = Id();
        Assert.Equal(200, (await c.PostAsync(f, rid, "{\"v\":1}")).Status);
        Assert.Equal(422, (await c.PostAsync(f, rid, "{\"v\": 1}")).Status);         // stesso ID, spaziatura diversa (T42)
        Assert.Equal(200, (await c.PostAsync(f, rid, "{\"v\":1}")).Status);          // body identico: replay
        Assert.Single(c.AppliedRows(), r => r.RequestId == rid);
    }

    [Fact]
    public async Task Client_supplied_forwarding_headers_are_ignored()
    {
        // v0.1: un client che inviava X-Forward-Hops riceveva 421
        await using var c = TestCluster.Create(3);
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync();
        var r = await c.PostAsync(AFollower(c, l), Id(), "{\"h\":1}", new() { ["X-Forward-Hops"] = "5", ["X-Raft-Deadline-Ms"] = "1" });
        Assert.Equal(200, r.Status);
    }

    [Fact]
    public async Task T33_concurrent_identical_requests_produce_one_write()
    {
        await using var c = TestCluster.Create(3);
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync();
        var rid = Id();
        var results = await Task.WhenAll(Enumerable.Range(0, 10).Select(k => c.PostAsync(k % 3, rid, "{\"same\":true}")));
        Assert.All(results, r => Assert.Equal(200, r.Status));
        Assert.Single(c.AppliedRows(), r => r.RequestId == rid && r.Status == ApplyStatus.Applied);
    }

    [Fact]
    public async Task T5_T6_T20_leader_stop_under_load_loses_nothing_and_duplicates_nothing()
    {
        await using var c = TestCluster.Create(3);
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync();
        int entry = AFollower(c, l);
        var ids = Enumerable.Range(0, 120).Select(_ => Id()).ToList();
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var tasks = new List<Task<(int, string, HttpResponseMessage)>>();
        for (int k = 0; k < ids.Count; k++)
        {
            tasks.Add(c.PostAsync(entry, ids[k], $"{{\"k\":{k}}}"));
            if (k == 40) _ = c.StopAsync(l);                                          // il Leader cade durante il carico
            await Task.Delay(20);
        }
        var res = await Task.WhenAll(tasks);
        _out.WriteLine($"durata {sw.ElapsedMilliseconds} ms, esiti: " + string.Join(", ", res.GroupBy(r => r.Item1).Select(g => $"{g.Key}x{g.Count()}")));
        // retry corretto del client (REQ-CLI-1): stesso ID e stesso body fino a 200
        for (int k = 0; k < ids.Count; k++)
            for (int attempt = 0; res[k].Item1 != 200 && attempt < 5; attempt++)
                res[k] = await c.PostAsync(entry, ids[k], $"{{\"k\":{k}}}");
        Assert.All(res, r => Assert.Equal(200, r.Item1));
        int newLeader = await c.WaitForLeaderAsync(exclude: l);
        Assert.NotEqual(l, newLeader);
        var rows = c.AppliedRows();
        foreach (var id in ids) Assert.Single(rows, r => r.RequestId == id && r.Status == ApplyStatus.Applied);
        c.AssertInvariants();
    }

    [Fact]
    public async Task Failover_is_fast_even_when_rpcs_to_the_dead_leader_hang_until_timeout()
    {
        // Su Windows una connessione verso una porta chiusa non viene rifiutata subito: il candidato non deve attendere
        // il nodo morto in ogni round (PreVote + voto), altrimenti i round si allungano e i voti contesi si ripetono.
        await using var c = TestCluster.Create(3);
        c.Extra["RaftCluster:ReplicationTimeoutMs"] = "1500";                            // RPC verso il morto: fino a 300 ms per round
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync();
        var times = new List<double>();
        for (int round = 0; round < 3; round++)
        {
            c.Faults.Blackhole(c.Nodes[l].Id.ToLowerInvariant());
            var sw = System.Diagnostics.Stopwatch.StartNew();
            await c.StopAsync(l);
            int nl = await c.WaitForLeaderAsync(TimeSpan.FromSeconds(10), exclude: l);
            times.Add(sw.Elapsed.TotalSeconds);
            Assert.Equal(200, (await c.PostAsync(nl, Id(), "{\"r\":" + round + "}")).Status);
            c.Faults.HealAll();
            await c.StartAsync(l, allowEmpty: false);
            await c.WaitUntilAsync(() => c.Raft(l).Probe().Commit == c.Raft(nl).Probe().Commit, TimeSpan.FromSeconds(10), "rientro");
            l = nl;
        }
        _out.WriteLine("failover (s): " + string.Join(", ", times.Select(t => t.ToString("F2"))));
        Assert.All(times, t => Assert.True(t < 3.0, $"failover {t:F2} s"));
    }

    [Fact]
    public async Task Followers_that_only_knew_the_leader_elect_a_new_one_after_it_dies()
    {
        // Il problema osservato: con discovery incompleta i Follower conoscevano solo il Leader e restavano in PreVote
        await using var c = TestCluster.Create(3);
        c.PeerVisible = (node, peer) => node == 0 || peer == c.Nodes[0].Id;          // i nodi 1 e 2 conoscono solo il nodo 0
        await c.StartAsync(0);
        await c.StartAsync(1); await c.StartAsync(2);
        int l = await c.WaitForLeaderAsync();
        if (l != 0)
        {
            // si forza la situazione critica: Leader = nodo 0
            await c.StopAsync(l); await Task.Delay(1500); await c.StartAsync(l, allowEmpty: false);
            await c.WaitUntilAsync(() => c.LeaderIndex() is int x && c.Raft(x).GetReadiness().Ready, TimeSpan.FromSeconds(15), "leader");
            l = c.LeaderIndex()!.Value;
        }
        Assert.Equal(200, (await c.PostAsync(1, Id(), "{\"x\":1}")).Status);
        await Task.Delay(500);                                                          // gli heartbeat propagano gli URL
        await c.StopAsync(l);
        int nl = await c.WaitForLeaderAsync(TimeSpan.FromSeconds(10), exclude: l);
        Assert.NotEqual(l, nl);
        Assert.Equal(200, (await c.PostAsync(c.Running.First(), Id(), "{\"x\":2}")).Status);
    }

    [Fact]
    public async Task T12_quorum_loss_returns_503_and_recovers()
    {
        await using var c = TestCluster.Create(3);
        c.Extra["RaftCluster:RequestDeadlineMs"] = "3000";
        c.Extra["RaftCluster:FailoverWaitMs"] = "2000";
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync();
        var followers = c.Running.Where(i => i != l).ToList();
        foreach (var f in followers) await c.StopAsync(f);
        await Task.Delay(600);
        var r = await c.PostAsync(l, Id(), "{\"q\":1}");
        Assert.True(r.Status is 503 or 504, $"status {r.Status}");
        Assert.False(c.Raft(l).IsLeader);
        await c.StartAsync(followers[0], allowEmpty: false);
        await c.WaitForLeaderAsync();
        Assert.Equal(200, (await c.PostAsync(followers[0], Id(), "{\"q\":2}")).Status);
        c.AssertInvariants();
    }

    [Fact]
    public async Task T10_T11_partitioned_leader_steps_down_and_converges_after_heal()
    {
        await using var c = TestCluster.Create(3);
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync();
        long termBefore = c.Raft(l).Probe().Term;
        c.Faults.Isolate(c.Nodes[l].Id.ToLowerInvariant(), c.Nodes.Where((_, i) => i != l).Select(n => n.Id.ToLowerInvariant()));
        await c.WaitUntilAsync(() => !c.Raft(l).IsLeader, TimeSpan.FromSeconds(3), "step down del Leader isolato");
        int nl = await c.WaitForLeaderAsync(exclude: l);
        Assert.True(c.Raft(nl).Probe().Term > termBefore);
        var rid = Id();
        Assert.Equal(200, (await c.PostAsync(nl, rid, "{\"p\":1}")).Status);
        c.Faults.HealAll();
        await c.WaitUntilAsync(() => c.Raft(l).Probe().Commit == c.Raft(nl).Probe().Commit, TimeSpan.FromSeconds(10), "convergenza");
        c.AssertInvariants();
    }

    [Fact]
    public async Task T26_T37_isolated_follower_rejoining_does_not_disrupt_leader()
    {
        await using var c = TestCluster.Create(3);
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync();
        int f = AFollower(c, l);
        long term = c.Raft(l).Probe().Term;
        c.Faults.Isolate(c.Nodes[f].Id.ToLowerInvariant(), c.Nodes.Where((_, i) => i != f).Select(n => n.Id.ToLowerInvariant()));
        await Task.Delay(3000);                                                         // tenta elezioni (negate: PreVote)
        Assert.Equal(term, c.Raft(f).Probe().Term);                                     // il PreVote non incrementa il term
        c.Faults.HealAll();
        await Task.Delay(1500);
        foreach (var i in c.Running)
            _out.WriteLine($"nodo {i}: {c.Raft(i).Probe()} stepDown={c.Raft(i).GetMetrics()["stepDownCount"]} elections={c.Raft(i).GetMetrics()["electionCount"]} prevoteDenied={c.Raft(i).GetMetrics()["prevoteDenied"]}");
        Assert.True(c.Raft(l).IsLeader);
        Assert.Equal(term, c.Raft(l).Probe().Term);
    }

    [Fact]
    public async Task T16_full_restart_keeps_data_and_idempotency()
    {
        await using var c = TestCluster.Create(3);
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync();
        var rid = Id();
        var first = await c.PostAsync(l, rid, "{\"keep\":1}");
        Assert.Equal(200, first.Status);
        foreach (var i in c.Running.ToList()) await c.StopAsync(i);
        for (int i = 0; i < 3; i++) await c.StartAsync(i, allowEmpty: false);
        int l2 = await c.WaitForLeaderAsync();
        var again = await c.PostAsync(l2, rid, "{\"keep\":1}");
        Assert.Equal(200, again.Status);
        Assert.Equal(first.Body, again.Body);
        Assert.Single(c.AppliedRows(), r => r.RequestId == rid && r.Status == ApplyStatus.Applied);
        c.AssertInvariants();
    }

    [Fact]
    public async Task T39_empty_state_without_flag_blocks_startup_and_datadir_is_locked()
    {
        await using var c = TestCluster.Create(3);
        await c.StartAllAsync();
        await c.WaitForLeaderAsync();
        await c.StopAsync(2);
        Directory.Delete(c.Nodes[2].DataDir, true);
        var ex = await Assert.ThrowsAsync<NodeStartException>(() => c.StartAsync(2, allowEmpty: false));
        Assert.Equal(ExitCodes.StartupBlocked, ex.Code);
        // un secondo processo sulla stessa DataDir di un nodo attivo non parte
        var dup = await Assert.ThrowsAsync<NodeStartException>(() => NodeHost.StartAsync(Array.Empty<string>(), null,
            new Dictionary<string, string?>(c.Settings(0, true)) { ["RaftCluster:RestPort"] = Ports.Free().ToString() }, contentRoot: c.Dir.Path));
        Assert.Equal(ExitCodes.DataDirInUse, dup.Code);
    }

    [Fact]
    public async Task Port_range_skips_ports_in_use()
    {
        await using var c = TestCluster.Create(1);
        int busy = Ports.Free();
        var blocker = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Any, busy);
        blocker.Start();
        try
        {
            c.Extra["RaftCluster:RestPortRange"] = $"{busy}-{busy + 5}";
            await c.StartAsync(0);
            Assert.NotEqual(busy, c.Nodes[0].Running!.Config.RestPort);
            Assert.InRange(c.Nodes[0].Running!.Config.RestPort, busy + 1, busy + 5);
        }
        finally { blocker.Stop(); }
    }

    [Fact]
    public async Task Poison_entry_is_quarantined_and_cluster_continues()
    {
        await using var c = TestCluster.Create(3);
        c.RepositoryFactory = (i, cfg) => new SharedFileDataRepository(cfg.StateMachine.SharedFile, true)
        {
            FaultInjector = e => System.Text.Encoding.UTF8.GetString(e.Payload).Contains("poison") ? new InvalidOperationException("bug applicativo") : null
        };
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync();
        var bad = await c.PostAsync(l, Id(), "{\"poison\":true}");
        Assert.Equal(422, bad.Status);
        Assert.Contains("quarantined", bad.Body);
        var good = await c.PostAsync(l, Id(), "{\"ok\":true}");
        Assert.Equal(200, good.Status);
        Assert.Equal(1, c.Raft(l).GetMetrics()["applyQuarantined"]);
    }

    [Fact]
    public async Task Transient_database_errors_are_retried_without_quarantine()
    {
        await using var c = TestCluster.Create(3);
        int failures = 0;
        c.RepositoryFactory = (i, cfg) => new SharedFileDataRepository(cfg.StateMachine.SharedFile, true)
        {
            FaultInjector = e => e.EntryType == LogEntryType.Data && Interlocked.Increment(ref failures) <= 8 ? new IOException("database non raggiungibile") : null
        };
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync();
        var r = await c.PostAsync(l, Id(), "{\"t\":1}");
        Assert.Equal(200, r.Status);
        // la leadership può passare a un altro nodo durante il test: le metriche si sommano su tutti i nodi
        long Sum(string m) => c.Running.Sum(i => c.Raft(i).GetMetrics()[m]);
        Assert.Equal(0, Sum("applyQuarantined"));
        Assert.True(Sum("applyTransientErrors") >= 8, $"applyTransientErrors = {Sum("applyTransientErrors")}");
    }

    [Fact]
    public async Task Stale_pending_entry_committed_weeks_later_is_expired_not_written()
    {
        // Punto 1: voce accettata da un Leader rimasto senza quorum, committata solo dopo il riavvio del cluster
        await using var c = TestCluster.Create(3);
        c.Extra["RaftCluster:RequestDeadlineMs"] = "2000";
        c.Extra["RaftCluster:FailoverWaitMs"] = "1000";
        c.Extra["RaftCluster:StateMachine:MaxPendingAgeMinutes"] = "60";
        await c.StartAllAsync();
        var rid = Id();
        var all = c.Nodes.Select(n => n.Id.ToLowerInvariant()).ToList();
        int l = -1;
        for (int attempt = 1; ; attempt++)
        {
            // Partizione totale istantanea: il Leader, con il lease ancora valido, appende la voce ma non può committarla;
            // i Follower non possono eleggere nessuno. (Fermare i Follower richiede tempo: il lease può scadere prima della POST.)
            l = await c.WaitForLeaderAsync();
            long before = c.Raft(l).Probe().Last;
            foreach (var a in all) c.Faults.Isolate(a, all.Where(b => b != a));
            var lost = await c.PostAsync(l, rid, "{\"stale\":true}");
            Assert.NotEqual(200, lost.Status);
            if (c.Raft(l).Probe().Last > before) break;                                  // voce nel log del Leader, mai committed
            Assert.True(attempt < 3, "la voce non è mai stata appesa dal Leader");
            c.Faults.HealAll();                                                          // lease scaduto prima della POST: si riprova
        }
        await c.WaitUntilAsync(() => !c.Raft(l).IsLeader, TimeSpan.FromSeconds(3), "step down");
        var followers = c.Running.Where(i => i != l).ToList();
        foreach (var f in followers) await c.StopAsync(f);
        c.Nodes[l].Clock.Offset = TimeSpan.FromDays(14);                               // "due settimane dopo"
        c.Faults.HealAll();
        // Si riavvia un solo Follower: con due nodi l'unico eleggibile è l'ex Leader, che ha il log più aggiornato
        // (contiene la voce). Così la voce viene committata "due settimane dopo" in modo deterministico.
        await c.StartAsync(followers[0], allowEmpty: false);
        await c.WaitUntilAsync(() => c.Raft(l).IsLeader, TimeSpan.FromSeconds(20), "rielezione dell'ex Leader");
        await c.StartAsync(followers[1], allowEmpty: false);
        await c.WaitUntilAsync(() => c.LeaderIndex() is int x && c.Raft(x).GetReadiness().Ready
                                     && c.Raft(x).Probe().Commit > 0 && c.Raft(x).Probe().Applied >= c.Raft(x).Probe().Commit,
            TimeSpan.FromSeconds(20), "Leader con tutte le voci committed applicate");
        var row = c.AppliedRows().Single(r => r.RequestId == rid);
        Assert.Equal(ApplyStatus.Expired, row.Status);                                  // committata dopo 14 giorni: scaduta, non scritta
        Assert.Equal("", row.Payload);
        var retry = await c.PostAsync(c.LeaderIndex()!.Value, rid, "{\"stale\":true}");
        Assert.Equal(410, retry.Status);
        Assert.DoesNotContain(c.AppliedRows(), r => r.RequestId == rid && r.Status == ApplyStatus.Applied);
    }

    [Fact]
    public async Task Lagging_follower_catches_up_through_InstallSnapshot_after_compaction()
    {
        await using var c = TestCluster.Create(3);
        c.Extra["RaftCluster:Log:RetainEntries"] = "100";
        c.Extra["RaftCluster:Log:CompactionBatch"] = "50";
        c.Extra["RaftCluster:Log:CompactionIntervalMs"] = "200";
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync();
        int f = AFollower(c, l);
        await c.StopAsync(f);
        for (int k = 0; k < 400; k += 20)
            await Task.WhenAll(Enumerable.Range(k, 20).Select(j => c.PostAsync(l, Id(), $"{{\"n\":{j}}}")));
        await c.WaitUntilAsync(() => c.Raft(l).Probe().Snapshot > 100, TimeSpan.FromSeconds(10), "compattazione sul Leader");
        Assert.True(c.Raft(l).Wal.WindowCount < 400, "la RAM del log deve essere limitata dalla compattazione");
        await c.StartAsync(f, allowEmpty: false);
        await c.WaitUntilAsync(() => c.Raft(f).Probe().Last == c.Raft(l).Probe().Last && c.Raft(f).Probe().Commit == c.Raft(l).Probe().Commit,
            TimeSpan.FromSeconds(15), "riallineamento del Follower");
        Assert.True(c.Raft(f).GetMetrics()["snapshotsInstalled"] >= 1);
        Assert.Equal(400, c.AppliedRows().Count(r => r.Status == ApplyStatus.Applied));
        c.AssertInvariants();
    }

    [Fact]
    public async Task Url_pointing_to_another_member_is_never_counted_twice()
    {
        // Punto 8: il nodo 0 crede che il nodo 1 sia all'URL del nodo 2
        await using var c = TestCluster.Create(3);
        c.PeerUrlOverride = (node, peer) => node == 0 && peer == c.Nodes[1].Id ? c.Nodes[2].Url : null;
        await c.StartAllAsync();
        await c.WaitForLeaderAsync();
        // l'URL errato viene rilevato (risposta firmata da un altro NodeId) oppure sostituito dall'URL appreso via RPC
        var reg = c.Nodes[0].Running!.App.Services.GetRequiredService<RaftCore.Services.Network.PeerRegistry>();
        await c.WaitUntilAsync(() => reg.GetUrl(c.Nodes[1].Id) == c.Nodes[1].Url, TimeSpan.FromSeconds(10), "correzione dell'URL errato");
        _out.WriteLine($"rpcWrongDestination: nodo0={c.Raft(0).GetMetrics()["rpcWrongDestination"]} nodo2={c.Raft(2).GetMetrics()["rpcWrongDestination"]}");
        Assert.Equal(200, (await c.PostAsync(0, Id(), "{\"w\":1}")).Status);
        c.AssertInvariants();
    }
}
