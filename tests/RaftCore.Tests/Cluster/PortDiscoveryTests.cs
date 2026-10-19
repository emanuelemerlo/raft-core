using System.Net;
using System.Net.Sockets;
using RaftCore.Tests.Infrastructure;
using Xunit;

namespace RaftCore.Tests.Cluster;

/// <summary>
/// Porte dinamiche: ogni nodo sceglie la prima porta libera dell'elenco e trova gli altri SOLO tramite la discovery a
/// sondaggio (nessun Peers statico, nessun UDP).
/// </summary>
[Trait("Category", "Cluster")]
public class PortDiscoveryTests
{
    private static string PortList(int n) => string.Join(",", Enumerable.Range(0, n).Select(_ => Ports.Free()));

    private static TestCluster Create(string ports)
    {
        var c = TestCluster.Create(3);
        c.PeerVisible = (_, _) => false;                                             // nessun indirizzo noto in partenza
        c.Extra["RaftCluster:RestPorts"] = ports;
        c.Extra["RaftCluster:Discovery:ProbeEnabled"] = "true";
        c.Extra["RaftCluster:Discovery:ProbeIntervalMs"] = "300";
        c.Extra["RaftCluster:Discovery:ProbeTimeoutMs"] = "300";
        c.Extra["RaftCluster:Discovery:SeedHosts:0"] = "127.0.0.1";
        c.Extra["RaftCluster:Discovery:UnreachableAfterFailures"] = "2";
        return c;
    }

    [Fact]
    public async Task Nodes_on_a_shared_port_list_find_each_other_by_probing()
    {
        await using var c = Create(PortList(6));
        await c.StartAllAsync();
        var used = c.Nodes.Select(n => n.Running!.Config.RestPort).ToList();
        Assert.Equal(3, used.Distinct().Count());                                    // ognuno ha preso una porta diversa dell'elenco
        int l = await c.WaitForLeaderAsync(TimeSpan.FromSeconds(20));
        Assert.Equal(200, (await c.PostAsync(c.Running.First(i => i != l), Guid.NewGuid().ToString(), "{\"probe\":1}")).Status);
        c.AssertInvariants();
    }

    [Fact]
    public async Task Node_restarted_on_another_port_is_found_again()
    {
        await using var c = Create(PortList(6));
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync(TimeSpan.FromSeconds(20));
        int f = c.Running.First(i => i != l);
        int oldPort = c.Nodes[f].Running!.Config.RestPort;
        await c.StopAsync(f);

        // la porta del nodo viene occupata da un altro programma che accetta connessioni ma non risponde
        var squatter = new TcpListener(IPAddress.Any, oldPort);
        squatter.Start();
        try
        {
            await c.StartAsync(f, allowEmpty: false);
            int newPort = c.Nodes[f].Running!.Config.RestPort;
            Assert.NotEqual(oldPort, newPort);
            var rid = Guid.NewGuid().ToString();
            Assert.Equal(200, (await c.PostAsync(l, rid, "{\"moved\":1}")).Status);
            await c.WaitUntilAsync(() => c.Raft(f).Probe().Commit == c.Raft(l).Probe().Commit && c.Raft(f).Probe().Commit > 0,
                TimeSpan.FromSeconds(20), "riallineamento del nodo spostato su un'altra porta");
            // e il nodo spostato raggiunge a sua volta il Leader: le scritture inoltrate da lui riescono
            Assert.Equal(200, (await c.PostAsync(f, Guid.NewGuid().ToString(), "{\"from\":\"moved\"}")).Status);
            c.AssertInvariants();
        }
        finally { squatter.Stop(); }
    }
}
