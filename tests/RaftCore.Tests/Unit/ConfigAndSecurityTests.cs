using System.Text;
using Microsoft.AspNetCore.Http;
using RaftCore.Configuration;
using RaftCore.Services.Common;
using RaftCore.Services.Core;
using RaftCore.Services.Http;
using RaftCore.Services.Network;
using RaftCore.Services.Storage;
using RaftCore.Abstractions;
using RaftCore.Tests.Infrastructure;
using Xunit;

namespace RaftCore.Tests.Unit;

public class ConfigTests
{
    internal static ClusterConfig Valid()
    {
        var ids = Enumerable.Range(0, 3).Select(_ => Guid.NewGuid().ToString()).ToList();
        return new ClusterConfig
        {
            ClusterId = "c", ClusterSize = 3, ClusterSecret = "0123456789abcdef0", NodeId = ids[0], Members = ids,
            Endpoints = new()
            {
                new EndpointConfig { Name = "misure", Path = "/api/v1/misure", Table = "dbo.Misure" },
                new EndpointConfig { Name = "eventi", Path = "/api/v1/eventi", Table = "dbo.Eventi" }
            }
        };
    }

    [Fact]
    public void Defaults_are_valid() => Valid().Validate();

    [Theory]
    [InlineData(250, 1000, 1800, 700, "LeaderLeaseMs")]   // lease < 3 x heartbeat (T44)
    [InlineData(250, 1000, 1200, 750, "ElectionTimeoutMaxMs - ElectionTimeoutMinMs")] // intervallo troppo stretto (T44)
    [InlineData(250, 900, 1800, 750, "ElectionTimeoutMinMs")]
    [InlineData(250, 1000, 1800, 1000, "LeaderLeaseMs")]
    public void Timer_constraints_are_enforced_T44(int hb, int min, int max, int lease, string expected)
    {
        var c = Valid(); c.HeartbeatIntervalMs = hb; c.ElectionTimeoutMinMs = min; c.ElectionTimeoutMaxMs = max; c.LeaderLeaseMs = lease;
        var ex = Assert.Throws<ConfigurationValidationException>(c.Validate);
        Assert.Contains(ex.Violations, v => v.Contains(expected));
    }

    [Fact]
    public void Sql_provider_requires_connection_string_and_safe_names()
    {
        var c = Valid(); c.StateMachine.Provider = StateMachineProvider.SqlServer; c.Endpoints[0].Table = "dbo.x;drop";
        var ex = Assert.Throws<ConfigurationValidationException>(c.Validate);
        Assert.Contains(ex.Violations, v => v.Contains("ConnectionString"));
        Assert.Contains(ex.Violations, v => v.Contains("Table 'dbo.x;drop'"));
    }

    [Fact]
    public void Endpoints_are_required_T65()
    {
        var c = Valid(); c.Endpoints.Clear();
        var ex = Assert.Throws<ConfigurationValidationException>(c.Validate);
        Assert.Contains(ex.Violations, v => v.Contains("almeno un endpoint"));
    }

    [Theory]
    [InlineData("Misure", "/api/v1/x", "Name 'Misure' non valido")]                 // maiuscole
    [InlineData("-x", "/api/v1/x", "Name '-x' non valido")]
    [InlineData("x", "api/v1/x", "Path 'api/v1/x' non valido")]                       // non assoluto
    [InlineData("x", "/api/v1/x/", "Path '/api/v1/x/' non valido")]                   // '/' finale
    [InlineData("x", "/api/{id}", "Path '/api/{id}' non valido")]                     // parametri
    [InlineData("x", "/api/../internal", "Path '/api/../internal' non valido")]
    [InlineData("x", "/internal/v1/append", "riservato")]
    [InlineData("x", "/Health/live", "riservato")]
    [InlineData("x", "/api/v1/cluster/status", "riservato")]
    [InlineData("x", "/api/v1/metrics", "riservato")]
    public void Invalid_endpoints_are_rejected_T65(string name, string path, string expected)
    {
        var c = Valid(); c.Endpoints.Add(new EndpointConfig { Name = name, Path = path, Table = "dbo.X" });
        var ex = Assert.Throws<ConfigurationValidationException>(c.Validate);
        Assert.Contains(ex.Violations, v => v.Contains(expected));
    }

    [Fact]
    public void Endpoint_names_paths_and_tables_must_be_unique_T65()
    {
        var c = Valid();
        c.Endpoints.Add(new EndpointConfig { Name = "misure", Path = "/api/v1/altro", Table = "dbo.Altro" });
        c.Endpoints.Add(new EndpointConfig { Name = "altro", Path = "/API/v1/Eventi", Table = "dbo.Altro2" });
        c.Endpoints.Add(new EndpointConfig { Name = "terzo", Path = "/api/v1/terzo", Table = "DBO.misure" });
        var ex = Assert.Throws<ConfigurationValidationException>(c.Validate);
        Assert.Contains(ex.Violations, v => v.Contains("Name 'misure' duplicato"));
        Assert.Contains(ex.Violations, v => v.Contains("Path '/api/v1/eventi' duplicato"));
        Assert.Contains(ex.Violations, v => v.Contains("usata da più endpoint"));
    }

    [Fact]
    public void Endpoint_table_cannot_be_a_service_table()
    {
        var c = Valid(); c.StateMachine.Provider = StateMachineProvider.SqlServer; c.StateMachine.SqlServer.ConnectionString = "Server=x";
        c.Endpoints[0].Table = "raft.Requests";
        var ex = Assert.Throws<ConfigurationValidationException>(c.Validate);
        Assert.Contains(ex.Violations, v => v.Contains("tabella di servizio"));
    }

    [Fact]
    public void Endpoint_values_are_trimmed_and_blank_overrides_ignored()
    {
        var c = Valid();
        c.Endpoints.Add(new EndpointConfig { Name = " terzo ", Path = " /api/v1/terzo ", Table = " " });
        c.Endpoints.Add(new EndpointConfig());                                   // elemento svuotato da un override
        c.Validate();
        Assert.Equal(3, c.Endpoints.Count);
        Assert.Equal("terzo", c.Endpoints[2].Name);
        Assert.Equal("/api/v1/terzo", c.Endpoints[2].Path);
        Assert.Null(c.Endpoints[2].Table);
    }

    [Fact]
    public void Endpoints_hash_ignores_order_and_case_but_not_content()
    {
        var a = Valid().Endpoints;
        var b = a.AsEnumerable().Reverse().Select(e => new EndpointConfig { Name = e.Name, Path = e.Path.ToUpperInvariant(), Table = e.Table }).ToList();
        Assert.Equal(ClusterConfig.ComputeEndpointsHash(a), ClusterConfig.ComputeEndpointsHash(b));
        b[0].Table = "dbo.Altra";
        Assert.NotEqual(ClusterConfig.ComputeEndpointsHash(a), ClusterConfig.ComputeEndpointsHash(b));
    }

    [Fact]
    public void Port_range_and_placeholders()
    {
        var c = Valid(); c.RestPortRange = "5001-5003";
        Assert.Equal(new[] { 5001, 5002, 5003 }, c.CandidatePorts());
        c.Storage.DataDir = "data/{port}/{nodeId}"; c.AdvertiseUrl = "http://host:{port}";
        c.ResolvePlaceholdersAndPaths(5002, "/base");
        Assert.Equal(Path.GetFullPath(Path.Combine("/base", "data", "5002", ClusterConfig.Norm(c.NodeId))), c.Storage.DataDir);
        Assert.Equal("http://host:5002", c.AdvertiseUrl);
        c.RestPortRange = "abc";
        Assert.Throws<ConfigurationValidationException>(() => c.CandidatePorts());
    }

    [Theory]
    [InlineData("5001-5003", new[] { 5001, 5002, 5003 })]
    [InlineData("5001,5003,5007", new[] { 5001, 5003, 5007 })]
    [InlineData("5001-5003;5010;5012", new[] { 5001, 5002, 5003, 5010, 5012 })]
    [InlineData(" 5010 ; 5001-5002 , 5010 ", new[] { 5010, 5001, 5002 })]   // ordine conservato, duplicati ignorati
    [InlineData("5005", new[] { 5005 })]
    public void Port_spec_supports_range_list_and_mixed(string spec, int[] expected)
    {
        Assert.Equal(expected, ClusterConfig.ParsePortSpec(spec));
        var c = Valid(); c.RestPorts = spec;
        Assert.Equal(expected, c.CandidatePorts());
    }

    [Theory]
    [InlineData("abc")]
    [InlineData("5003-5001")]
    [InlineData("0")]
    [InlineData("70000")]
    [InlineData("1-5000")]
    [InlineData(";,")]
    public void Invalid_port_specs_are_rejected(string spec)
    {
        var ex = Assert.Throws<ConfigurationValidationException>(() => ClusterConfig.ParsePortSpec(spec));
        Assert.Contains("5001-5005;5010;5012", ex.Violations[0]);
    }

    [Fact]
    public void MaxBodyBytes_has_a_hard_limit()
    {
        var c = Valid(); c.MaxBodyBytes = ClusterConfig.MaxBodyBytesLimit + 1;
        Assert.Throws<ConfigurationValidationException>(c.Validate);
    }
}

public class SecurityTests
{
    [Fact]
    public void Response_signature_binds_responder_nonce_status_and_body()
    {
        var body = Encoding.UTF8.GetBytes("{\"ok\":true}");
        var sig = RpcAuth.SignResponse("s3cret-s3cret-s3", "11111111-1111-1111-1111-111111111111", "nonce1", 200, body);
        Assert.Equal(sig, RpcAuth.SignResponse("s3cret-s3cret-s3", "11111111-1111-1111-1111-111111111111", "nonce1", 200, body));
        Assert.NotEqual(sig, RpcAuth.SignResponse("s3cret-s3cret-s3", "22222222-2222-2222-2222-222222222222", "nonce1", 200, body)); // altro nodo
        Assert.NotEqual(sig, RpcAuth.SignResponse("s3cret-s3cret-s3", "11111111-1111-1111-1111-111111111111", "nonce2", 200, body)); // replay
        Assert.NotEqual(sig, RpcAuth.SignResponse("s3cret-s3cret-s3", "11111111-1111-1111-1111-111111111111", "nonce1", 503, body));
        // l'impronta degli endpoint è firmata: non si può alterare senza invalidare la risposta
        Assert.NotEqual(sig, RpcAuth.SignResponse("s3cret-s3cret-s3", "11111111-1111-1111-1111-111111111111", "nonce1", 200, body, "abc"));
    }

    [Fact]
    public void Verifier_rejects_wrong_destination_replay_and_tampering()
    {
        var cfg = ConfigTests.Valid();
        var other = cfg.Members[1];
        var verifier = new RpcVerifier(cfg, new Membership(cfg, cfg.NodeId), new Metrics());
        HttpRequest Req(string dest, string nonce, byte[] body, string? sigOverride = null, string? protocol = null)
        {
            var ctx = new DefaultHttpContext();
            ctx.Request.Headers[RpcAuth.HProtocol] = protocol ?? RaftCore.Models.ProtocolInfo.Version.ToString();
            ctx.Request.Method = "POST"; ctx.Request.Path = "/internal/v1/vote";
            long ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            ctx.Request.Headers[RpcAuth.HNode] = other; ctx.Request.Headers[RpcAuth.HTime] = ts.ToString();
            ctx.Request.Headers[RpcAuth.HNonce] = nonce; ctx.Request.Headers[RpcAuth.HDest] = dest;
            ctx.Request.Headers[RpcAuth.HSig] = sigOverride ?? RpcAuth.Sign(cfg.ClusterSecret, "POST", "/internal/v1/vote", ts, body, other, nonce, dest);
            return ctx.Request;
        }
        var b = Encoding.UTF8.GetBytes("{}");
        Assert.Equal(RpcVerifyResult.Ok, verifier.Verify(Req(cfg.NodeId, "n1", b), b));
        Assert.Equal(RpcVerifyResult.Replay, verifier.Verify(Req(cfg.NodeId, "n1", b), b));
        Assert.Equal(RpcVerifyResult.WrongDestination, verifier.Verify(Req(cfg.Members[2], "n2", b), b));
        Assert.Equal(RpcVerifyResult.BadSignature, verifier.Verify(Req(cfg.NodeId, "n3", b), Encoding.UTF8.GetBytes("{\"x\":1}")));
    }

    [Fact]
    public void Verifier_rejects_other_protocol_versions_T62()
    {
        var cfg = ConfigTests.Valid();
        var other = cfg.Members[1];
        var metrics = new Metrics();
        var verifier = new RpcVerifier(cfg, new Membership(cfg, cfg.NodeId), metrics);
        HttpRequest Req(string? protocol)
        {
            var ctx = new DefaultHttpContext();
            ctx.Request.Method = "POST"; ctx.Request.Path = "/internal/v1/vote";
            long ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (protocol != null) ctx.Request.Headers[RpcAuth.HProtocol] = protocol;
            ctx.Request.Headers[RpcAuth.HNode] = other; ctx.Request.Headers[RpcAuth.HTime] = ts.ToString();
            ctx.Request.Headers[RpcAuth.HNonce] = Guid.NewGuid().ToString("N"); ctx.Request.Headers[RpcAuth.HDest] = cfg.NodeId;
            ctx.Request.Headers[RpcAuth.HSig] = RpcAuth.Sign(cfg.ClusterSecret, "POST", "/internal/v1/vote", ts, "{}"u8, other, ctx.Request.Headers[RpcAuth.HNonce]!, cfg.NodeId);
            return ctx.Request;
        }
        Assert.Equal(RpcVerifyResult.ProtocolMismatch, verifier.Verify(Req(null), "{}"u8));                                            // nodo v0.2
        Assert.Equal(RpcVerifyResult.ProtocolMismatch, verifier.Verify(Req((RaftCore.Models.ProtocolInfo.Version + 1).ToString()), "{}"u8));
        Assert.Equal(2, metrics.Get("rpcProtocolMismatch"));
        Assert.Equal(409, RpcVerifier.HttpStatusFor(RpcVerifyResult.ProtocolMismatch));
        Assert.Equal(RpcVerifyResult.Ok, verifier.Verify(Req(RaftCore.Models.ProtocolInfo.Version.ToString()), "{}"u8));
    }

    [Fact]
    public void Request_signature_depends_on_path_so_forwards_cannot_change_endpoint()
    {
        var b = "{}"u8.ToArray();
        Assert.NotEqual(RpcAuth.Sign("k", "POST", "/api/v1/misure", 1, b, "n", "x", "d"), RpcAuth.Sign("k", "POST", "/api/v1/eventi", 1, b, "n", "x", "d"));
    }

    [Fact]
    public void Payload_hash_covers_the_endpoint_T59()
    {
        var b = "{\"a\":1}"u8.ToArray();
        Assert.Equal(RaftCore.Models.PayloadHashing.Compute("misure", b), RaftCore.Models.PayloadHashing.Compute("misure", b));
        Assert.NotEqual(RaftCore.Models.PayloadHashing.Compute("misure", b), RaftCore.Models.PayloadHashing.Compute("eventi", b));
        // separatore: ("ab", "c...") e ("a", "bc...") non collidono
        Assert.NotEqual(RaftCore.Models.PayloadHashing.Compute("ab", "c"u8), RaftCore.Models.PayloadHashing.Compute("a", "bc"u8));
    }

    [Fact]
    public void Endpoint_catalog_tracks_mismatched_members_T63()
    {
        var cfg = ConfigTests.Valid();
        var metrics = new Metrics();
        var cat = new EndpointCatalog(cfg, metrics);
        Assert.True(cat.Contains("misure"));
        Assert.False(cat.Contains("ordini"));
        cat.NoteRemote(cfg.Members[1], cat.Hash);
        cat.NoteRemote(cfg.Members[2], "diversa");
        Assert.Equal(new[] { cfg.Members[2] }, cat.MismatchedNodes());
        Assert.Equal(1, metrics.Get("endpointsHashMismatch"));
        cat.NoteRemote(cfg.Members[2], "");                                   // impronta assente: ignorata
        Assert.Single(cat.MismatchedNodes());
        cat.NoteRemote(cfg.Members[2], cat.Hash);
        Assert.Empty(cat.MismatchedNodes());
        Assert.Equal(0, metrics.Get("endpointsHashMismatch"));
    }
}

public class RegistryAndMembershipTests
{
    [Fact]
    public void Url_priority_gossip_and_invalidation()
    {
        var cfg = ConfigTests.Valid();
        var peer = ClusterConfig.Norm(cfg.Members[1]);
        cfg.Peers[peer] = "http://static:1";
        var reg = new PeerRegistry(new SystemClock(), cfg);
        reg.NoteGossip(new Dictionary<string, string> { [peer] = "http://gossip:1" }, cfg.NodeId);
        Assert.Equal("http://gossip:1", reg.GetUrl(peer));
        reg.NoteRpcUrl(peer, "http://rpc:1/");
        Assert.Equal("http://rpc:1", reg.GetUrl(peer));
        reg.InvalidateUrl(peer, "http://rpc:1");
        reg.InvalidateUrl(peer, "http://gossip:1");
        Assert.Equal("http://static:1", reg.GetUrl(peer));
    }

    [Fact]
    public async Task Bootstrap_admission_does_not_deadlock_with_node_lock()
    {
        // v0.1: TryAdmit teneva il lock dei membri mentre Persist prendeva il lock del nodo (e viceversa)
        var cfg = ConfigTests.Valid(); cfg.MembershipMode = MembershipMode.Bootstrap; cfg.Members.Clear();
        var m = new Membership(cfg, cfg.NodeId);
        var nodeLock = new object();
        m.Persist = list => { lock (nodeLock) { var c = m.IsComplete; } };
        var t1 = Task.Run(() => { for (int i = 0; i < 2000; i++) m.TryAdmit(Guid.NewGuid().ToString()); });
        var t2 = Task.Run(() => { for (int i = 0; i < 20000; i++) lock (nodeLock) { _ = m.IsComplete; _ = m.Members; } });
        var all = Task.WhenAll(t1, t2);
        Assert.Same(all, await Task.WhenAny(all, Task.Delay(10000)));
        Assert.Equal(3, m.Members.Count);
    }

    [Fact]
    public void DataDir_lock_prevents_second_process()
    {
        using var d = new TempDir();
        using var l = DataDirLock.Acquire(d.Path);
        Assert.Throws<IOException>(() => DataDirLock.Acquire(d.Path));
    }

    [Fact]
    public void Sql_schema_script_is_embedded_and_parametrized()
    {
        var s = SqlServerDataRepository.SchemaScript("ingest");
        Assert.Contains("[ingest].[ApplyState]", s);
        Assert.DoesNotContain("[raft]", s);
        Assert.Contains("[ingest].[SchemaVersion]", s);
        Assert.DoesNotContain("IngestedData", s);
        Assert.True(SqlServerDataRepository.SplitBatches(s).Count() >= 8);
        Assert.True(SqlServerDataRepository.IsPermanent(new PayloadRejectedException("x")));
        Assert.False(SqlServerDataRepository.IsPermanent(new TimeoutException()));
        var t = JsonPayloadWriter.TableScript("dati.Misure");
        Assert.Contains("CREATE TABLE [dati].[Misure]", t);
        Assert.Contains("[UQ_Misure_Request]", t);
        Assert.DoesNotContain("$(", t);
        Assert.Throws<ArgumentException>(() => new JsonPayloadWriter("dbo.x;drop", 30));
        var repo = new SqlServerDataRepository(new SqlServerConfig { ConnectionString = "Server=x" }, "c", "n", new Dictionary<string, ISqlPayloadWriter>());
        Assert.True(repo.IsTransient(new TimeoutException()));
        Assert.False(repo.IsTransient(new NullReferenceException()));
    }
}
