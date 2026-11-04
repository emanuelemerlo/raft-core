using System.Text;
using Microsoft.Data.SqlClient;
using RaftCore.Abstractions;
using RaftCore.Configuration;
using RaftCore.Models;
using RaftCore.Services.Storage;
using RaftCore.Tests.Infrastructure;
using RaftCore.Tests.Unit;
using Xunit;

namespace RaftCore.Tests.Sql;

/// <summary>
/// Test d'integrazione su SQL Server reale. Eseguiti solo se RAFT_TEST_SQL contiene una stringa di connessione
/// (in CI: container mcr.microsoft.com/mssql/server). Ogni test usa uno schema dedicato.
/// </summary>
[Trait("Category", "SqlServer")]
public sealed class SqlServerRepositoryTests : IAsyncLifetime
{
    private static readonly string? Conn = Environment.GetEnvironmentVariable("RAFT_TEST_SQL");
    private readonly string _schema = "t" + Guid.NewGuid().ToString("N")[..10];
    private readonly ApplyContext _ctx = new(DateTime.UtcNow, TimeSpan.FromHours(1), "c1");

    private SqlServerConfig Cfg(bool autoCreate = true) => new() { ConnectionString = Conn!, Schema = _schema, AutoCreateSchema = autoCreate };
    private Dictionary<string, ISqlPayloadWriter> Writers() => new()
    {
        ["misure"] = new JsonPayloadWriter(_schema + ".Misure", 30),
        ["eventi"] = new JsonPayloadWriter(_schema + ".Eventi", 30)
    };
    private SqlServerDataRepository Repo(string node = "n1", bool autoCreate = true) => new(Cfg(autoCreate), "c1", node, Writers());

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (Conn == null) return;
        await using var c = new SqlConnection(Conn); await c.OpenAsync();
        foreach (var t in new[] { "Misure", "Eventi", "Quarantine", "Requests", "AppliedEntries", "ApplyState", "SchemaVersion" })
            await new SqlCommand($"IF OBJECT_ID(N'[{_schema}].[{t}]') IS NOT NULL DROP TABLE [{_schema}].[{t}]", c).ExecuteNonQueryAsync();
        await new SqlCommand($"IF SCHEMA_ID(N'{_schema}') IS NOT NULL EXEC(N'DROP SCHEMA [{_schema}]')", c).ExecuteNonQueryAsync();
    }

    private async Task<long> Count(string table)
    {
        await using var c = new SqlConnection(Conn); await c.OpenAsync();
        return Convert.ToInt64(await new SqlCommand($"SELECT COUNT(*) FROM [{_schema}].[{table}]", c).ExecuteScalarAsync());
    }

    private async Task Exec(string sql)
    {
        await using var c = new SqlConnection(Conn); await c.OpenAsync();
        await new SqlCommand(sql, c).ExecuteNonQueryAsync();
    }

    [SkippableFact]
    public async Task Full_lifecycle_applied_duplicate_conflict_expired_rejected_quarantined()
    {
        Skip.If(Conn == null, "RAFT_TEST_SQL non impostata");
        var r = Repo(); await r.InitializeAsync(default); await r.InitializeAsync(default);   // idempotente
        var rid = Guid.NewGuid().ToString();
        Assert.Equal(ApplyStatus.NoOp, (await r.ApplyAsync(SharedFileRepositoryTests.Entry(1, "", type: LogEntryType.NoOp), _ctx, default)).Status);
        var a = await r.ApplyAsync(SharedFileRepositoryTests.Entry(2, "{\"v\":1}", rid), _ctx, default);
        Assert.Equal(ApplyStatus.Applied, a.Status);
        var dup = await r.ApplyAsync(SharedFileRepositoryTests.Entry(3, "{\"v\":1}", rid), _ctx, default);
        Assert.Equal(ApplyStatus.Duplicate, dup.Status); Assert.Equal(a.Body, dup.Body);
        Assert.Equal(ApplyStatus.Conflict, (await r.ApplyAsync(SharedFileRepositoryTests.Entry(4, "{\"v\": 1}", rid), _ctx, default)).Status);
        Assert.Equal(ApplyStatus.Expired, (await r.ApplyAsync(SharedFileRepositoryTests.Entry(5, "{}", received: DateTime.UtcNow.AddDays(-14)), _ctx, default)).Status);
        Assert.Equal(ApplyStatus.Rejected, (await r.ApplyAsync(SharedFileRepositoryTests.Entry(6, "[1]"), _ctx, default)).Status);
        Assert.Equal(ApplyStatus.Quarantined, (await r.QuarantineAsync(SharedFileRepositoryTests.Entry(7, "{\"p\":1}"), "boom", _ctx, default)).Status);
        Assert.Equal(7, await r.GetLastAppliedIndexAsync(default));
        Assert.Equal(1, await Count("Misure"));
        Assert.Equal(1, await Count("Quarantine"));
        Assert.True((await r.ApplyAsync(SharedFileRepositoryTests.Entry(2, "{\"v\":1}", rid), _ctx, default)).Replay);
        await Assert.ThrowsAsync<ApplyGapException>(() => r.ApplyAsync(SharedFileRepositoryTests.Entry(9, "{}"), _ctx, default));
        Assert.Equal(a.Body, (await r.GetOutcomeAsync(rid, default))!.Body);
    }

    [SkippableFact]
    public async Task Each_endpoint_writes_its_own_table_and_ids_are_global_T58_T59_T61()
    {
        Skip.If(Conn == null, "RAFT_TEST_SQL non impostata");
        var r = Repo(); await r.InitializeAsync(default);
        var rid = Guid.NewGuid().ToString();
        Assert.Equal(ApplyStatus.Applied, (await r.ApplyAsync(SharedFileRepositoryTests.Entry(1, "{\"m\":1}", payloadType: "misure"), _ctx, default)).Status);
        Assert.Equal(ApplyStatus.Applied, (await r.ApplyAsync(SharedFileRepositoryTests.Entry(2, "{\"e\":1}", rid, payloadType: "eventi"), _ctx, default)).Status);
        // stesso RequestId e stesso body su un altro endpoint: conflitto, nessuna scrittura
        Assert.Equal(ApplyStatus.Conflict, (await r.ApplyAsync(SharedFileRepositoryTests.Entry(3, "{\"e\":1}", rid, payloadType: "misure"), _ctx, default)).Status);
        // endpoint sconosciuto all'applicatore: rifiutata, LastApplied avanza
        var unknown = await r.ApplyAsync(SharedFileRepositoryTests.Entry(4, "{}", payloadType: "ordini"), _ctx, default);
        Assert.Equal(ApplyStatus.Rejected, unknown.Status);
        Assert.Contains("ordini", Outcomes.BodyText(unknown));
        Assert.Equal(4, await r.GetLastAppliedIndexAsync(default));
        Assert.Equal(1, await Count("Misure"));
        Assert.Equal(1, await Count("Eventi"));
        await using var c = new SqlConnection(Conn); await c.OpenAsync();
        var types = new List<string>();
        await using (var rd = await new SqlCommand($"SELECT ISNULL(PayloadType, '') FROM [{_schema}].[AppliedEntries] ORDER BY LogIndex", c).ExecuteReaderAsync())
            while (await rd.ReadAsync()) types.Add(rd.GetString(0));
        Assert.Equal(new[] { "misure", "eventi", "misure", "ordini" }, types);
    }

    [SkippableFact]
    public async Task Tables_of_another_schema_version_block_initialization_T66()
    {
        Skip.If(Conn == null, "RAFT_TEST_SQL non impostata");
        await Repo().InitializeAsync(default);
        await Exec($"UPDATE [{_schema}].[SchemaVersion] SET Version = 1");
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => Repo().InitializeAsync(default));           // AutoCreateSchema: lo script si ferma
        Assert.Contains("SchemaVersion", ex.Message);
        await Assert.ThrowsAsync<SchemaVersionException>(() => Repo(autoCreate: false).InitializeAsync(default));
        // layout della v0.2: tabelle di servizio senza SchemaVersion
        await Exec($"DROP TABLE [{_schema}].[SchemaVersion]");
        ex = await Assert.ThrowsAnyAsync<Exception>(() => Repo().InitializeAsync(default));
        Assert.Contains("SchemaVersion", ex.Message);
        Assert.Equal(0, await Count("AppliedEntries"));
    }

    [SkippableFact]
    public async Task Concurrent_appliers_write_each_entry_exactly_once()
    {
        // Ex Leader e nuovo Leader che applicano contemporaneamente: la condizione su LastApplied garantisce una sola scrittura
        Skip.If(Conn == null, "RAFT_TEST_SQL non impostata");
        var a = Repo("a"); var b = Repo("b");
        await a.InitializeAsync(default);
        var entries = Enumerable.Range(1, 40).Select(i => SharedFileRepositoryTests.Entry(i, $"{{\"i\":{i}}}")).ToList();
        async Task Run(SqlServerDataRepository r)
        {
            foreach (var e in entries)
                for (int attempt = 0; ; attempt++)
                {
                    try { await r.ApplyAsync(e, _ctx, default); break; }
                    catch (Exception ex) when (r.IsTransient(ex) && attempt < 20) { await Task.Delay(20); }
                }
        }
        await Task.WhenAll(Run(a), Run(b));
        Assert.Equal(40, await a.GetLastAppliedIndexAsync(default));
        Assert.Equal(40, await Count("Misure"));
    }

    [SkippableFact]
    public async Task Constraint_violation_from_custom_writer_is_recorded_as_rejected()
    {
        Skip.If(Conn == null, "RAFT_TEST_SQL non impostata");
        var r = new SqlServerDataRepository(Cfg(), "c1", "n1", new Dictionary<string, ISqlPayloadWriter> { ["misure"] = new FailingWriter(547) });
        await r.InitializeAsync(default);
        var o = await r.ApplyAsync(SharedFileRepositoryTests.Entry(1, "{}"), _ctx, default);
        Assert.Equal(ApplyStatus.Rejected, o.Status);
        Assert.Equal(1, await r.GetLastAppliedIndexAsync(default));
    }

    private sealed class FailingWriter : ISqlPayloadWriter
    {
        private readonly int _err;
        public FailingWriter(int err) => _err = err;
        public async Task WriteAsync(SqlConnection c, SqlTransaction tx, LogEntry e, string clusterId, CancellationToken ct)
        {
            // genera un vero errore SQL del numero richiesto (547 = violazione di vincolo)
            await new SqlCommand($"RAISERROR(N'violazione simulata', 16, 1); THROW {50000 + 1}, N'errore applicativo', 1;", c, tx).ExecuteNonQueryAsync(ct);
        }
    }
}

/// <summary>Cluster di 3 nodi su SQL Server condiviso: caduta del Leader sotto carico, nessuna perdita né duplicazione.</summary>
[Trait("Category", "SqlServer")]
public sealed class SqlServerClusterTests
{
    private static readonly string? Conn = Environment.GetEnvironmentVariable("RAFT_TEST_SQL");

    [SkippableFact]
    public async Task Leader_stop_under_load_with_shared_sql_server()
    {
        Skip.If(Conn == null, "RAFT_TEST_SQL non impostata");
        var schema = "c" + Guid.NewGuid().ToString("N")[..10];
        await using var c = TestCluster.Create(3);
        c.Extra["RaftCluster:StateMachine:Provider"] = "SqlServer";
        c.Extra["RaftCluster:StateMachine:SqlServer:ConnectionString"] = Conn;
        c.Extra["RaftCluster:StateMachine:SqlServer:Schema"] = schema;
        c.Extra["RaftCluster:Endpoints:0:Table"] = schema + ".Misure";
        c.Extra["RaftCluster:Endpoints:1:Table"] = schema + ".Eventi";
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync(TimeSpan.FromSeconds(30));
        int entry = c.Running.First(i => i != l);
        static string Path(int k) => k % 2 == 0 ? TestCluster.MisurePath : TestCluster.EventiPath;
        var ids = Enumerable.Range(0, 60).Select(_ => Guid.NewGuid().ToString()).ToList();
        var tasks = new List<Task<(int Status, string Body, HttpResponseMessage Resp)>>();
        for (int k = 0; k < ids.Count; k++)
        {
            tasks.Add(c.PostAsync(entry, ids[k], $"{{\"k\":{k}}}", path: Path(k)));
            if (k == 20) _ = c.StopAsync(l);
            await Task.Delay(30);
        }
        var res = await Task.WhenAll(tasks);
        for (int k = 0; k < ids.Count; k++)
            for (int a = 0; res[k].Status != 200 && a < 5; a++) res[k] = await c.PostAsync(entry, ids[k], $"{{\"k\":{k}}}", path: Path(k));
        Assert.All(res, r => Assert.Equal(200, r.Status));
        await using var conn = new SqlConnection(Conn); await conn.OpenAsync();
        async Task<long> Scalar(string sql) => Convert.ToInt64(await new SqlCommand(sql, conn).ExecuteScalarAsync());
        Assert.Equal(30, await Scalar($"SELECT COUNT(*) FROM [{schema}].[Misure]"));
        Assert.Equal(30, await Scalar($"SELECT COUNT(*) FROM [{schema}].[Eventi]"));
        Assert.Equal(60, await Scalar($"SELECT COUNT(*) FROM (SELECT RequestId FROM [{schema}].[Misure] UNION SELECT RequestId FROM [{schema}].[Eventi]) u"));
    }
}
