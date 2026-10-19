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

    private SqlServerConfig Cfg() => new() { ConnectionString = Conn!, Schema = _schema, DataTable = _schema + ".IngestedData", AutoCreateSchema = true };
    private SqlServerDataRepository Repo(string node = "n1") => new(Cfg(), "c1", node);

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (Conn == null) return;
        await using var c = new SqlConnection(Conn); await c.OpenAsync();
        foreach (var t in new[] { "IngestedData", "Quarantine", "Requests", "AppliedEntries", "ApplyState" })
            await new SqlCommand($"IF OBJECT_ID(N'[{_schema}].[{t}]') IS NOT NULL DROP TABLE [{_schema}].[{t}]", c).ExecuteNonQueryAsync();
        await new SqlCommand($"IF SCHEMA_ID(N'{_schema}') IS NOT NULL EXEC(N'DROP SCHEMA [{_schema}]')", c).ExecuteNonQueryAsync();
    }

    private async Task<long> Count(string table)
    {
        await using var c = new SqlConnection(Conn); await c.OpenAsync();
        return Convert.ToInt64(await new SqlCommand($"SELECT COUNT(*) FROM [{_schema}].[{table}]", c).ExecuteScalarAsync());
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
        Assert.Equal(1, await Count("IngestedData"));
        Assert.Equal(1, await Count("Quarantine"));
        Assert.True((await r.ApplyAsync(SharedFileRepositoryTests.Entry(2, "{\"v\":1}", rid), _ctx, default)).Replay);
        await Assert.ThrowsAsync<ApplyGapException>(() => r.ApplyAsync(SharedFileRepositoryTests.Entry(9, "{}"), _ctx, default));
        Assert.Equal(a.Body, (await r.GetOutcomeAsync(rid, default))!.Body);
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
        Assert.Equal(40, await Count("IngestedData"));
    }

    [SkippableFact]
    public async Task Constraint_violation_from_custom_writer_is_recorded_as_rejected()
    {
        Skip.If(Conn == null, "RAFT_TEST_SQL non impostata");
        var r = new SqlServerDataRepository(Cfg(), "c1", "n1", new FailingWriter(547));
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
        c.Extra["RaftCluster:StateMachine:SqlServer:DataTable"] = schema + ".IngestedData";
        await c.StartAllAsync();
        int l = await c.WaitForLeaderAsync(TimeSpan.FromSeconds(30));
        int entry = c.Running.First(i => i != l);
        var ids = Enumerable.Range(0, 60).Select(_ => Guid.NewGuid().ToString()).ToList();
        var tasks = new List<Task<(int Status, string Body, HttpResponseMessage Resp)>>();
        for (int k = 0; k < ids.Count; k++)
        {
            tasks.Add(c.PostAsync(entry, ids[k], $"{{\"k\":{k}}}"));
            if (k == 20) _ = c.StopAsync(l);
            await Task.Delay(30);
        }
        var res = await Task.WhenAll(tasks);
        for (int k = 0; k < ids.Count; k++)
            for (int a = 0; res[k].Status != 200 && a < 5; a++) res[k] = await c.PostAsync(entry, ids[k], $"{{\"k\":{k}}}");
        Assert.All(res, r => Assert.Equal(200, r.Status));
        await using var conn = new SqlConnection(Conn); await conn.OpenAsync();
        var count = Convert.ToInt64(await new SqlCommand($"SELECT COUNT(*) FROM [{schema}].[IngestedData]", conn).ExecuteScalarAsync());
        var distinct = Convert.ToInt64(await new SqlCommand($"SELECT COUNT(DISTINCT RequestId) FROM [{schema}].[IngestedData]", conn).ExecuteScalarAsync());
        Assert.Equal(60, count);
        Assert.Equal(60, distinct);
    }
}
