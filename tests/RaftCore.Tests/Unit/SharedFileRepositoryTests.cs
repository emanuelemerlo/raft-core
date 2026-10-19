using System.Security.Cryptography;
using System.Text;
using RaftCore.Abstractions;
using RaftCore.Configuration;
using RaftCore.Models;
using RaftCore.Services.Storage;
using RaftCore.Tests.Infrastructure;
using Xunit;

namespace RaftCore.Tests.Unit;

public class SharedFileRepositoryTests
{
    private static readonly ApplyContext Ctx = new(DateTime.UtcNow, TimeSpan.FromHours(1), "c1");

    internal static LogEntry Entry(long idx, string json, string? rid = null, DateTime? received = null, LogEntryType type = LogEntryType.Data)
    {
        var p = Encoding.UTF8.GetBytes(json);
        return new LogEntry
        {
            Index = idx, Term = 1, EntryType = type, RequestId = rid ?? Guid.NewGuid().ToString(), Payload = p,
            PayloadHash = Convert.ToHexString(SHA256.HashData(p)).ToLowerInvariant(), ReceivedUtc = received ?? DateTime.UtcNow
        };
    }

    private static SharedFileDataRepository Repo(string dir) => new(new SharedFileConfig { Directory = dir }, fsync: true);

    [Fact]
    public async Task Applies_in_order_and_replays_already_applied_entries()
    {
        using var d = new TempDir();
        var r = Repo(d.Path); await r.InitializeAsync(default);
        var e1 = Entry(1, "{\"a\":1}");
        var o1 = await r.ApplyAsync(e1, Ctx, default);
        Assert.Equal(ApplyStatus.Applied, o1.Status);
        Assert.Equal(200, o1.HttpStatus);
        var again = await r.ApplyAsync(e1, Ctx, default);
        Assert.True(again.Replay);
        Assert.Equal(o1.Body, again.Body);
        Assert.Equal(1, await r.GetLastAppliedIndexAsync(default));
        await Assert.ThrowsAsync<ApplyGapException>(() => r.ApplyAsync(Entry(3, "{}"), Ctx, default));
    }

    [Fact]
    public async Task Second_node_never_rewrites_entries_applied_by_the_first()
    {
        // Punto 1: un nodo rientrato dopo settimane (o un ex Leader) non riscrive il database condiviso
        using var d = new TempDir();
        var nodeA = Repo(d.Path); var nodeB = Repo(d.Path);
        await nodeA.InitializeAsync(default); await nodeB.InitializeAsync(default);
        var entries = Enumerable.Range(1, 5).Select(i => Entry(i, $"{{\"i\":{i}}}")).ToList();
        foreach (var e in entries) await nodeA.ApplyAsync(e, Ctx, default);
        foreach (var e in entries)
        {
            var o = await nodeB.ApplyAsync(e, Ctx, default);
            Assert.True(o.Replay);
        }
        Assert.Equal(5, File.ReadAllLines(nodeA.FilePath).Length);
        await nodeB.ApplyAsync(Entry(6, "{\"i\":6}"), Ctx, default);
        Assert.Equal(6, await nodeA.GetLastAppliedIndexAsync(default));
    }

    [Fact]
    public async Task Duplicate_request_returns_original_outcome_and_conflict_returns_422()
    {
        using var d = new TempDir();
        var r = Repo(d.Path); await r.InitializeAsync(default);
        var rid = Guid.NewGuid().ToString();
        var first = await r.ApplyAsync(Entry(1, "{\"v\":1}", rid), Ctx, default);
        var dup = await r.ApplyAsync(Entry(2, "{\"v\":1}", rid), Ctx, default);
        Assert.Equal(ApplyStatus.Duplicate, dup.Status);
        Assert.Equal(first.Body, dup.Body);
        Assert.True(dup.Replay);
        var conflict = await r.ApplyAsync(Entry(3, "{\"v\": 1}", rid), Ctx, default);   // stesso ID, spaziatura diversa
        Assert.Equal(ApplyStatus.Conflict, conflict.Status);
        Assert.Equal(422, conflict.HttpStatus);
        var stored = await r.GetOutcomeAsync(rid, default);
        Assert.Equal(first.Body, stored!.Body);
    }

    [Fact]
    public async Task Entry_older_than_MaxPendingAge_is_expired_and_not_written()
    {
        using var d = new TempDir();
        var r = Repo(d.Path); await r.InitializeAsync(default);
        var old = Entry(1, "{\"stale\":true}", received: DateTime.UtcNow.AddDays(-14));
        var o = await r.ApplyAsync(old, Ctx, default);
        Assert.Equal(ApplyStatus.Expired, o.Status);
        Assert.Equal(410, o.HttpStatus);
        var line = File.ReadAllLines(r.FilePath).Single().Split('\t');
        Assert.Equal("", line[6]);                                                    // nessun dato scritto
        Assert.Equal(ApplyStatus.Expired, (await r.GetOutcomeAsync(old.RequestId, default))!.Status);
        // disattivato con MaxPendingAge = 0
        var o2 = await r.ApplyAsync(Entry(2, "{\"stale\":true}", received: DateTime.UtcNow.AddDays(-14)), Ctx with { MaxPendingAge = TimeSpan.Zero }, default);
        Assert.Equal(ApplyStatus.Applied, o2.Status);
    }

    [Fact]
    public async Task Invalid_business_payload_is_rejected_deterministically()
    {
        using var d = new TempDir();
        var r = Repo(d.Path); await r.InitializeAsync(default);
        var o = await r.ApplyAsync(Entry(1, "[1,2,3]"), Ctx, default);
        Assert.Equal(ApplyStatus.Rejected, o.Status);
        Assert.Equal(422, o.HttpStatus);
        Assert.Equal(1, await r.GetLastAppliedIndexAsync(default));                  // il cluster prosegue
    }

    [Fact]
    public async Task Quarantine_records_poison_entry_and_advances()
    {
        using var d = new TempDir();
        var r = Repo(d.Path); await r.InitializeAsync(default);
        var e = Entry(1, "{\"poison\":true}");
        var o = await r.QuarantineAsync(e, "boom", Ctx, default);
        Assert.Equal(ApplyStatus.Quarantined, o.Status);
        Assert.Equal(1, await r.GetLastAppliedIndexAsync(default));
        Assert.Contains("quarantined", Encoding.UTF8.GetString((await r.GetOutcomeAsync(e.RequestId, default))!.Body));
    }

    [Fact]
    public async Task Torn_line_written_by_a_crashed_node_is_repaired()
    {
        using var d = new TempDir();
        var r = Repo(d.Path); await r.InitializeAsync(default);
        await r.ApplyAsync(Entry(1, "{}"), Ctx, default);
        File.AppendAllText(r.FilePath, "2\t1\tabc");                                 // riga incompleta
        var r2 = Repo(d.Path);
        Assert.Equal(1, await r2.GetLastAppliedIndexAsync(default));
        await r2.ApplyAsync(Entry(2, "{}"), Ctx, default);
        Assert.Equal(2, File.ReadAllLines(r.FilePath).Length);
    }
}
