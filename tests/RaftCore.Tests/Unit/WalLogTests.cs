using System.Text;
using RaftCore.Models;
using RaftCore.Services.Storage;
using RaftCore.Tests.Infrastructure;
using Xunit;

namespace RaftCore.Tests.Unit;

public class WalLogTests
{
    private static WalInput Data(long idx, long term, string? payload = null) =>
        new(idx, term, LogEntryType.Data, Guid.NewGuid().ToString(), new string('a', 64), DateTime.UtcNow.Ticks,
            Encoding.UTF8.GetBytes(payload ?? $"{{\"i\":{idx}}}"));

    private static WalLog Open(string dir, long segBytes = 64 * 1024 * 1024)
    {
        var w = new WalLog(dir, true, segBytes);
        var r = w.Open();
        Assert.False(r.CorruptNonFinal, r.Message);
        return w;
    }

    [Fact]
    public void Append_read_and_reopen_preserve_entries()
    {
        using var d = new TempDir();
        using (var w = Open(d.Path))
        {
            w.Append(0, new[] { Data(1, 1), Data(2, 1), Data(3, 2) });
            Assert.Equal(3, w.LastIndex);
            Assert.Equal(2, w.LastTerm);
            Assert.Equal("{\"i\":2}", Encoding.UTF8.GetString(w.ReadEntry(2)!.Payload));
        }
        using var w2 = Open(d.Path);
        Assert.Equal(3, w2.LastIndex);
        Assert.Equal(1, w2.TermAt(2));
        Assert.Equal("{\"i\":3}", Encoding.UTF8.GetString(w2.ReadEntry(3)!.Payload));
    }

    [Fact]
    public void Truncate_record_removes_conflicting_suffix_and_survives_restart()
    {
        using var d = new TempDir();
        using (var w = Open(d.Path))
        {
            w.Append(0, new[] { Data(1, 1), Data(2, 1), Data(3, 1) });
            w.Append(2, new[] { Data(2, 2) });
            Assert.Equal(2, w.LastIndex);
            Assert.Equal(2, w.TermAt(2));
        }
        using var w2 = Open(d.Path);
        Assert.Equal(2, w2.LastIndex);
        Assert.Equal(2, w2.TermAt(2));
    }

    [Fact]
    public void Torn_final_record_is_truncated_T22()
    {
        using var d = new TempDir();
        using (var w = Open(d.Path)) w.Append(0, new[] { Data(1, 1), Data(2, 1) });
        var seg = Directory.GetFiles(d.Path, "*.wal").Single();
        long len = new FileInfo(seg).Length;
        using (var fs = new FileStream(seg, FileMode.Open)) fs.SetLength(len - 7);   // crash durante la scrittura del secondo record
        using var w2 = new WalLog(d.Path, true, 64 * 1024 * 1024);
        var r = w2.Open();
        Assert.False(r.CorruptNonFinal);
        Assert.True(r.TruncatedBytes > 0);
        Assert.Equal(1, w2.LastIndex);
        w2.Append(0, new[] { Data(2, 1) });                                         // si continua a scrivere dopo il troncamento
        Assert.Equal(2, w2.LastIndex);
    }

    [Fact]
    public void Corrupt_non_final_record_requires_explicit_recovery()
    {
        using var d = new TempDir();
        using (var w = Open(d.Path)) w.Append(0, new[] { Data(1, 1), Data(2, 1), Data(3, 1) });
        var seg = Directory.GetFiles(d.Path, "*.wal").Single();
        var bytes = File.ReadAllBytes(seg);
        bytes[40] ^= 0xFF;                                                          // corrompe il primo record
        File.WriteAllBytes(seg, bytes);
        using var w2 = new WalLog(d.Path, true, 64 * 1024 * 1024);
        Assert.True(w2.Open().CorruptNonFinal);
    }

    [Fact]
    public void Segments_rotate_and_compaction_deletes_covered_segments_and_frees_ram()
    {
        using var d = new TempDir();
        var big = new string('x', 300 * 1024);
        using (var w = Open(d.Path, segBytes: 1024 * 1024))
        {
            for (int i = 1; i <= 20; i++) w.Append(0, new[] { Data(i, 1, $"{{\"p\":\"{big}\"}}") });
            int segs = w.SegmentCount;
            Assert.True(segs >= 5, $"segmenti: {segs}");
            Assert.Equal(15, w.Compact(15));
            Assert.Equal(15, w.SnapshotIndex);
            Assert.Equal(5, w.WindowCount);
            Assert.True(w.SegmentCount < segs);
            Assert.Null(w.ReadEntry(10));
            Assert.Equal(-1, w.TermAt(10));
            Assert.Equal(1, w.TermAt(15));
            Assert.NotNull(w.ReadEntry(16));
        }
        Assert.True(Directory.GetFiles(d.Path, "*.wal").Length < 8);
        using var w2 = Open(d.Path, segBytes: 1024 * 1024);
        Assert.Equal(15, w2.SnapshotIndex);
        Assert.Equal(20, w2.LastIndex);
        Assert.NotNull(w2.ReadEntry(20));
    }

    [Fact]
    public void ResetToSnapshot_discards_divergent_log_crash_safely()
    {
        using var d = new TempDir();
        using (var w = Open(d.Path))
        {
            w.Append(0, new[] { Data(1, 1), Data(2, 1), Data(3, 1) });
            w.ResetToSnapshot(100, 7);
            Assert.Equal(100, w.LastIndex);
            Assert.Equal(7, w.LastTerm);
            Assert.Equal(0, w.WindowCount);
            w.Append(0, new[] { Data(101, 7) });
        }
        using var w2 = Open(d.Path);
        Assert.Equal(100, w2.SnapshotIndex);
        Assert.Equal(101, w2.LastIndex);
        Assert.Equal(7, w2.TermAt(101));
    }

    [Fact]
    public void TryAppend_fails_when_another_writer_changed_the_log()
    {
        using var d = new TempDir();
        using var w = Open(d.Path);
        w.Append(0, new[] { Data(1, 1) });
        long gen = w.Generation;
        w.Append(0, new[] { Data(2, 1) });
        Assert.False(w.TryAppend(gen, new[] { Data(3, 1) }));
        Assert.True(w.TryAppend(w.Generation, new[] { Data(3, 1) }));
    }

    [Fact]
    public void Non_contiguous_append_is_rejected_without_marking_wal_failed()
    {
        using var d = new TempDir();
        using var w = Open(d.Path);
        Assert.Throws<InvalidOperationException>(() => w.Append(0, new[] { Data(2, 1) }));
        Assert.False(w.Failed);
        w.Append(0, new[] { Data(1, 1) });
    }

    [Fact]
    public void Large_record_remains_valid_regardless_of_configured_body_limit()
    {
        // v0.1: abbassare MaxBodyBytes rendeva "corrotti" e troncava record committed più grandi
        using var d = new TempDir();
        var payload = new string('y', 3 * 1024 * 1024);
        using (var w = Open(d.Path)) w.Append(0, new[] { Data(1, 1, $"{{\"p\":\"{payload}\"}}") });
        using var w2 = Open(d.Path);
        Assert.Equal(1, w2.LastIndex);
    }

    [Fact]
    public void Commit_index_file_tolerates_corruption()
    {
        using var d = new TempDir();
        using (var c = new CommitIndexFile(d.Path)) { c.Write(42); Assert.Equal(42, c.Read()); }
        var f = Path.Combine(d.Path, "commit.idx");
        var b = File.ReadAllBytes(f); b[3] ^= 0x55; File.WriteAllBytes(f, b);
        using var c2 = new CommitIndexFile(d.Path);
        Assert.Equal(0, c2.Read());
    }
}
