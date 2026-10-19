using System.Text;
using RaftCore.Abstractions;
using RaftCore.Models;
using RaftCore.Services.Common;

namespace RaftCore.Services.Storage;

/// <summary>
/// State machine su file di testo (REQ-SM-3). Una riga per voce applicata:
///   LogIndex \t KIND \t RequestId \t Base64(payload)       KIND = DATA | NOOP | DUP | CONFLICT
/// (per DUP/CONFLICT il campo finale è l'indice della prima voce). Append + flush precedono l'avanzamento di lastApplied;
/// lastApplied è ricavato dalla coda del file più recente (anche dopo rotazione), quindi un crash non produce righe duplicate.
/// </summary>
public sealed class FileDataRepository : IDataRepository
{
    private readonly string _active;
    private readonly string _dir, _stem, _ext;
    private readonly long _maxBytes;
    private readonly bool _fsync;
    private readonly object _lock = new();
    private long _lastApplied;
    private FileStream? _fs;
    /// <summary>Risolve una voce del log (per ricostruire il risultato dopo restart).</summary>
    public Func<long, LogEntry?>? EntryResolver { get; set; }

    public FileDataRepository(string outputPath, long maxBytes, bool fsync)
    {
        _active = Path.GetFullPath(outputPath);
        _dir = Path.GetDirectoryName(_active)!; _stem = Path.GetFileNameWithoutExtension(_active); _ext = Path.GetExtension(_active);
        _maxBytes = maxBytes; _fsync = fsync;
        Directory.CreateDirectory(_dir);
        Initialize();
    }

    private void Initialize()
    {
        RepairTail(_active);
        long last = LastIndexInFile(_active);
        if (last == 0)
        {
            // file attivo vuoto/assente: cerca l'ultimo file ruotato
            var rotated = Directory.GetFiles(_dir, $"{_stem}.*{_ext}")
                .Select(f => (f, n: long.TryParse(Path.GetFileNameWithoutExtension(f)[(_stem.Length + 1)..], out var v) ? v : -1))
                .Where(x => x.n >= 0).OrderByDescending(x => x.n).ToList();
            foreach (var r in rotated) { RepairTail(r.f); last = LastIndexInFile(r.f); if (last > 0) break; }
        }
        _lastApplied = last;
        _fs = new FileStream(_active, FileMode.Append, FileAccess.Write, FileShare.ReadWrite, 1);
    }

    /// <summary>Tronca una riga finale incompleta (crash a metà scrittura).</summary>
    private static void RepairTail(string path)
    {
        if (!File.Exists(path)) return;
        using var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        if (fs.Length == 0) return;
        fs.Seek(-1, SeekOrigin.End);
        if (fs.ReadByte() == '\n') return;
        long pos = fs.Length; var buf = new byte[65536]; long cut = 0;
        while (pos > 0)
        {
            int n = (int)Math.Min(buf.Length, pos); pos -= n; fs.Seek(pos, SeekOrigin.Begin);
            fs.ReadExactly(buf, 0, n);
            int i = Array.LastIndexOf(buf, (byte)'\n', n - 1);
            if (i >= 0) { cut = pos + i + 1; break; }
        }
        fs.SetLength(cut); fs.Flush(true);
    }

    private static long LastIndexInFile(string path)
    {
        if (!File.Exists(path)) return 0;
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        if (fs.Length == 0) return 0;
        int chunk = 65536; long end = fs.Length - 1; // ultimo byte = '\n'
        while (true)
        {
            long start = Math.Max(0, fs.Length - chunk);
            var buf = new byte[fs.Length - start]; fs.Seek(start, SeekOrigin.Begin); fs.ReadExactly(buf, 0, buf.Length);
            int lineEnd = buf.Length - 1;
            int prev = lineEnd > 0 ? Array.LastIndexOf(buf, (byte)'\n', lineEnd - 1) : -1;
            if (prev >= 0 || start == 0)
            {
                var line = Encoding.ASCII.GetString(buf, prev + 1, lineEnd - (prev + 1));
                var tab = line.IndexOf('\t');
                return tab > 0 && long.TryParse(line.AsSpan(0, tab), out var idx) ? idx : 0;
            }
            chunk *= 2;
        }
    }

    public Task<long> GetLastAppliedIndexAsync(CancellationToken ct) { lock (_lock) return Task.FromResult(_lastApplied); }

    public Task ApplyAsync(LogEntry entry, CancellationToken ct)
    {
        lock (_lock)
        {
            if (entry.Index <= _lastApplied) return Task.CompletedTask; // REQ-SM-1/SM-3: no-op
            string kind, tail;
            if (entry.EntryType == LogEntryType.NoOp) { kind = "NOOP"; tail = ""; }
            else if (entry.IdConflict) { kind = "CONFLICT"; tail = entry.DuplicateOf.ToString(); }
            else if (entry.DuplicateOf > 0) { kind = "DUP"; tail = entry.DuplicateOf.ToString(); }
            else { kind = "DATA"; tail = Convert.ToBase64String(entry.Payload); }
            var line = Encoding.ASCII.GetBytes($"{entry.Index}\t{kind}\t{entry.RequestId}\t{tail}\n");
            _fs!.Write(line, 0, line.Length);
            _fs.Flush(_fsync);
            _lastApplied = entry.Index;
            if (_fs.Length >= _maxBytes) Rotate(entry.Index);
        }
        return Task.CompletedTask;
    }

    private void Rotate(long lastIndex)
    {
        _fs!.Dispose();
        File.Move(_active, Path.Combine(_dir, $"{_stem}.{lastIndex}{_ext}"));
        DirSync.Sync(_dir);
        _fs = new FileStream(_active, FileMode.Create, FileAccess.Write, FileShare.ReadWrite, 1);
        DirSync.Sync(_dir);
    }

    public Task<byte[]> GetResultAsync(long logIndex, CancellationToken ct)
    {
        var e = EntryResolver?.Invoke(logIndex);
        if (e == null || e.EntryType == LogEntryType.NoOp) return Task.FromResult(Array.Empty<byte>());
        // Risposta deterministica: identica su tutti i nodi e dopo un failover (REQ-IDM-2/IDM-7).
        return Task.FromResult(Encoding.UTF8.GetBytes($"{{\"status\":\"Committed\",\"logIndex\":{e.Index},\"requestId\":\"{e.RequestId}\"}}"));
    }
}
