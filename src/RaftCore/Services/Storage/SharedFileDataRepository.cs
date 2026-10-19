using System.Text;
using System.Text.Json;
using RaftCore.Abstractions;
using RaftCore.Configuration;
using RaftCore.Models;
using RaftCore.Services.Common;

namespace RaftCore.Services.Storage;

/// <summary>
/// State machine condivisa su file, per sviluppo e test sulla stessa macchina (o su una share locale).
/// Emula la semantica del database condiviso: un lock tra processi fa da transazione, ogni riga di applied.log
/// è il punto di commit di una voce (LogIndex, esito, payload) e LastApplied è l'indice dell'ultima riga.
/// Formato riga: LogIndex \t Status \t RequestId \t PayloadHash \t HttpStatus \t Base64(risposta) \t Base64(payload)
/// NON adatto alla produzione: l'indice delle richieste è in memoria.
/// </summary>
public sealed class SharedFileDataRepository : IDataRepository
{
    private sealed record Rec(long Index, ApplyStatus Status, string RequestId, string Hash, ApplyOutcome Outcome);

    private readonly string _dir, _file, _lock;
    private readonly int _lockTimeoutMs;
    private readonly bool _fsync;
    private readonly SemaphoreSlim _local = new(1, 1);
    private readonly Dictionary<string, Rec> _byRequest = new();
    private readonly Dictionary<long, ApplyOutcome> _byIndex = new();
    private long _offset, _lastApplied;

    /// <summary>Solo test: eccezione da sollevare per una voce (simula errori transitori o poison).</summary>
    public Func<LogEntry, Exception?>? FaultInjector { get; set; }

    public SharedFileDataRepository(SharedFileConfig cfg, bool fsync)
    {
        _dir = Path.GetFullPath(cfg.Directory);
        _file = Path.Combine(_dir, "applied.log");
        _lock = Path.Combine(_dir, "applied.lock");
        _lockTimeoutMs = cfg.LockTimeoutMs;
        _fsync = fsync;
    }

    public string FilePath => _file;

    public async Task InitializeAsync(CancellationToken ct)
    {
        Directory.CreateDirectory(_dir);
        await WithLockAsync(() => { Refresh(); return 0; }, ct);
    }

    public Task<long> GetLastAppliedIndexAsync(CancellationToken ct) => WithLockAsync(() => { Refresh(); return _lastApplied; }, ct);

    public Task<ApplyOutcome?> GetOutcomeAsync(string requestId, CancellationToken ct) =>
        WithLockAsync(() => { Refresh(); return _byRequest.TryGetValue(requestId, out var r) ? r.Outcome.AsReplay() : (ApplyOutcome?)null; }, ct);

    public Task<ApplyOutcome> ApplyAsync(LogEntry entry, ApplyContext ctx, CancellationToken ct) =>
        WithLockAsync(() =>
        {
            Refresh();
            if (entry.Index <= _lastApplied) return Replay(entry.Index);
            if (entry.Index != _lastApplied + 1) throw new ApplyGapException(_lastApplied, entry.Index);
            var fault = FaultInjector?.Invoke(entry);
            if (fault != null) throw fault;
            var (outcome, record) = Decide(entry, ctx);
            AppendLine(entry, outcome, record ? entry.Payload : Array.Empty<byte>());
            return outcome;
        }, ct);

    public Task<ApplyOutcome> QuarantineAsync(LogEntry entry, string reason, ApplyContext ctx, CancellationToken ct) =>
        WithLockAsync(() =>
        {
            Refresh();
            if (entry.Index <= _lastApplied) return Replay(entry.Index);
            if (entry.Index != _lastApplied + 1) throw new ApplyGapException(_lastApplied, entry.Index);
            var o = Outcomes.Quarantined(entry.Index, entry.RequestId, reason);
            AppendLine(entry, o, entry.Payload); // il payload resta disponibile per il riprocessamento manuale
            return o;
        }, ct);

    public bool IsTransient(Exception ex) => ex is IOException or TimeoutException or UnauthorizedAccessException;

    // ------------------------------------------------------------------ decisione deterministica
    private (ApplyOutcome Outcome, bool StorePayload) Decide(LogEntry e, ApplyContext ctx)
    {
        if (e.EntryType == LogEntryType.NoOp) return (Outcomes.NoOp(e.Index), false);
        if (_byRequest.TryGetValue(e.RequestId, out var first))
        {
            if (first.Hash != e.PayloadHash) return (Outcomes.Conflict(e.Index, e.RequestId), false);
            return (first.Outcome with { Status = ApplyStatus.Duplicate, LogIndex = e.Index, Replay = true }, false);
        }
        if (ctx.MaxPendingAge > TimeSpan.Zero && ctx.NowUtc - e.ReceivedUtc > ctx.MaxPendingAge)
            return (Outcomes.Expired(e.Index, e.RequestId, e.ReceivedUtc), false);
        if (!IsJsonObject(e.Payload))
            return (Outcomes.Rejected(e.Index, e.RequestId, "il payload deve essere un oggetto JSON"), false);
        return (Outcomes.Applied(e.Index, e.RequestId), true);
    }

    private static bool IsJsonObject(byte[] p)
    {
        try { using var d = JsonDocument.Parse(p); return d.RootElement.ValueKind == JsonValueKind.Object; }
        catch (JsonException) { return false; }
    }

    private ApplyOutcome Replay(long index) =>
        _byIndex.TryGetValue(index, out var o) ? o.AsReplay() : Outcomes.NoOp(index);

    // ------------------------------------------------------------------ file
    private void AppendLine(LogEntry e, ApplyOutcome o, byte[] payload)
    {
        var line = $"{e.Index}\t{(int)o.Status}\t{e.RequestId}\t{e.PayloadHash}\t{o.HttpStatus}\t{Convert.ToBase64String(o.Body)}\t{Convert.ToBase64String(payload)}\n";
        var bytes = Encoding.ASCII.GetBytes(line);
        using (var fs = new FileStream(_file, FileMode.Append, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete))
        {
            fs.Write(bytes);
            if (_fsync) Durability.FlushFile(fs); else fs.Flush();
        }
        _offset += bytes.Length;
        Index(e.Index, o.Status, e.RequestId, e.PayloadHash, o);
    }

    private void Index(long index, ApplyStatus status, string requestId, string hash, ApplyOutcome o)
    {
        _lastApplied = index;
        _byIndex[index] = o with { Replay = false };
        if (status is not (ApplyStatus.NoOp or ApplyStatus.Duplicate or ApplyStatus.Conflict) && !_byRequest.ContainsKey(requestId))
            _byRequest[requestId] = new Rec(index, status, requestId, hash, o with { Replay = false });
    }

    /// <summary>Legge le righe scritte da altri nodi dall'ultima lettura; ripara una riga finale incompleta.</summary>
    private void Refresh()
    {
        if (!File.Exists(_file)) { _offset = 0; return; }
        using var fs = new FileStream(_file, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
        if (fs.Length < _offset) { _offset = 0; _byIndex.Clear(); _byRequest.Clear(); _lastApplied = 0; } // file sostituito
        if (fs.Length == _offset) return;
        fs.Seek(_offset, SeekOrigin.Begin);
        var buf = new byte[fs.Length - _offset];
        fs.ReadExactly(buf);
        int lastNl = Array.LastIndexOf(buf, (byte)'\n');
        if (lastNl < buf.Length - 1)
        {
            // crash di un altro nodo a metà scrittura: siamo sotto lock, la coda incompleta si può eliminare
            fs.SetLength(_offset + lastNl + 1);
            if (_fsync) Durability.FlushFile(fs);
        }
        if (lastNl < 0) return;
        foreach (var line in Encoding.ASCII.GetString(buf, 0, lastNl).Split('\n'))
        {
            var f = line.Split('\t');
            if (f.Length < 7 || !long.TryParse(f[0], out var idx) || !int.TryParse(f[1], out var st) || !int.TryParse(f[4], out var http))
                throw new InvalidDataException($"applied.log: riga non valida: {line}");
            var status = (ApplyStatus)st;
            Index(idx, status, f[2], f[3], new ApplyOutcome(status, http, Convert.FromBase64String(f[5]), idx));
        }
        _offset += lastNl + 1;
    }

    /// <summary>Transazione tra processi: lock esclusivo su applied.lock (FileShare.None) con timeout.</summary>
    private async Task<T> WithLockAsync<T>(Func<T> body, CancellationToken ct)
    {
        await _local.WaitAsync(ct);
        try
        {
            long end = Environment.TickCount64 + _lockTimeoutMs;
            while (true)
            {
                FileStream? l = null;
                try { l = new FileStream(_lock, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None); }
                catch (IOException) when (Environment.TickCount64 < end) { await Task.Delay(10, ct); continue; }
                catch (IOException ex) { throw new TimeoutException("Lock del repository condiviso non ottenuto: " + ex.Message, ex); }
                using (l) return body();
            }
        }
        finally { _local.Release(); }
    }
}
