using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Microsoft.Win32.SafeHandles;
using RaftCore.Configuration;
using RaftCore.Models;
using RaftCore.Services.Common;

namespace RaftCore.Services.Storage;

public sealed class WalMeta
{
    public long Term;
    public LogEntryType Type;
    public string RequestId = "";
    public string PayloadHash = "";
    public long ReceivedTicks;
    internal WalSegment Segment = null!;
    public long PayloadOffset;
    public int PayloadLen;
}

public readonly record struct WalInput(long Index, long Term, LogEntryType Type, string RequestId, string PayloadHash, long ReceivedTicks, ReadOnlyMemory<byte> Payload);

public sealed record WalOpenResult(bool CorruptNonFinal, string? Message, long TruncatedBytes, bool HadData);

public sealed class WalIoException : IOException { public WalIoException(string m, Exception? inner = null) : base(m, inner) { } }

internal sealed class WalSegment
{
    public int Seq;
    public string Path = "";
    public FileStream Fs = null!;
    public SafeFileHandle H = null!;
    public long Length;
    /// <summary>Indice massimo citato da un record (voce o troncamento) del segmento: decide quando è eliminabile.</summary>
    public long MaxIndex;
}

/// <summary>
/// WAL append-only a segmenti (REQ-WAL-1..3, REQ-SNP).
/// Record: magic(4) ver(1) tipo(1) lunghezza(4) index(8) term(8) corpo(n) crc32c(4).
/// - Il troncamento di voci in conflitto è un record "Truncate" appeso: i file non vengono mai riscritti.
/// - La compattazione scrive snapshot.json (SnapshotIndex, SnapshotTerm) in modo durevole e poi elimina i segmenti
///   interamente precedenti: RAM e disco restano limitati alla finestra di log non ancora compattata.
/// - Scritture serializzate da un lock dedicato; i lettori usano un lock separato e non attendono l'fsync.
/// </summary>
public sealed class WalLog : IDisposable
{
    private const uint Magic = 0x4C415752;
    private const byte Version = 1;
    private const byte RecEntry = 1, RecTruncate = 2;
    private const int HeaderSize = 26, CrcSize = 4;
    /// <summary>Limite di formato, indipendente dalla configurazione: ridurre MaxBodyBytes non rende "corrotti" record esistenti.</summary>
    public const int MaxRecordBody = (int)(ClusterConfig.MaxBodyBytesLimit + 64 * 1024);

    private readonly string _dir;
    private readonly string _snapPath;
    private readonly bool _fsync;
    private readonly long _segmentBytes;
    private readonly Func<bool>? _faultInjector;
    private readonly Action<TimeSpan>? _onFsync;
    private readonly object _sync = new();
    private readonly SemaphoreSlim _write = new(1, 1);
    private readonly List<WalMeta> _metas = new();
    private readonly List<WalSegment> _segments = new();
    private readonly List<(WalSegment Seg, long DueMs)> _retired = new();
    private long _snapIndex, _snapTerm, _generation;

    public bool Failed { get; private set; }

    public WalLog(string walDir, bool fsync, long segmentBytes, Func<bool>? faultInjector = null, Action<TimeSpan>? onFsync = null)
    {
        _dir = Path.GetFullPath(walDir); _snapPath = Path.Combine(_dir, "snapshot.json");
        _fsync = fsync; _segmentBytes = segmentBytes; _faultInjector = faultInjector; _onFsync = onFsync;
    }

    // ------------------------------------------------------------------ lettura (solo _sync)
    public long SnapshotIndex { get { lock (_sync) return _snapIndex; } }
    public long SnapshotTerm { get { lock (_sync) return _snapTerm; } }
    public long FirstIndex { get { lock (_sync) return _snapIndex + 1; } }
    public long LastIndex { get { lock (_sync) return _snapIndex + _metas.Count; } }
    public long LastTerm { get { lock (_sync) return _metas.Count == 0 ? _snapTerm : _metas[^1].Term; } }
    public long Generation { get { lock (_sync) return _generation; } }
    public int WindowCount { get { lock (_sync) return _metas.Count; } }
    public int SegmentCount { get { lock (_sync) return _segments.Count; } }

    /// <summary>Term della voce; 0 per l'indice 0 o oltre la fine; -1 se l'indice è stato compattato (sconosciuto).</summary>
    public long TermAt(long index)
    {
        lock (_sync)
        {
            if (index == _snapIndex) return _snapTerm;
            if (index < _snapIndex) return -1;
            long k = index - _snapIndex - 1;
            return k < _metas.Count ? _metas[(int)k].Term : 0;
        }
    }

    public WalMeta? GetMeta(long index)
    {
        lock (_sync)
        {
            long k = index - _snapIndex - 1;
            return k < 0 || k >= _metas.Count ? null : _metas[(int)k];
        }
    }

    public long FirstIndexOfTerm(long term)
    {
        lock (_sync)
        {
            for (int i = 0; i < _metas.Count; i++) if (_metas[i].Term == term) return _snapIndex + i + 1;
            return term == _snapTerm && _snapIndex > 0 ? _snapIndex : 0;
        }
    }

    public long LastIndexOfTerm(long term)
    {
        lock (_sync)
        {
            for (int i = _metas.Count - 1; i >= 0; i--) if (_metas[i].Term == term) return _snapIndex + i + 1;
            return term == _snapTerm && _snapIndex > 0 ? _snapIndex : 0;
        }
    }

    public LogEntry? ReadEntry(long index)
    {
        WalMeta? m = GetMeta(index);
        if (m == null) return null;
        var payload = new byte[m.PayloadLen];
        if (m.PayloadLen > 0) ReadAt(m.Segment.H, m.PayloadOffset, payload);
        return new LogEntry { Index = index, Term = m.Term, EntryType = m.Type, RequestId = m.RequestId, PayloadHash = m.PayloadHash,
            Payload = payload, ReceivedUtc = new DateTime(m.ReceivedTicks, DateTimeKind.Utc) };
    }

    // ------------------------------------------------------------------ recovery (REQ-WAL-3)
    public WalOpenResult Open()
    {
        _write.Wait();
        try
        {
            lock (_sync)
            {
                Directory.CreateDirectory(_dir);
                bool hadData = false;
                if (File.Exists(_snapPath))
                {
                    var s = JsonSerializer.Deserialize<SnapshotMeta>(File.ReadAllBytes(_snapPath)) ?? throw new InvalidDataException("snapshot.json vuoto");
                    _snapIndex = s.Index; _snapTerm = s.Term; hadData = s.Index > 0;
                }
                var files = Directory.GetFiles(_dir, "*.wal")
                    .Select(f => (f, ok: int.TryParse(Path.GetFileNameWithoutExtension(f), out var n), n))
                    .Where(x => x.ok).OrderBy(x => x.n).ToList();
                if (files.Count == 0) { CreateSegment(1); return new WalOpenResult(false, null, 0, hadData); }

                long truncated = 0;
                for (int fi = 0; fi < files.Count; fi++)
                {
                    bool last = fi == files.Count - 1;
                    var seg = OpenSegment(files[fi].f, files[fi].n);
                    _segments.Add(seg);
                    if (seg.Length > 0) hadData = true;
                    var r = ReplaySegment(seg, last, ref truncated);
                    if (r != null) return new WalOpenResult(true, r, 0, true);
                }
                return new WalOpenResult(false, null, truncated, hadData);
            }
        }
        finally { _write.Release(); }
    }

    private string? ReplaySegment(WalSegment seg, bool isLast, ref long truncated)
    {
        long len = seg.Length, pos = 0;
        var hdr = new byte[HeaderSize];
        while (pos < len)
        {
            bool bad = false; byte type = 0; int bodyLen = 0; long idx = 0, term = 0; byte[]? body = null;
            if (len - pos < HeaderSize + CrcSize) bad = true;
            else
            {
                ReadAt(seg.H, pos, hdr);
                uint magic = BinaryPrimitives.ReadUInt32LittleEndian(hdr);
                type = hdr[5];
                bodyLen = BinaryPrimitives.ReadInt32LittleEndian(hdr.AsSpan(6));
                idx = BinaryPrimitives.ReadInt64LittleEndian(hdr.AsSpan(10));
                term = BinaryPrimitives.ReadInt64LittleEndian(hdr.AsSpan(18));
                if (magic != Magic || hdr[4] != Version || bodyLen < 0 || bodyLen > MaxRecordBody || pos + HeaderSize + bodyLen + CrcSize > len) bad = true;
                else
                {
                    body = new byte[bodyLen + CrcSize];
                    ReadAt(seg.H, pos + HeaderSize, body);
                    if (ComputeCrc(hdr, body.AsSpan(0, bodyLen)) != BinaryPrimitives.ReadUInt32LittleEndian(body.AsSpan(bodyLen))) bad = true;
                }
            }

            if (bad)
            {
                // Solo la coda dell'ultimo segmento può essere incompleta (crash durante la scrittura).
                if (!isLast || HasValidRecordAfter(seg, pos + 1, len))
                    return $"WAL: record corrotto non finale in {Path.GetFileName(seg.Path)} a offset {pos}; richiesta recovery esplicita (REQ-WAL-3).";
                seg.Fs.SetLength(pos); FlushSegment(seg); seg.Length = pos;
                truncated += len - pos;
                return null;
            }

            seg.MaxIndex = Math.Max(seg.MaxIndex, idx);
            if (type == RecEntry)
            {
                if (idx > _snapIndex)
                {
                    if (idx != _snapIndex + _metas.Count + 1)
                        return $"WAL: indice non contiguo ({idx}, atteso {_snapIndex + _metas.Count + 1}) in {Path.GetFileName(seg.Path)} a offset {pos}.";
                    var m = ParseMeta(body!, bodyLen, pos + HeaderSize, seg);
                    if (m == null) return $"WAL: corpo voce non valido in {Path.GetFileName(seg.Path)} a offset {pos}.";
                    m.Term = term; _metas.Add(m);
                }
            }
            else if (type == RecTruncate)
            {
                if (idx < 1 || idx > _snapIndex + _metas.Count + 1) return $"WAL: truncate non valido ({idx}) a offset {pos}.";
                long keep = Math.Max(0, idx - 1 - _snapIndex);
                if (keep < _metas.Count) _metas.RemoveRange((int)keep, _metas.Count - (int)keep);
            }
            else return $"WAL: tipo record sconosciuto in {Path.GetFileName(seg.Path)} a offset {pos}.";
            pos += HeaderSize + bodyLen + CrcSize;
        }
        return null;
    }

    private bool HasValidRecordAfter(WalSegment seg, long from, long len)
    {
        var magicBytes = new byte[4]; BinaryPrimitives.WriteUInt32LittleEndian(magicBytes, Magic);
        const int Chunk = 1 << 20;
        var buf = new byte[Chunk + 3];
        for (long off = from; off < len; off += Chunk)
        {
            int n = ReadAt(seg.H, off, buf.AsSpan(0, (int)Math.Min(buf.Length, len - off)));
            for (int i = 0; i + 4 <= n; i++)
            {
                if (buf[i] != magicBytes[0] || buf[i + 1] != magicBytes[1] || buf[i + 2] != magicBytes[2] || buf[i + 3] != magicBytes[3]) continue;
                if (VerifyRecordAt(seg, off + i, len)) return true;
            }
        }
        return false;
    }

    private bool VerifyRecordAt(WalSegment seg, long pos, long len)
    {
        if (len - pos < HeaderSize + CrcSize) return false;
        var hdr = new byte[HeaderSize]; ReadAt(seg.H, pos, hdr);
        int bodyLen = BinaryPrimitives.ReadInt32LittleEndian(hdr.AsSpan(6));
        if (hdr[4] != Version || bodyLen < 0 || bodyLen > MaxRecordBody || pos + HeaderSize + bodyLen + CrcSize > len) return false;
        var rest = new byte[bodyLen + CrcSize]; ReadAt(seg.H, pos + HeaderSize, rest);
        return ComputeCrc(hdr, rest.AsSpan(0, bodyLen)) == BinaryPrimitives.ReadUInt32LittleEndian(rest.AsSpan(bodyLen));
    }

    private static uint ComputeCrc(byte[] hdr, ReadOnlySpan<byte> body)
    {
        var all = new byte[HeaderSize - 4 + body.Length];
        Array.Copy(hdr, 4, all, 0, HeaderSize - 4);
        body.CopyTo(all.AsSpan(HeaderSize - 4));
        return Crc32C.Compute(all);
    }

    private static WalMeta? ParseMeta(byte[] body, int bodyLen, long bodyOffset, WalSegment seg)
    {
        try
        {
            int p = 0; var type = (LogEntryType)body[p++];
            long ticks = BinaryPrimitives.ReadInt64LittleEndian(body.AsSpan(p)); p += 8;
            int ridLen = BinaryPrimitives.ReadUInt16LittleEndian(body.AsSpan(p)); p += 2;
            var rid = Encoding.UTF8.GetString(body, p, ridLen); p += ridLen;
            int hl = body[p++]; var hash = Encoding.UTF8.GetString(body, p, hl); p += hl;
            if (p > bodyLen) return null;
            return new WalMeta { Type = type, RequestId = rid, PayloadHash = hash, ReceivedTicks = ticks, Segment = seg, PayloadOffset = bodyOffset + p, PayloadLen = bodyLen - p };
        }
        catch { return null; }
    }

    // ------------------------------------------------------------------ scrittura
    /// <summary>Acquisisce il lock di scrittura per una sequenza "leggi stato del log, verifica, appendi" atomica rispetto agli altri scrittori.</summary>
    public IDisposable AcquireWrite()
    {
        _write.Wait();
        return new Releaser(_write);
    }

    private sealed class Releaser : IDisposable
    {
        private SemaphoreSlim? _s;
        public Releaser(SemaphoreSlim s) => _s = s;
        public void Dispose() => Interlocked.Exchange(ref _s, null)?.Release();
    }

    /// <summary>Tronca opzionalmente da truncateFrom (&gt;0) e appende con UN solo fsync. Ritorna solo a dati durevoli.</summary>
    public void Append(long truncateFrom, IReadOnlyList<WalInput> entries)
    {
        using (AcquireWrite()) AppendLocked(truncateFrom, entries);
    }

    /// <summary>Group commit del Leader: appende solo se nessun altro scrittore ha modificato il log da expectedGeneration.</summary>
    public bool TryAppend(long expectedGeneration, IReadOnlyList<WalInput> entries)
    {
        using (AcquireWrite())
        {
            if (Generation != expectedGeneration) return false;
            AppendLocked(0, entries);
            return true;
        }
    }

    /// <summary>Richiede il lock di scrittura (<see cref="AcquireWrite"/>).</summary>
    public void AppendLocked(long truncateFrom, IReadOnlyList<WalInput> entries)
    {
        long snap, count;
        lock (_sync)
        {
            if (Failed || _segments.Count == 0) throw new WalIoException("WAL non utilizzabile (errore di I/O precedente).");
            snap = _snapIndex; count = _metas.Count;
        }
        if (_faultInjector?.Invoke() == true) { Failed = true; throw new WalIoException("Errore di I/O iniettato."); }
        if (truncateFrom > 0 && (truncateFrom <= snap || truncateFrom > snap + count + 1))
            throw new InvalidOperationException($"truncateFrom {truncateFrom} fuori dalla finestra non compattata ({snap + 1}..{snap + count + 1}).");
        long expected = truncateFrom > 0 ? truncateFrom : snap + count + 1;
        for (int i = 0; i < entries.Count; i++)
            if (entries[i].Index != expected + i) throw new InvalidOperationException($"Indice non contiguo: {entries[i].Index}, atteso {expected + i}.");
        foreach (var e in entries)
        {
            if (Encoding.UTF8.GetByteCount(e.RequestId) > ushort.MaxValue || Encoding.UTF8.GetByteCount(e.PayloadHash) > byte.MaxValue)
                throw new InvalidOperationException("RequestId o PayloadHash troppo lunghi.");
            if (e.Payload.Length > ClusterConfig.MaxBodyBytesLimit) throw new InvalidOperationException("Voce oltre il limite di formato del WAL.");
        }
        if (truncateFrom > snap + count) truncateFrom = 0; // troncamento oltre la fine: nulla da eliminare
        if (truncateFrom == 0 && entries.Count == 0) return;

        var newMetas = new List<WalMeta>(entries.Count);
        WalSegment seg;
        long off;
        try
        {
            seg = _segments[^1];
            if (seg.Length >= _segmentBytes) seg = CreateSegment(seg.Seq + 1);
            off = seg.Length;
            if (truncateFrom > 0) off += WriteRecord(seg, off, RecTruncate, truncateFrom, 0, ReadOnlySpan<byte>.Empty);
            long maxIdx = truncateFrom;
            foreach (var e in entries)
            {
                var rid = Encoding.UTF8.GetBytes(e.RequestId); var hash = Encoding.UTF8.GetBytes(e.PayloadHash);
                if (rid.Length > ushort.MaxValue || hash.Length > byte.MaxValue) throw new InvalidOperationException("RequestId o PayloadHash troppo lunghi.");
                int prefix = 1 + 8 + 2 + rid.Length + 1 + hash.Length;
                int bodyLen = prefix + e.Payload.Length;
                if (bodyLen > MaxRecordBody) throw new InvalidOperationException("Voce oltre il limite di formato del WAL.");
                var body = ArrayPool<byte>.Shared.Rent(bodyLen);
                try
                {
                    int p = 0; body[p++] = (byte)e.Type;
                    BinaryPrimitives.WriteInt64LittleEndian(body.AsSpan(p), e.ReceivedTicks); p += 8;
                    BinaryPrimitives.WriteUInt16LittleEndian(body.AsSpan(p), (ushort)rid.Length); p += 2;
                    rid.CopyTo(body, p); p += rid.Length;
                    body[p++] = (byte)hash.Length; hash.CopyTo(body, p); p += hash.Length;
                    e.Payload.Span.CopyTo(body.AsSpan(p));
                    long start = off;
                    off += WriteRecord(seg, off, RecEntry, e.Index, e.Term, body.AsSpan(0, bodyLen));
                    newMetas.Add(new WalMeta { Term = e.Term, Type = e.Type, RequestId = e.RequestId, PayloadHash = e.PayloadHash,
                        ReceivedTicks = e.ReceivedTicks, Segment = seg, PayloadOffset = start + HeaderSize + prefix, PayloadLen = e.Payload.Length });
                    maxIdx = Math.Max(maxIdx, e.Index);
                }
                finally { ArrayPool<byte>.Shared.Return(body); }
            }
            if (_fsync)
            {
                var sw = Stopwatch.StartNew();
                FlushSegment(seg);
                _onFsync?.Invoke(sw.Elapsed);
            }
            lock (_sync)
            {
                seg.Length = off;
                seg.MaxIndex = Math.Max(seg.MaxIndex, maxIdx);
                if (truncateFrom > 0)
                {
                    long keep = truncateFrom - 1 - _snapIndex;
                    _metas.RemoveRange((int)keep, _metas.Count - (int)keep);
                }
                _metas.AddRange(newMetas);
                _generation++;
            }
        }
        catch (Exception ex) when (ex is not InvalidOperationException)
        {
            Failed = true; // lo stato su disco è incerto: il nodo diventa unhealthy (REQ-WAL-5)
            throw new WalIoException("Errore di scrittura WAL: " + ex.Message, ex);
        }
    }

    private int WriteRecord(WalSegment seg, long offset, byte type, long index, long term, ReadOnlySpan<byte> body)
    {
        int total = HeaderSize + body.Length + CrcSize;
        var buf = ArrayPool<byte>.Shared.Rent(total);
        try
        {
            BinaryPrimitives.WriteUInt32LittleEndian(buf, Magic); buf[4] = Version; buf[5] = type;
            BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(6), body.Length);
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(10), index);
            BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(18), term);
            body.CopyTo(buf.AsSpan(HeaderSize));
            uint crc = Crc32C.Compute(buf.AsSpan(4, HeaderSize - 4 + body.Length));
            BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(HeaderSize + body.Length), crc);
            RandomAccess.Write(seg.H, buf.AsSpan(0, total), offset);
            return total;
        }
        finally { ArrayPool<byte>.Shared.Return(buf); }
    }

    // ------------------------------------------------------------------ compattazione e snapshot
    /// <summary>
    /// Scarta dal log le voci fino a upTo (incluso), già committed e applicate. Scrive prima snapshot.json in modo
    /// durevole, poi elimina i segmenti interamente coperti. Ritorna il numero di voci rimosse dalla RAM.
    /// </summary>
    public int Compact(long upTo)
    {
        using (AcquireWrite())
        {
            long term;
            lock (_sync)
            {
                if (upTo <= _snapIndex || upTo > _snapIndex + _metas.Count) return 0;
                term = _metas[(int)(upTo - _snapIndex - 1)].Term;
            }
            WriteSnapshotMeta(upTo, term);
            int removed;
            lock (_sync)
            {
                removed = (int)(upTo - _snapIndex);
                _metas.RemoveRange(0, removed);
                _snapIndex = upTo; _snapTerm = term;
            }
            DeleteCoveredSegments(upTo);
            return removed;
        }
    }

    /// <summary>
    /// InstallSnapshot sul Follower con log non coerente: scarta TUTTO il log e riparte da (index, term).
    /// Sequenza crash-safe: nuovo segmento con record Truncate durevole, snapshot.json durevole, eliminazione dei vecchi segmenti.
    /// </summary>
    public void ResetToSnapshot(long index, long term)
    {
        using (AcquireWrite())
        {
            long snap;
            lock (_sync) { if (Failed) throw new WalIoException("WAL non utilizzabile."); snap = _snapIndex; }
            try
            {
                var seg = CreateSegment(_segments[^1].Seq + 1);
                long off = WriteRecord(seg, 0, RecTruncate, snap + 1, 0, ReadOnlySpan<byte>.Empty);
                FlushSegment(seg);
                WriteSnapshotMeta(index, term);
                lock (_sync)
                {
                    seg.Length = off; seg.MaxIndex = snap + 1;
                    _metas.Clear(); _snapIndex = index; _snapTerm = term; _generation++;
                }
                DeleteCoveredSegments(long.MaxValue);
            }
            catch (Exception ex)
            {
                Failed = true;
                throw new WalIoException("Errore durante InstallSnapshot: " + ex.Message, ex);
            }
        }
    }

    private void WriteSnapshotMeta(long index, long term) =>
        Durability.WriteAllBytesDurable(_snapPath, JsonSerializer.SerializeToUtf8Bytes(new SnapshotMeta { Index = index, Term = term }));

    /// <summary>Elimina i segmenti iniziali (mai quello attivo) i cui record citano solo indici &lt;= upTo.</summary>
    private void DeleteCoveredSegments(long upTo)
    {
        var toDelete = new List<WalSegment>();
        lock (_sync)
        {
            while (_segments.Count > 1 && _segments[0].MaxIndex <= upTo)
            {
                toDelete.Add(_segments[0]);
                _segments.RemoveAt(0);
            }
            long now = Environment.TickCount64;
            foreach (var s in toDelete) _retired.Add((s, now + 30_000));
            // chiusura differita: un lettore potrebbe avere appena letto un meta del segmento
            for (int i = _retired.Count - 1; i >= 0; i--)
                if (_retired[i].DueMs <= now) { _retired[i].Seg.Fs.Dispose(); _retired.RemoveAt(i); }
        }
        foreach (var s in toDelete) { try { File.Delete(s.Path); } catch (IOException) { /* ritentato alla prossima compattazione non necessario: file orfano innocuo */ } }
        if (toDelete.Count > 0) Durability.SyncDirectory(_dir);
    }

    // ------------------------------------------------------------------ segmenti
    private WalSegment CreateSegment(int seq)
    {
        var path = Path.Combine(_dir, seq.ToString("D8") + ".wal");
        var seg = OpenSegment(path, seq);
        FlushSegment(seg);
        Durability.SyncDirectory(_dir);
        lock (_sync) _segments.Add(seg);
        return seg;
    }

    private static WalSegment OpenSegment(string path, int seq)
    {
        // FileShare.Delete: su Windows consente l'eliminazione di un segmento compattato ancora aperto da un lettore
        var fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete, 1, FileOptions.None);
        return new WalSegment { Seq = seq, Path = path, Fs = fs, H = fs.SafeFileHandle, Length = fs.Length };
    }

    private static void FlushSegment(WalSegment seg) => Durability.FlushFile(seg.Fs);

    private static int ReadAt(SafeFileHandle h, long offset, Span<byte> dst)
    {
        int total = 0;
        while (total < dst.Length)
        {
            int n = RandomAccess.Read(h, dst.Slice(total), offset + total);
            if (n <= 0) break;
            total += n;
        }
        return total;
    }

    public void Dispose()
    {
        lock (_sync)
        {
            foreach (var s in _segments) s.Fs.Dispose();
            foreach (var r in _retired) r.Seg.Fs.Dispose();
            _segments.Clear(); _retired.Clear();
        }
    }

    private sealed class SnapshotMeta { public long Index { get; set; } public long Term { get; set; } }
}
