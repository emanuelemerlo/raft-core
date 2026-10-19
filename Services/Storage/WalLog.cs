using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Microsoft.Win32.SafeHandles;
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
    public long PayloadOffset;
    public int PayloadLen;
    // derivati dal log (REQ-IDM-4), assegnati dal nodo
    public long DupOf;
    public bool Conflict;
}

public readonly record struct WalInput(long Index, long Term, LogEntryType Type, string RequestId, string PayloadHash, long ReceivedTicks, ReadOnlyMemory<byte> Payload);

public sealed record WalOpenResult(bool CorruptNonFinal, string? Message, long TruncatedBytes);

public sealed class WalIoException : IOException { public WalIoException(string m, Exception? inner = null) : base(m, inner) { } }

/// <summary>
/// WAL append-only (REQ-WAL-1..3). Record: magic(4) ver(1) tipo(1) lunghezza(4) index(8) term(8) corpo(n) crc32c(4).
/// Il troncamento di voci in conflitto è un record "Truncate" appeso: il file non viene mai riscritto.
/// </summary>
public sealed class WalLog : IDisposable
{
    private const uint Magic = 0x4C415752;
    private const byte Version = 1;
    private const byte RecEntry = 1, RecTruncate = 2;
    private const int HeaderSize = 26, CrcSize = 4;

    private readonly string _path;
    private readonly bool _fsync;
    private readonly int _maxBody;
    private readonly Func<bool>? _faultInjector;
    private readonly object _sync = new();
    private readonly List<WalMeta> _metas = new();
    private FileStream? _fs;
    private SafeFileHandle? _h;
    private long _end;

    public bool Failed { get; private set; }

    public WalLog(string path, bool fsync, long maxBodyBytes, Func<bool>? faultInjector = null)
    {
        _path = path; _fsync = fsync; _maxBody = (int)Math.Min(int.MaxValue / 2, maxBodyBytes + 4096); _faultInjector = faultInjector;
    }

    public long LastIndex { get { lock (_sync) return _metas.Count; } }
    public long LastTerm { get { lock (_sync) return _metas.Count == 0 ? 0 : _metas[^1].Term; } }
    public long TermAt(long index) { lock (_sync) return index < 1 || index > _metas.Count ? 0 : _metas[(int)index - 1].Term; }
    public WalMeta? GetMeta(long index) { lock (_sync) return index < 1 || index > _metas.Count ? null : _metas[(int)index - 1]; }
    public long FirstIndexOfTerm(long term) { lock (_sync) { for (int i = 0; i < _metas.Count; i++) if (_metas[i].Term == term) return i + 1; return 0; } }
    public long LastIndexOfTerm(long term) { lock (_sync) { for (int i = _metas.Count - 1; i >= 0; i--) if (_metas[i].Term == term) return i + 1; return 0; } }
    public long FileLength { get { lock (_sync) return _end; } }

    // ------------------------------------------------------------------ recovery (REQ-WAL-3)
    public WalOpenResult Open()
    {
        lock (_sync)
        {
            bool existed = File.Exists(_path);
            Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(_path))!);
            _fs = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite, 1, FileOptions.None);
            _h = _fs.SafeFileHandle;
            if (!existed) { _fs.Flush(true); DirSync.Sync(Path.GetDirectoryName(Path.GetFullPath(_path))); }

            long len = _fs.Length, pos = 0;
            var hdr = new byte[HeaderSize];
            while (pos < len)
            {
                bool bad = false; byte type = 0; int bodyLen = 0; long idx = 0, term = 0; byte[]? body = null;
                if (len - pos < HeaderSize + CrcSize) bad = true;
                else
                {
                    ReadAt(pos, hdr);
                    uint magic = BinaryPrimitives.ReadUInt32LittleEndian(hdr);
                    type = hdr[5];
                    bodyLen = BinaryPrimitives.ReadInt32LittleEndian(hdr.AsSpan(6));
                    idx = BinaryPrimitives.ReadInt64LittleEndian(hdr.AsSpan(10));
                    term = BinaryPrimitives.ReadInt64LittleEndian(hdr.AsSpan(18));
                    if (magic != Magic || hdr[4] != Version || bodyLen < 0 || bodyLen > _maxBody || pos + HeaderSize + bodyLen + CrcSize > len) bad = true;
                    else
                    {
                        body = new byte[bodyLen + CrcSize];
                        ReadAt(pos + HeaderSize, body);
                        var all = new byte[HeaderSize - 4 + bodyLen];
                        Array.Copy(hdr, 4, all, 0, HeaderSize - 4);
                        Array.Copy(body, 0, all, HeaderSize - 4, bodyLen);
                        if (Crc32C.Compute(all) != BinaryPrimitives.ReadUInt32LittleEndian(body.AsSpan(bodyLen))) bad = true;
                    }
                }

                if (bad)
                {
                    if (HasValidRecordAfter(pos + 1, len))
                        return new WalOpenResult(true, $"WAL: record corrotto non finale a offset {pos}; richiesta recovery esplicita (REQ-WAL-3).", 0);
                    _fs.SetLength(pos); _fs.Flush(true); _end = pos;
                    return Finish(len - pos);
                }

                if (type == RecEntry)
                {
                    if (idx != _metas.Count + 1)
                        return new WalOpenResult(true, $"WAL: indice non contiguo ({idx}, atteso {_metas.Count + 1}) a offset {pos}.", 0);
                    var m = ParseMeta(body!, bodyLen, pos + HeaderSize);
                    if (m == null) return new WalOpenResult(true, $"WAL: corpo voce non valido a offset {pos}.", 0);
                    m.Term = term; _metas.Add(m);
                }
                else if (type == RecTruncate)
                {
                    if (idx < 1 || idx > _metas.Count + 1) return new WalOpenResult(true, $"WAL: truncate non valido ({idx}) a offset {pos}.", 0);
                    _metas.RemoveRange((int)idx - 1, _metas.Count - ((int)idx - 1));
                }
                else return new WalOpenResult(true, $"WAL: tipo record sconosciuto a offset {pos}.", 0);
                pos += HeaderSize + bodyLen + CrcSize;
            }
            _end = pos;
            return Finish(0);
        }
        WalOpenResult Finish(long truncated) => new(false, null, truncated);
    }

    private bool HasValidRecordAfter(long from, long len)
    {
        var magicBytes = new byte[4]; BinaryPrimitives.WriteUInt32LittleEndian(magicBytes, Magic);
        const int Chunk = 1 << 20;
        var buf = new byte[Chunk + 3];
        for (long off = from; off < len; off += Chunk)
        {
            int n = ReadAt(off, buf.AsSpan(0, (int)Math.Min(buf.Length, len - off)));
            for (int i = 0; i + 4 <= n; i++)
            {
                if (buf[i] != magicBytes[0] || buf[i + 1] != magicBytes[1] || buf[i + 2] != magicBytes[2] || buf[i + 3] != magicBytes[3]) continue;
                if (VerifyRecordAt(off + i, len)) return true;
            }
        }
        return false;
    }

    private bool VerifyRecordAt(long pos, long len)
    {
        if (len - pos < HeaderSize + CrcSize) return false;
        var hdr = new byte[HeaderSize]; ReadAt(pos, hdr);
        int bodyLen = BinaryPrimitives.ReadInt32LittleEndian(hdr.AsSpan(6));
        if (hdr[4] != Version || bodyLen < 0 || bodyLen > _maxBody || pos + HeaderSize + bodyLen + CrcSize > len) return false;
        var rest = new byte[bodyLen + CrcSize]; ReadAt(pos + HeaderSize, rest);
        var all = new byte[HeaderSize - 4 + bodyLen];
        Array.Copy(hdr, 4, all, 0, HeaderSize - 4); Array.Copy(rest, 0, all, HeaderSize - 4, bodyLen);
        return Crc32C.Compute(all) == BinaryPrimitives.ReadUInt32LittleEndian(rest.AsSpan(bodyLen));
    }

    private static WalMeta? ParseMeta(byte[] body, int bodyLen, long bodyOffset)
    {
        try
        {
            int p = 0; var type = (LogEntryType)body[p++];
            long ticks = BinaryPrimitives.ReadInt64LittleEndian(body.AsSpan(p)); p += 8;
            int ridLen = BinaryPrimitives.ReadUInt16LittleEndian(body.AsSpan(p)); p += 2;
            var rid = Encoding.UTF8.GetString(body, p, ridLen); p += ridLen;
            int hl = body[p++]; var hash = Encoding.UTF8.GetString(body, p, hl); p += hl;
            if (p > bodyLen) return null;
            return new WalMeta { Type = type, RequestId = rid, PayloadHash = hash, ReceivedTicks = ticks, PayloadOffset = bodyOffset + p, PayloadLen = bodyLen - p };
        }
        catch { return null; }
    }

    // ------------------------------------------------------------------ scrittura
    /// <summary>
    /// Opzionalmente tronca dal truncateFrom (&gt;0) e appende le voci, con UN solo fsync (group commit):
    /// ritorna solo a dati durevoli (REQ-WAL-2, REQ-ID-5).
    /// </summary>
    public void Append(long truncateFrom, IReadOnlyList<WalInput> entries)
    {
        lock (_sync)
        {
            if (Failed || _fs == null) throw new WalIoException("WAL non utilizzabile (errore di I/O precedente).");
            if (_faultInjector?.Invoke() == true) { Failed = true; throw new WalIoException("Errore di I/O iniettato."); }
            long expected = truncateFrom > 0 ? truncateFrom : _metas.Count + 1;
            if (truncateFrom > 0 && (truncateFrom < 1 || truncateFrom > _metas.Count + 1)) throw new InvalidOperationException("truncateFrom fuori range.");
            for (int i = 0; i < entries.Count; i++)
                if (entries[i].Index != expected + i) throw new InvalidOperationException($"Indice non contiguo: {entries[i].Index}, atteso {expected + i}.");

            var newMetas = new List<WalMeta>(entries.Count);
            long off = _end;
            try
            {
                if (truncateFrom > 0 && truncateFrom <= _metas.Count)
                    off += WriteRecord(off, RecTruncate, truncateFrom, 0, ReadOnlySpan<byte>.Empty, default, out _);
                else truncateFrom = 0;
                foreach (var e in entries)
                {
                    var rid = Encoding.UTF8.GetBytes(e.RequestId); var hash = Encoding.UTF8.GetBytes(e.PayloadHash);
                    int prefix = 1 + 8 + 2 + rid.Length + 1 + hash.Length;
                    int bodyLen = prefix + e.Payload.Length;
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
                        off += WriteRecord(off, RecEntry, e.Index, e.Term, body.AsSpan(0, bodyLen), default, out _);
                        newMetas.Add(new WalMeta { Term = e.Term, Type = e.Type, RequestId = e.RequestId, PayloadHash = e.PayloadHash,
                            ReceivedTicks = e.ReceivedTicks, PayloadOffset = start + HeaderSize + prefix, PayloadLen = e.Payload.Length });
                    }
                    finally { ArrayPool<byte>.Shared.Return(body); }
                }
                if (_fsync) _fs.Flush(true);
            }
            catch (Exception ex)
            {
                Failed = true; // lo stato su disco è incerto: il nodo diventa unhealthy (REQ-WAL-5)
                throw new WalIoException("Errore di scrittura WAL: " + ex.Message, ex);
            }
            _end = off;
            if (truncateFrom > 0) _metas.RemoveRange((int)truncateFrom - 1, _metas.Count - ((int)truncateFrom - 1));
            _metas.AddRange(newMetas);
        }
    }

    private int WriteRecord(long offset, byte type, long index, long term, ReadOnlySpan<byte> body, object? _, out uint crc)
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
            crc = Crc32C.Compute(buf.AsSpan(4, HeaderSize - 4 + body.Length));
            BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(HeaderSize + body.Length), crc);
            RandomAccess.Write(_h!, buf.AsSpan(0, total), offset);
            return total;
        }
        finally { ArrayPool<byte>.Shared.Return(buf); }
    }

    public LogEntry? ReadEntry(long index)
    {
        WalMeta? m; lock (_sync) m = index < 1 || index > _metas.Count ? null : _metas[(int)index - 1];
        if (m == null) return null;
        var payload = new byte[m.PayloadLen];
        if (m.PayloadLen > 0) ReadAt(m.PayloadOffset, payload);
        return new LogEntry { Index = index, Term = m.Term, EntryType = m.Type, RequestId = m.RequestId, PayloadHash = m.PayloadHash,
            Payload = payload, ReceivedUtc = new DateTime(m.ReceivedTicks, DateTimeKind.Utc), DuplicateOf = m.DupOf, IdConflict = m.Conflict };
    }

    private int ReadAt(long offset, Span<byte> dst)
    {
        int total = 0;
        while (total < dst.Length)
        {
            int n = RandomAccess.Read(_h!, dst.Slice(total), offset + total);
            if (n <= 0) break;
            total += n;
        }
        return total;
    }

    public void Dispose() { lock (_sync) { _fs?.Dispose(); _fs = null; } }
}
