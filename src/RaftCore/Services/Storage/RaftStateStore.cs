using System.Buffers.Binary;
using System.Text.Json;
using RaftCore.Models;
using RaftCore.Services.Common;

namespace RaftCore.Services.Storage;

/// <summary>currentTerm / votedFor / members: scrittura sempre atomica e durevole (REQ-ID-5).</summary>
public sealed class RaftStateStore
{
    private readonly string _path;
    public RaftStateStore(string dataDir)
    {
        var dir = Path.GetFullPath(dataDir); Directory.CreateDirectory(dir);
        _path = Path.Combine(dir, "raft_state.json");
    }
    public bool Exists => File.Exists(_path);
    public PersistentRaftState Load() =>
        JsonSerializer.Deserialize<PersistentRaftState>(File.ReadAllBytes(_path)) ?? throw new InvalidDataException("raft_state.json vuoto");

    public void Save(PersistentRaftState s) => Durability.WriteAllBytesDurable(_path, JsonSerializer.SerializeToUtf8Bytes(s));
}

/// <summary>
/// commitIndex come limite inferiore (REQ-ID-4): 12 byte (valore + CRC32C) riscritti sul posto senza fsync.
/// Un file danneggiato da un crash vale 0: il valore reale viene riappreso dal Leader.
/// Separato da raft_state.json, così un crash non può mai rendere illeggibili term e voto.
/// </summary>
public sealed class CommitIndexFile : IDisposable
{
    private readonly FileStream _fs;
    public CommitIndexFile(string dataDir)
    {
        _fs = new FileStream(Path.Combine(Path.GetFullPath(dataDir), "commit.idx"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
    }

    public long Read()
    {
        if (_fs.Length < 12) return 0;
        Span<byte> b = stackalloc byte[12];
        RandomAccess.Read(_fs.SafeFileHandle, b, 0);
        long v = BinaryPrimitives.ReadInt64LittleEndian(b);
        return Crc32C.Compute(b[..8]) == BinaryPrimitives.ReadUInt32LittleEndian(b[8..]) && v >= 0 ? v : 0;
    }

    public void Write(long value)
    {
        Span<byte> b = stackalloc byte[12];
        BinaryPrimitives.WriteInt64LittleEndian(b, value);
        BinaryPrimitives.WriteUInt32LittleEndian(b[8..], Crc32C.Compute(b[..8]));
        RandomAccess.Write(_fs.SafeFileHandle, b, 0);
    }

    public void Dispose() => _fs.Dispose();
}
