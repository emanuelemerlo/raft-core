using System.Text.Json;
using RaftCore.Models;
using RaftCore.Services.Common;

namespace RaftCore.Services.Storage;

/// <summary>currentTerm / votedFor / commitIndex / members, scritti in modo atomico e crash-safe (REQ-ID-5).</summary>
public sealed class RaftStateStore
{
    private readonly string _path;
    private readonly string _dir;
    public RaftStateStore(string dataDir)
    {
        _dir = Path.GetFullPath(dataDir); Directory.CreateDirectory(_dir);
        _path = Path.Combine(_dir, "raft_state.json");
    }
    public bool Exists => File.Exists(_path);
    public PersistentRaftState Load() =>
        JsonSerializer.Deserialize<PersistentRaftState>(File.ReadAllText(_path)) ?? throw new InvalidDataException("raft_state.json vuoto");

    /// <param name="durable">true: fsync del file e della directory (term/voto). false: aggiornamento di solo commitIndex.</param>
    public void Save(PersistentRaftState s, bool durable)
    {
        var tmp = _path + ".tmp";
        using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            JsonSerializer.Serialize(fs, s);
            if (durable) fs.Flush(true);
        }
        File.Move(tmp, _path, true);
        if (durable) DirSync.Sync(_dir);
    }
}
