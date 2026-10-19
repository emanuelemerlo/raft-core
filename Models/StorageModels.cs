using System.Text.Json.Serialization;

namespace RaftCore.Models;

/// <summary>Voce del log Raft (spec §9).</summary>
public class LogEntry
{
    public long Index { get; set; }
    public long Term { get; set; }
    public LogEntryType EntryType { get; set; }
    public string RequestId { get; set; } = string.Empty;
    public string PayloadHash { get; set; } = string.Empty; // SHA-256 (hex) dei byte grezzi del body
    public byte[] Payload { get; set; } = Array.Empty<byte>();
    /// <summary>Assegnato dal Leader; usato SOLO per la retention dell'idempotenza (mai per il consenso).</summary>
    public DateTime ReceivedUtc { get; set; }

    /// <summary>Non replicato: indice della prima voce con stesso RequestId/PayloadHash (REQ-IDM-4), 0 se nessuna.</summary>
    [JsonIgnore] public long DuplicateOf { get; set; }
    /// <summary>Non replicato: stesso RequestId con PayloadHash diverso (race tra Leader): nessun effetto.</summary>
    [JsonIgnore] public bool IdConflict { get; set; }
}

public class PersistentRaftState
{
    public long CurrentTerm { get; set; }
    public string? VotedFor { get; set; }
    /// <summary>Limite inferiore del commitIndex (REQ-ID-4).</summary>
    public long CommitIndex { get; set; }
    public List<string> Members { get; set; } = new();
    public string? NodeId { get; set; }
}
