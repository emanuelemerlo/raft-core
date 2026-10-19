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
    /// <summary>Assegnato dal Leader. Usato per la scadenza delle voci obsolete e mai per il consenso.</summary>
    public DateTime ReceivedUtc { get; set; }
}

/// <summary>Stato durevole: term, voto, membri. commitIndex è in un file separato non sincronizzato (limite inferiore).</summary>
public class PersistentRaftState
{
    public long CurrentTerm { get; set; }
    public string? VotedFor { get; set; }
    public List<string> Members { get; set; } = new();
    public string? NodeId { get; set; }
}

/// <summary>Esito dell'applicazione di una voce al database condiviso.</summary>
public enum ApplyStatus
{
    Applied = 1,
    Duplicate = 2,
    Conflict = 3,
    Rejected = 4,
    Expired = 5,
    Quarantined = 6,
    NoOp = 7
}

/// <summary>Esito persistito e deterministico: ogni replay restituisce gli stessi status e body.</summary>
public sealed record ApplyOutcome(ApplyStatus Status, int HttpStatus, byte[] Body, long LogIndex, bool Replay = false)
{
    public ApplyOutcome AsReplay() => this with { Replay = true };
}

/// <summary>Contesto dell'apply: orologio dell'applicatore e soglia di obsolescenza.</summary>
public sealed record ApplyContext(DateTime NowUtc, TimeSpan MaxPendingAge, string ClusterId);
