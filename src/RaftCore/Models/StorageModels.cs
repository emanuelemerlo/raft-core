using System.Security.Cryptography;
using System.Text;

namespace RaftCore.Models;

/// <summary>Hash di idempotenza (REQ-IDM-8, REQ-EPT-3): copre l'endpoint, così lo stesso RequestId su due endpoint è un conflitto.</summary>
public static class PayloadHashing
{
    public static string Compute(string payloadType, ReadOnlySpan<byte> body)
    {
        using var h = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        h.AppendData(Encoding.UTF8.GetBytes(payloadType));
        h.AppendData(stackalloc byte[] { 0 });
        h.AppendData(body);
        return Convert.ToHexString(h.GetHashAndReset()).ToLowerInvariant();
    }
}

/// <summary>Voce del log Raft (spec §9).</summary>
public class LogEntry
{
    public long Index { get; set; }
    public long Term { get; set; }
    public LogEntryType EntryType { get; set; }
    public string RequestId { get; set; } = string.Empty;
    /// <summary>Nome dell'endpoint che ha ricevuto la richiesta (EndpointConfig.Name); vuoto per le voci NoOp. Sceglie il writer all'apply.</summary>
    public string PayloadType { get; set; } = string.Empty;
    /// <summary>SHA-256 (hex) di PayloadType, un byte 0 e i byte grezzi del body (<see cref="PayloadHashing"/>).</summary>
    public string PayloadHash { get; set; } = string.Empty;
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
