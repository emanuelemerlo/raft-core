using System.Text;
using System.Text.Json;

namespace RaftCore.Simulator.Services;

/// <summary>
/// Confronta ciò che il simulatore ha inviato con ciò che il cluster ha scritto nella state machine SharedFile
/// (applied.log): nessun duplicato, nessuna operazione confermata con 200 e poi mancante.
/// Formato riga: LogIndex \t Status \t RequestId \t PayloadHash \t HttpStatus \t Base64(risposta) \t Base64(payload)
/// </summary>
public sealed class AppliedLogVerifier
{
    private const int Applied = 1, Duplicate = 2, Conflict = 3, Rejected = 4, Expired = 5, Quarantined = 6, NoOp = 7;

    private sealed record Row(long Index, int Status, string RequestId, long? Seq, string? RunId);

    public string FilePath { get; }

    public AppliedLogVerifier(SimulatorOptions opt, PathResolver paths) => FilePath = paths.Resolve(opt.AppliedLogPath);

    public bool Available => File.Exists(FilePath);

    private async Task<List<Row>> ReadAsync(CancellationToken ct)
    {
        await using var fs = new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        using var reader = new StreamReader(fs, Encoding.ASCII);
        var text = await reader.ReadToEndAsync(ct);
        int last = text.LastIndexOf('\n');                             // una riga finale incompleta è in scrittura: si ignora
        var rows = new List<Row>();
        if (last < 0) return rows;
        foreach (var line in text[..last].Split('\n'))
        {
            var f = line.Split('\t');
            if (f.Length < 7 || !long.TryParse(f[0], out var idx) || !int.TryParse(f[1], out var st)) continue;
            long? seq = null; string? runId = null;
            if (st == Applied && f[6].Length > 0)
            {
                try
                {
                    using var doc = JsonDocument.Parse(Convert.FromBase64String(f[6]));
                    var r = doc.RootElement;
                    if (r.ValueKind == JsonValueKind.Object)
                    {
                        if (r.TryGetProperty("seq", out var s) && s.ValueKind == JsonValueKind.Number) seq = s.GetInt64();
                        if (r.TryGetProperty("runId", out var ru) && ru.ValueKind == JsonValueKind.String) runId = ru.GetString();
                    }
                }
                catch (Exception) { }
            }
            rows.Add(new Row(idx, st, f[2], seq, runId));
        }
        return rows;
    }

    /// <summary>Numero di voci applicate (non duplicate) per un RequestId; null se applied.log non è disponibile.</summary>
    public async Task<int?> AppliedRowsForRequestAsync(string requestId, CancellationToken ct)
    {
        if (!Available) return null;
        var rows = await ReadAsync(ct);
        return rows.Count(r => r.Status == Applied && r.RequestId.Equals(requestId, StringComparison.OrdinalIgnoreCase));
    }

    public async Task<object> VerifyAsync(LoadGenerator gen, ClusterMonitor monitor, CancellationToken ct)
    {
        var nodes = monitor.Snapshot().Where(n => n.Reachable).Select(n => new { n.ShortId, n.Port, n.Role, n.Term, n.CommitIndex, n.LastApplied }).ToList();
        if (!Available)
            return new
            {
                available = false,
                path = FilePath,
                message = "applied.log non trovato. Con la state machine SQL Server la verifica si fa sul database, ad esempio: " +
                          "SELECT RequestId, COUNT(*) FROM raft.IngestedData GROUP BY RequestId HAVING COUNT(*) > 1",
                nodes
            };

        var rows = await ReadAsync(ct);
        var ops = gen.AllOps.Where(o => o.Outcome != OpOutcome.InCorso).ToList();
        var mine = rows.Where(r => r.Status == Applied && r.RunId == gen.RunId && r.Seq != null).ToList();
        var bySeq = mine.GroupBy(r => r.Seq!.Value).ToDictionary(g => g.Key, g => g.ToList());

        var duplicateSeq = bySeq.Where(kv => kv.Value.Count > 1)
            .Select(kv => new { seq = kv.Key, logIndexes = kv.Value.Select(r => r.Index).ToList() }).Take(50).ToList();
        var duplicateRequestIds = rows.Where(r => r.Status == Applied).GroupBy(r => r.RequestId, StringComparer.OrdinalIgnoreCase)
            .Where(g => g.Count() > 1).Select(g => new { requestId = g.Key, logIndexes = g.Select(r => r.Index).ToList() }).Take(50).ToList();
        var ackedMissing = ops.Where(o => o.Outcome is OpOutcome.Ok or OpOutcome.OkDopoRetry && !bySeq.ContainsKey(o.Seq))
            .Select(o => new { o.Seq, o.RequestId, path = o.Path }).Take(50).ToList();
        var failedButWritten = ops.Where(o => o.Outcome is OpOutcome.Abbandonata && bySeq.ContainsKey(o.Seq))
            .Select(o => new { o.Seq, o.RequestId, o.Error }).Take(50).ToList();
        var failedNotWritten = ops.Count(o => o.Outcome is OpOutcome.Abbandonata && !bySeq.ContainsKey(o.Seq));
        bool passed = duplicateSeq.Count == 0 && duplicateRequestIds.Count == 0 && ackedMissing.Count == 0;

        // indici del log consecutivi: ogni voce committed è stata registrata una volta e in ordine
        var indexes = rows.Select(r => r.Index).ToList();
        int gaps = 0;
        for (int i = 1; i < indexes.Count; i++) if (indexes[i] != indexes[i - 1] + 1) gaps++;

        return new
        {
            available = true,
            path = FilePath,
            passed,
            summary = passed
                ? $"Nessun duplicato e nessuna operazione confermata mancante su {ops.Count(o => o.Outcome is OpOutcome.Ok or OpOutcome.OkDopoRetry)} operazioni con 200."
                : "VIOLAZIONE delle garanzie: vedere i dettagli.",
            totals = new
            {
                rows = rows.Count,
                applied = rows.Count(r => r.Status == Applied),
                duplicateDedup = rows.Count(r => r.Status == Duplicate),
                conflict = rows.Count(r => r.Status == Conflict),
                rejected = rows.Count(r => r.Status == Rejected),
                expired = rows.Count(r => r.Status == Expired),
                quarantined = rows.Count(r => r.Status == Quarantined),
                noOp = rows.Count(r => r.Status == NoOp),
                lastLogIndex = indexes.Count == 0 ? 0 : indexes[^1],
                indexGaps = gaps
            },
            run = new
            {
                runId = gen.RunId,
                completedOps = ops.Count,
                ok = ops.Count(o => o.Outcome is OpOutcome.Ok or OpOutcome.OkDopoRetry),
                rejected = ops.Count(o => o.Outcome == OpOutcome.Rifiutata),
                gaveUp = ops.Count(o => o.Outcome == OpOutcome.Abbandonata),
                appliedForRun = mine.Count,
                distinctSeqApplied = bySeq.Count
            },
            duplicateSeq,
            duplicateRequestIds,
            ackedMissing,
            failedButWritten,
            failedNotWritten,
            nodes
        };
    }
}
