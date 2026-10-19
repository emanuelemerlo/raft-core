using System.Diagnostics;
using System.Text;
using System.Text.Json;
using RaftCore.Models;

namespace RaftCore.Abstractions;

/// <summary>Astrazione del tempo (REQ-LDR-5: solo clock monotoni per le decisioni Raft; §20 fault injection).</summary>
public interface IClock
{
    long MonotonicMs { get; }
    DateTime UtcNow { get; }
    Task Delay(int milliseconds, CancellationToken ct);
}

public sealed class SystemClock : IClock
{
    private static readonly Stopwatch Sw = Stopwatch.StartNew();
    public long MonotonicMs => Sw.ElapsedMilliseconds + 1_000_000; // offset: evita valori vicini a 0
    public DateTime UtcNow => DateTime.UtcNow;
    public Task Delay(int milliseconds, CancellationToken ct) => Task.Delay(milliseconds, ct);
}

public sealed record PeerEndpoint(string NodeId, string Url);

/// <summary>StatusCode 0 = errore di rete, timeout o risposta non autenticata.</summary>
public sealed record RpcResult<T>(int StatusCode, T? Body)
{
    public bool Ok => StatusCode == 200 && Body != null;
}

/// <summary>Astrazione della rete tra nodi (iniezione di perdite, ritardi, partizioni nei test).</summary>
public interface IPeerTransport
{
    Task<RpcResult<TResp>> SendAsync<TReq, TResp>(PeerEndpoint peer, string path, TReq request, TimeSpan timeout, CancellationToken ct)
        where TReq : BaseRpcRequest;
}

public interface IUdpChannel
{
    Task SendAsync(byte[] data, CancellationToken ct);
    Task<byte[]> ReceiveAsync(CancellationToken ct);
}

/// <summary>
/// State machine applicativa su database CONDIVISO tra i nodi (topologia scelta per v0.2).
/// Contratto:
/// <list type="bullet">
/// <item>ApplyAsync è condizionale su LastApplied nella stessa transazione della scrittura: se entry.Index &lt;= LastApplied
/// restituisce l'esito già registrato (Replay), se entry.Index &gt; LastApplied + 1 lancia <see cref="ApplyGapException"/>.
/// Così un nodo rientrato dopo settimane, o un ex Leader, non riscrive mai dati già scritti.</item>
/// <item>Gli errori di business permanenti (vincoli, conversioni, payload non valido) NON vengono rilanciati:
/// l'esito Rejected viene registrato e LastApplied avanza.</item>
/// <item>Le voci più vecchie di <see cref="ApplyContext.MaxPendingAge"/> producono l'esito Expired senza scrivere dati.</item>
/// <item>Ogni altra eccezione viene ritentata dal nodo; se <see cref="IsTransient"/> è false, dopo MaxApplyAttempts la voce
/// viene messa in quarantena con <see cref="QuarantineAsync"/> (poison entry).</item>
/// </list>
/// </summary>
public interface IDataRepository
{
    Task InitializeAsync(CancellationToken ct);
    Task<long> GetLastAppliedIndexAsync(CancellationToken ct);
    Task<ApplyOutcome> ApplyAsync(LogEntry entry, ApplyContext ctx, CancellationToken ct);
    Task<ApplyOutcome> QuarantineAsync(LogEntry entry, string reason, ApplyContext ctx, CancellationToken ct);
    /// <summary>Esito registrato per un RequestId, oppure null se mai applicato.</summary>
    Task<ApplyOutcome?> GetOutcomeAsync(string requestId, CancellationToken ct);
    bool IsTransient(Exception ex);
}

/// <summary>Il database ha LastApplied diverso da quello atteso dall'applicatore: il cursore va riallineato.</summary>
public sealed class ApplyGapException : Exception
{
    public long DatabaseLastApplied { get; }
    public ApplyGapException(long dbLast, long requested)
        : base($"LastApplied nel database = {dbLast}, voce richiesta = {requested}") => DatabaseLastApplied = dbLast;
}

/// <summary>Errore di business permanente sollevato da un writer applicativo: la voce viene registrata come Rejected.</summary>
public sealed class PayloadRejectedException : Exception
{
    public PayloadRejectedException(string message, Exception? inner = null) : base(message, inner) { }
}

/// <summary>Costruzione deterministica degli esiti HTTP (identici su ogni replay e dopo un failover).</summary>
public static class Outcomes
{
    private static byte[] Json(object o) => JsonSerializer.SerializeToUtf8Bytes(o);

    public static int HttpFor(ApplyStatus s) => s switch
    {
        ApplyStatus.Applied or ApplyStatus.Duplicate or ApplyStatus.NoOp => 200,
        ApplyStatus.Conflict or ApplyStatus.Rejected or ApplyStatus.Quarantined => 422,
        ApplyStatus.Expired => 410,
        _ => 500
    };

    public static ApplyOutcome Applied(long index, string requestId) =>
        new(ApplyStatus.Applied, 200, Json(new { status = "Committed", logIndex = index, requestId }), index);

    public static ApplyOutcome Conflict(long index, string requestId) =>
        new(ApplyStatus.Conflict, 422, Json(new { error = "RequestId già associato a un payload diverso", requestId }), index);

    public static ApplyOutcome Rejected(long index, string requestId, string reason) =>
        new(ApplyStatus.Rejected, 422, Json(new { error = "rejected", reason = Truncate(reason), logIndex = index, requestId }), index);

    public static ApplyOutcome Quarantined(long index, string requestId, string reason) =>
        new(ApplyStatus.Quarantined, 422, Json(new { error = "quarantined", reason = Truncate(reason), logIndex = index, requestId }), index);

    public static ApplyOutcome Expired(long index, string requestId, DateTime receivedUtc) =>
        new(ApplyStatus.Expired, 410, Json(new { error = "expired", detail = "richiesta rimasta in sospeso oltre MaxPendingAge: non scritta", receivedUtc, logIndex = index, requestId }), index);

    public static ApplyOutcome NoOp(long index) => new(ApplyStatus.NoOp, 200, Array.Empty<byte>(), index);

    public static string Truncate(string s) => s.Length <= 500 ? s : s[..500];

    public static string BodyText(ApplyOutcome o) => Encoding.UTF8.GetString(o.Body);
}
