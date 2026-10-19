using System.Diagnostics;
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

/// <summary>StatusCode 0 = errore di rete / timeout.</summary>
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

/// <summary>State machine applicativa (REQ-SM-*, spec §11).</summary>
public interface IDataRepository
{
    /// <summary>Idempotente rispetto a entry.Index (REQ-SM-1).</summary>
    Task ApplyAsync(LogEntry entry, CancellationToken ct);
    Task<long> GetLastAppliedIndexAsync(CancellationToken ct);
    Task<byte[]> GetResultAsync(long logIndex, CancellationToken ct);
}
