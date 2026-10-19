using System.Collections.Concurrent;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using RaftCore.Configuration;

namespace RaftCore.Services.Common;

public static class Crc32C
{
    private static readonly uint[] Table = Build();
    private static uint[] Build()
    {
        var t = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint c = i;
            for (int k = 0; k < 8; k++) c = (c & 1) != 0 ? (c >> 1) ^ 0x82F63B78u : c >> 1;
            t[i] = c;
        }
        return t;
    }
    public static uint Compute(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFFu;
        foreach (var b in data) crc = Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
        return ~crc;
    }
}

/// <summary>
/// Persistenza durevole multipiattaforma (REQ-WAL-2).
/// Linux: fsync. macOS: fsync non svuota la cache del disco, serve F_FULLFSYNC. Windows: FlushFileBuffers;
/// la sincronizzazione della directory non è possibile né necessaria (NTFS registra i metadati nel journal).
/// </summary>
public static class Durability
{
    private const int F_FULLFSYNC = 51; // macOS
    [DllImport("libc", SetLastError = true)] private static extern int open([MarshalAs(UnmanagedType.LPStr)] string path, int flags);
    [DllImport("libc", SetLastError = true)] private static extern int fsync(int fd);
    [DllImport("libc", SetLastError = true)] private static extern int close(int fd);
    [DllImport("libc", SetLastError = true)] private static extern int fcntl(int fd, int cmd, int arg);

    public static void FlushFile(FileStream fs)
    {
        fs.Flush(true);
        if (OperatingSystem.IsMacOS())
        {
            bool added = false;
            try
            {
                fs.SafeFileHandle.DangerousAddRef(ref added);
                if (fcntl((int)fs.SafeFileHandle.DangerousGetHandle(), F_FULLFSYNC, 0) != 0)
                    throw new IOException("F_FULLFSYNC fallito, errno " + Marshal.GetLastWin32Error());
            }
            finally { if (added) fs.SafeFileHandle.DangerousRelease(); }
        }
    }

    public static void SyncDirectory(string? dir)
    {
        if (string.IsNullOrEmpty(dir) || OperatingSystem.IsWindows()) return;
        int fd = open(dir, 0 /* O_RDONLY */);
        if (fd < 0) throw new IOException($"Impossibile aprire la directory {dir} per la sincronizzazione, errno " + Marshal.GetLastWin32Error());
        try
        {
            int r = OperatingSystem.IsMacOS() ? fcntl(fd, F_FULLFSYNC, 0) : fsync(fd);
            if (r != 0 && OperatingSystem.IsLinux()) throw new IOException($"fsync della directory {dir} fallito, errno " + Marshal.GetLastWin32Error());
        }
        finally { close(fd); }
    }

    /// <summary>Scrittura atomica e durevole: file temporaneo, flush, rename, sincronizzazione della directory.</summary>
    public static void WriteAllBytesDurable(string path, ReadOnlySpan<byte> data)
    {
        var tmp = path + ".tmp";
        using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            fs.Write(data);
            FlushFile(fs);
        }
        File.Move(tmp, path, true);
        SyncDirectory(Path.GetDirectoryName(path));
    }
}

/// <summary>Impedisce a due processi di usare la stessa DataDir (corromperebbero il WAL).</summary>
public sealed class DataDirLock : IDisposable
{
    private readonly FileStream _fs;
    public string Path { get; }
    private DataDirLock(FileStream fs, string path) { _fs = fs; Path = path; }

    public static DataDirLock Acquire(string dataDir)
    {
        Directory.CreateDirectory(dataDir);
        var p = System.IO.Path.Combine(dataDir, ".lock");
        try
        {
            var fs = new FileStream(p, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            fs.SetLength(0);
            var info = System.Text.Encoding.UTF8.GetBytes($"pid={Environment.ProcessId} host={Environment.MachineName} since={DateTime.UtcNow:O}\n");
            fs.Write(info); fs.Flush();
            // Windows: FileShare.None è un lock obbligatorio. Linux/macOS: .NET applica flock(LOCK_EX) per FileShare.None.
            return new DataDirLock(fs, p);
        }
        catch (IOException ex)
        {
            throw new IOException($"La directory dati {dataDir} è già in uso da un altro processo ({ex.Message}).", ex);
        }
    }

    public void Dispose() => _fs.Dispose();
}

/// <summary>Metriche minime (REQ-OPS-3).</summary>
public sealed class Metrics
{
    private readonly ConcurrentDictionary<string, long> _c = new();
    private readonly ConcurrentDictionary<string, long> _g = new();
    public void Inc(string name, long by = 1) => _c.AddOrUpdate(name, by, (_, v) => v + by);
    public void Set(string name, long value) => _g[name] = value;
    public long Get(string name) => _c.TryGetValue(name, out var v) ? v : (_g.TryGetValue(name, out var g) ? g : 0);

    private long _fsyncCount, _fsyncTotalMicros, _fsyncMaxMicros;
    public void ObserveFsync(TimeSpan d)
    {
        long us = (long)d.TotalMicroseconds;
        Interlocked.Increment(ref _fsyncCount);
        Interlocked.Add(ref _fsyncTotalMicros, us);
        long cur;
        while (us > (cur = Interlocked.Read(ref _fsyncMaxMicros)) && Interlocked.CompareExchange(ref _fsyncMaxMicros, us, cur) != cur) { }
    }

    public static readonly string[] Known =
    {
        "electionCount", "prevoteDenied", "stepDownCount", "requestCount", "requestRetryCount", "requestDuplicateCount",
        "requestFailureCount", "requestRejectedOverload", "walWriteErrors", "udpInvalidHmac", "udpReplayRejected", "rpcClockSkewRejected",
        "membersHashMismatch", "nonMemberRejected", "rpcInvalidAuth", "rpcReplayRejected", "rpcWrongDestination", "rpcResponseAuthFailed",
        "applyTransientErrors", "applyErrors", "applyQuarantined", "applyRejected", "applyExpired", "snapshotsInstalled", "snapshotsSent",
        "compactions", "forwardUntrustedHeaders", "peerUrlUnknown", "writeBatches"
    };

    public Dictionary<string, long> Snapshot()
    {
        var d = new SortedDictionary<string, long>(StringComparer.Ordinal);
        foreach (var n in Known) d[n] = 0;
        foreach (var kv in _c) d[kv.Key] = kv.Value;
        foreach (var kv in _g) d[kv.Key] = kv.Value;
        long n2 = Interlocked.Read(ref _fsyncCount);
        d["walFsyncCount"] = n2;
        d["walFsyncLatencyAvgUs"] = n2 == 0 ? 0 : Interlocked.Read(ref _fsyncTotalMicros) / n2;
        d["walFsyncLatencyMaxUs"] = Interlocked.Read(ref _fsyncMaxMicros);
        return new Dictionary<string, long>(d);
    }
}

/// <summary>Evita che un messaggio ripetuto (es. peer irraggiungibile) inondi il log.</summary>
public sealed class LogThrottle
{
    private readonly ConcurrentDictionary<string, long> _last = new();
    private readonly long _intervalMs;
    public LogThrottle(TimeSpan interval) => _intervalMs = (long)interval.TotalMilliseconds;
    public bool ShouldLog(string key)
    {
        long now = Environment.TickCount64;
        var prev = _last.GetOrAdd(key, long.MinValue / 2);
        if (now - prev < _intervalMs) return false;
        _last[key] = now;
        return true;
    }
}

public static class NetworkHelper
{
    /// <summary>REQ-NET-3: AdvertiseUrl, altrimenti IP di BindInterface, altrimenti route predefinita.</summary>
    public static string ResolveAdvertiseUrl(ClusterConfig cfg)
    {
        if (!string.IsNullOrWhiteSpace(cfg.AdvertiseUrl)) return cfg.AdvertiseUrl!.TrimEnd('/');
        IPAddress? ip = null;
        if (!string.IsNullOrWhiteSpace(cfg.BindInterface))
        {
            if (IPAddress.TryParse(cfg.BindInterface, out var parsed)) ip = parsed;
            else
            {
                var nic = NetworkInterface.GetAllNetworkInterfaces().FirstOrDefault(n => n.Name == cfg.BindInterface);
                ip = nic?.GetIPProperties().UnicastAddresses.FirstOrDefault(a => a.Address.AddressFamily == AddressFamily.InterNetwork)?.Address;
            }
        }
        ip ??= DefaultRouteAddress();
        return $"http://{(ip ?? IPAddress.Loopback)}:{cfg.RestPort}";
    }

    private static IPAddress? DefaultRouteAddress()
    {
        try
        {
            using var s = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            s.Connect(new IPEndPoint(IPAddress.Parse("192.0.2.1"), 9)); // nessun pacchetto inviato: seleziona solo la route
            return (s.LocalEndPoint as IPEndPoint)?.Address;
        }
        catch { return null; }
    }

    /// <summary>True se la porta TCP è libera su tutte le interfacce (punto 6: range di porte).</summary>
    public static bool IsTcpPortFree(int port)
    {
        try
        {
            var l = new TcpListener(IPAddress.Any, port);
            l.Server.ExclusiveAddressUse = OperatingSystem.IsWindows();
            l.Start();
            l.Stop();
            return true;
        }
        catch (SocketException) { return false; }
    }
}
