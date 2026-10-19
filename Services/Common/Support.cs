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

/// <summary>fsync della directory (REQ-WAL-2). Su Windows è un no-op (NTFS journaling dei metadati).</summary>
public static class DirSync
{
    [DllImport("libc", SetLastError = true)] private static extern int open([MarshalAs(UnmanagedType.LPStr)] string path, int flags);
    [DllImport("libc", SetLastError = true)] private static extern int fsync(int fd);
    [DllImport("libc", SetLastError = true)] private static extern int close(int fd);

    public static void Sync(string? dir)
    {
        if (string.IsNullOrEmpty(dir) || RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) return;
        try
        {
            int fd = open(dir, 0 /* O_RDONLY */);
            if (fd < 0) return;
            try { fsync(fd); } finally { close(fd); }
        }
        catch { /* best effort su piattaforme senza libc */ }
    }
}

/// <summary>Metriche minime (REQ-OPS-3).</summary>
public sealed class Metrics
{
    private readonly ConcurrentDictionary<string, long> _c = new();
    private readonly ConcurrentDictionary<string, long> _g = new();
    public void Inc(string name, long by = 1) => _c.AddOrUpdate(name, by, (_, v) => v + by);
    public void Set(string name, long value) => _g[name] = value;
    public long Get(string name) => _c.TryGetValue(name, out var v) ? v : (_g.TryGetValue(name, out var g) ? g : 0);
    public Dictionary<string, long> Snapshot()
    {
        var d = new SortedDictionary<string, long>(StringComparer.Ordinal);
        foreach (var n in new[] { "electionCount", "prevoteDenied", "stepDownCount", "requestCount", "requestRetryCount", "requestDuplicateCount",
            "requestFailureCount", "requestRejectedOverload", "walWriteErrors", "udpInvalidHmac", "udpReplayRejected", "rpcClockSkewRejected",
            "membersHashMismatch", "nonMemberRejected" }) d[n] = 0;
        foreach (var kv in _c) d[kv.Key] = kv.Value;
        foreach (var kv in _g) d[kv.Key] = kv.Value;
        return new Dictionary<string, long>(d);
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
}
