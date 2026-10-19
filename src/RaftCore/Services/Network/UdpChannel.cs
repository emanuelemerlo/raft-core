using System.Net;
using System.Net.Sockets;
using RaftCore.Abstractions;
using RaftCore.Configuration;

namespace RaftCore.Services.Network;

/// <summary>
/// Canale UDP (broadcast o multicast) con porta condivisa tra più istanze sulla stessa macchina (REQ-NET-2).
/// Windows: SO_REUSEADDR senza ExclusiveAddressUse. Linux: SO_REUSEADDR + SO_REUSEPORT (SOL_SOCKET=1, 15).
/// macOS: SO_REUSEPORT (SOL_SOCKET=0xFFFF, 0x0200).
/// Nota: con più istanze sulla stessa macchina Windows il broadcast può essere consegnato a un solo socket;
/// in quel caso usare UdpMode=Multicast oppure Peers statici (gli URL vengono poi propagati dal Leader).
/// </summary>
public sealed class UdpChannel : IUdpChannel, IDisposable
{
    private readonly UdpClient _rx, _tx;
    private readonly IPEndPoint _dest;

    public UdpChannel(ClusterConfig cfg)
    {
        _rx = new UdpClient(AddressFamily.InterNetwork);
        Share(_rx.Client);
        _rx.Client.Bind(new IPEndPoint(IPAddress.Any, cfg.UdpDiscoveryPort));
        _tx = new UdpClient(AddressFamily.InterNetwork);
        if (cfg.UdpMode == UdpMode.Multicast)
        {
            var g = IPAddress.Parse(cfg.UdpMulticastAddress);
            _rx.JoinMulticastGroup(g);
            _tx.MulticastLoopback = true;
            _tx.Client.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastTimeToLive, 1);
            _dest = new IPEndPoint(g, cfg.UdpDiscoveryPort);
        }
        else
        {
            _tx.EnableBroadcast = true;
            _dest = new IPEndPoint(IPAddress.Parse(cfg.UdpBroadcastAddress), cfg.UdpDiscoveryPort);
        }
    }

    internal static void Share(Socket s)
    {
        if (OperatingSystem.IsWindows()) s.ExclusiveAddressUse = false;
        s.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        try
        {
            if (OperatingSystem.IsLinux()) s.SetRawSocketOption(1, 15, BitConverter.GetBytes(1));
            else if (OperatingSystem.IsMacOS() || OperatingSystem.IsFreeBSD()) s.SetRawSocketOption(0xFFFF, 0x0200, BitConverter.GetBytes(1));
        }
        catch (SocketException) { /* SO_REUSEADDR basta per broadcast/multicast */ }
    }

    public async Task SendAsync(byte[] data, CancellationToken ct) => await _tx.SendAsync(data, _dest, ct);
    public async Task<byte[]> ReceiveAsync(CancellationToken ct) => (await _rx.ReceiveAsync(ct)).Buffer;
    public void Dispose() { _rx.Dispose(); _tx.Dispose(); }
}

/// <summary>Discovery UDP disattivata (UdpMode=Disabled): si usano Peers statici e il gossip del Leader.</summary>
public sealed class NullUdpChannel : IUdpChannel
{
    public Task SendAsync(byte[] data, CancellationToken ct) => Task.CompletedTask;
    public async Task<byte[]> ReceiveAsync(CancellationToken ct) { await Task.Delay(Timeout.Infinite, ct); return Array.Empty<byte>(); }
}
