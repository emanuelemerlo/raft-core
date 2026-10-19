using System.Net;
using System.Net.Sockets;
using RaftCore.Abstractions;
using RaftCore.Configuration;

namespace RaftCore.Services.Network;

/// <summary>Canale UDP (broadcast o multicast) con porta condivisa SO_REUSEADDR/SO_REUSEPORT (REQ-NET-2).</summary>
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
            _dest = new IPEndPoint(g, cfg.UdpDiscoveryPort);
        }
        else
        {
            _tx.EnableBroadcast = true;
            _dest = new IPEndPoint(IPAddress.Parse(cfg.UdpBroadcastAddress), cfg.UdpDiscoveryPort);
        }
    }

    private static void Share(Socket s)
    {
        s.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        try { if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS()) s.SetRawSocketOption(1, 15, BitConverter.GetBytes(1)); } catch { }
    }

    public async Task SendAsync(byte[] data, CancellationToken ct) => await _tx.SendAsync(data, data.Length, _dest);
    public async Task<byte[]> ReceiveAsync(CancellationToken ct) => (await _rx.ReceiveAsync(ct)).Buffer;
    public void Dispose() { _rx.Dispose(); _tx.Dispose(); }
}
