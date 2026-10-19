using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using RaftCore.Abstractions;
using RaftCore.Configuration;
using RaftCore.Models;
using RaftCore.Services.Common;
using RaftCore.Services.Core;

namespace RaftCore.Services.Network;

/// <summary>Discovery UDP (§5): serve SOLO a trovare gli endpoint dei peer, mai per decisioni Raft (REQ-DIS-1/2).</summary>
public sealed class UdpDiscoveryService : BackgroundService
{
    private readonly ClusterConfig _cfg;
    private readonly RaftNode _node;
    private readonly PeerRegistry _registry;
    private readonly Membership _members;
    private readonly IUdpChannel _udp;
    private readonly Metrics _metrics;
    private readonly ILogger<UdpDiscoveryService> _log;
    private readonly string _incarnation = Convert.ToHexString(RandomNumberGenerator.GetBytes(16)).ToLowerInvariant(); // 128 bit
    private readonly string _url;
    private long _sequence;
    private readonly byte[] _key;

    public UdpDiscoveryService(ClusterConfig cfg, RaftNode node, PeerRegistry registry, Membership members, IUdpChannel udp, Metrics metrics, ILogger<UdpDiscoveryService> log)
    {
        _cfg = cfg; _node = node; _registry = registry; _members = members; _udp = udp; _metrics = metrics; _log = log;
        _url = NetworkHelper.ResolveAdvertiseUrl(cfg);
        _key = Encoding.UTF8.GetBytes(cfg.ClusterSecret);
    }

    protected override Task ExecuteAsync(CancellationToken ct) =>
        Task.WhenAll(Task.Run(() => AnnounceLoopAsync(ct), ct), Task.Run(() => ListenLoopAsync(ct), ct));

    private async Task AnnounceLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var (role, term) = _node.GetRoleAndTerm();
                var msg = new DiscoveryMessage
                {
                    ClusterId = _cfg.ClusterId, ClusterSize = _cfg.ClusterSize, MembersHash = _members.Hash,
                    NodeId = _members.SelfId, RestUrl = _url, Role = role, Term = term,
                    Incarnation = _incarnation, Sequence = Interlocked.Increment(ref _sequence)
                };
                msg.Hmac = ComputeHmac(msg);
                await _udp.SendAsync(JsonSerializer.SerializeToUtf8Bytes(msg), ct);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex) { _log.LogWarning("Invio annuncio UDP fallito: {Msg}", ex.Message); }
            // all'avvio, ogni AnnounceIntervalMs e a ogni cambio di ruolo
            try { await _node.AnnounceNow.WaitAsync(_cfg.AnnounceIntervalMs, ct); }
            catch (OperationCanceledException) { break; }
        }
    }

    private async Task ListenLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var data = await _udp.ReceiveAsync(ct);
                Handle(data);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex) { _log.LogDebug("Datagramma scartato: {Msg}", ex.Message); }
        }
    }

    /// <summary>Visibile per i test.</summary>
    public bool Handle(byte[] data)
    {
        DiscoveryMessage? msg;
        try { msg = JsonSerializer.Deserialize<DiscoveryMessage>(data); } catch { return false; }
        if (msg == null || string.IsNullOrEmpty(msg.NodeId)) return false;
        msg.NodeId = ClusterConfig.Norm(msg.NodeId);
        if (msg.NodeId == _members.SelfId) return false;

        // 1) HMAC (REQ-SEC-2) - prima di fidarsi di qualunque campo
        if (!CryptographicOperations.FixedTimeEquals(Encoding.ASCII.GetBytes(ComputeHmac(msg)), Encoding.ASCII.GetBytes(msg.Hmac ?? "")))
        { _metrics.Inc("udpInvalidHmac"); return false; }
        // 2) ClusterId
        if (msg.ClusterId != _cfg.ClusterId) { _metrics.Inc("clusterIdMismatch"); return false; }
        // 3) Bootstrap: ammissione dei primi ClusterSize NodeId (REQ-CLU-3)
        if (_cfg.MembershipMode == MembershipMode.Bootstrap && _members.TryAdmit(msg.NodeId))
            _log.LogInformation("Bootstrap: ammesso NodeId {Node}", msg.NodeId);
        // 4) ClusterSize / MembersHash (REQ-CLU-4)
        bool hashCheck = _members.IsComplete;
        if (msg.ClusterSize != _cfg.ClusterSize || (hashCheck && msg.MembersHash != _members.Hash))
        {
            _metrics.Inc("membersHashMismatch");
            _log.LogError("ALLARME: annuncio da {Node} con ClusterSize/MembersHash discordanti", msg.NodeId);
            return false;
        }
        // 5) appartenenza (REQ-CLU-5, REQ-DIS-4)
        if (!_members.Contains(msg.NodeId))
        {
            _metrics.Inc("nonMemberRejected");
            _log.LogWarning("ALLARME: annuncio di un non membro {Node} ignorato", msg.NodeId);
            return false;
        }
        // 6) anti-replay (REQ-DIS-5)
        if (_registry.RegisterOrUpdate(msg) == RegisterResult.Replay) { _metrics.Inc("udpReplayRejected"); return false; }
        return true;
    }

    private string ComputeHmac(DiscoveryMessage m)
    {
        // HMAC su TUTTI i campi (REQ-DIS-1)
        var raw = string.Join("\n", m.ClusterId, m.ClusterSize, m.MembersHash, m.NodeId, m.RestUrl, (int)m.Role, m.Term, m.Incarnation, m.Sequence);
        return Convert.ToHexString(HMACSHA256.HashData(_key, Encoding.UTF8.GetBytes(raw))).ToLowerInvariant();
    }
}
