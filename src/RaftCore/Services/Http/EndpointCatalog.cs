using System.Collections.Concurrent;
using RaftCore.Configuration;
using RaftCore.Services.Common;

namespace RaftCore.Services.Http;

/// <summary>
/// Endpoint di ingestione configurati (REQ-EPT-1) e confronto della loro configurazione con gli altri nodi (REQ-VER-1).
/// Una differenza non blocca nulla: viene segnalata nei log, nella metrica endpointsHashMismatch (numero di membri
/// discordanti) e nel campo endpointsMismatch dello stato.
/// </summary>
public sealed class EndpointCatalog
{
    private readonly Dictionary<string, EndpointConfig> _byName;
    private readonly ConcurrentDictionary<string, string> _remote = new(); // NodeId -> ultima impronta osservata
    private readonly Metrics? _metrics;
    private readonly ILogger? _log;
    private readonly LogThrottle _throttle = new(TimeSpan.FromSeconds(60));

    public IReadOnlyList<EndpointConfig> Endpoints { get; }
    public string Hash { get; }

    public EndpointCatalog(ClusterConfig cfg, Metrics? metrics = null, ILogger<EndpointCatalog>? log = null)
    {
        Endpoints = cfg.Endpoints.ToList();
        _byName = Endpoints.ToDictionary(e => e.Name, StringComparer.Ordinal);
        Hash = ClusterConfig.ComputeEndpointsHash(Endpoints);
        _metrics = metrics; _log = log;
    }

    public bool Contains(string name) => _byName.ContainsKey(name);

    public EndpointConfig? Find(string name) => _byName.TryGetValue(name, out var e) ? e : null;

    /// <summary>Registra l'impronta vista in una RPC autenticata di un membro (vuota = sconosciuta, ignorata).</summary>
    public void NoteRemote(string nodeId, string? hash)
    {
        if (string.IsNullOrEmpty(hash)) return;
        var previous = _remote.TryGetValue(nodeId, out var p) ? p : null;
        _remote[nodeId] = hash;
        if (previous == hash) return;
        _metrics?.Set("endpointsHashMismatch", _remote.Count(kv => kv.Value != Hash));
        if (hash != Hash && _throttle.ShouldLog("endpoints:" + nodeId))
            _log?.LogWarning("Configurazione Endpoints diversa sul membro {Node} (impronta {Remote}, locale {Local}): " +
                "allineare Endpoints su tutti i nodi", nodeId, hash, Hash);
        else if (hash == Hash && previous != null)
            _log?.LogInformation("Configurazione Endpoints di {Node} di nuovo allineata", nodeId);
    }

    /// <summary>Membri la cui ultima impronta osservata differisce da quella locale.</summary>
    public IReadOnlyList<string> MismatchedNodes() =>
        _remote.Where(kv => kv.Value != Hash).Select(kv => kv.Key).OrderBy(x => x, StringComparer.Ordinal).ToList();
}
