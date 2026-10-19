using System.Diagnostics;
using System.Text;

namespace RaftCore.Simulator.Services;

public sealed class SimulatorOptions
{
    /// <summary>Vuoto = 127.0.0.1 (nessun default nella lista: il binder della configurazione aggiungerebbe, non sostituirebbe).</summary>
    public List<string> Hosts { get; set; } = new();
    public string Ports { get; set; } = "5001-5010";
    public int StatusPollMs { get; set; } = 1000;
    public int StatusTimeoutMs { get; set; } = 500;
    public int DefaultIntervalMs { get; set; } = 200;
    public string DefaultTarget { get; set; } = "RoundRobin";
    public int MaxInFlight { get; set; } = 64;
    public int RequestTimeoutMs { get; set; } = 15000;
    public int RetryBudgetMs { get; set; } = 60000;
    public int MaxBodyBytes { get; set; } = 1048576;
    public string Source { get; set; } = "simulatore";
    public string AppliedLogPath { get; set; } = "../RaftCore/devcluster/shared-state/applied.log";
    public string NodeWorkingDirectory { get; set; } = "../RaftCore";
    public Dictionary<string, string> NodeEnvironment { get; set; } = new();
}

/// <summary>Risolve i percorsi relativi rispetto alla cartella del progetto del simulatore, ovunque giri l'eseguibile.</summary>
public sealed class PathResolver
{
    public string BaseDir { get; }

    public PathResolver(IWebHostEnvironment env)
    {
        // Da Visual Studio o da "dotnet run" la content root è già la cartella del progetto; avviando l'exe da bin/
        // si risale fino alla soluzione.
        string root = env.ContentRootPath;
        if (!File.Exists(Path.Combine(root, "RaftCore.Simulator.csproj")))
        {
            for (var d = new DirectoryInfo(AppContext.BaseDirectory); d != null; d = d.Parent)
            {
                var candidate = Path.Combine(d.FullName, "src", "RaftCore.Simulator");
                if (File.Exists(Path.Combine(d.FullName, "RaftCore.sln")) && Directory.Exists(candidate)) { root = candidate; break; }
            }
        }
        BaseDir = root;
    }

    public string Resolve(string path) => Path.GetFullPath(Path.IsPathRooted(path) ? path : Path.Combine(BaseDir, path));
}

public static class PortSpec
{
    /// <summary>"5001-5010", "5001,5003" o misto "5001-5005;5010": stesso formato di RaftCluster:RestPorts.</summary>
    public static IReadOnlyList<int> Parse(string spec)
    {
        var ports = new List<int>();
        foreach (var raw in spec.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var parts = raw.Split('-', StringSplitOptions.TrimEntries);
            if (parts.Length == 2 && int.TryParse(parts[0], out var a) && int.TryParse(parts[1], out var b) && a <= b)
            { for (int p = a; p <= b; p++) if (!ports.Contains(p)) ports.Add(p); }
            else if (parts.Length == 1 && int.TryParse(parts[0], out var single)) { if (!ports.Contains(single)) ports.Add(single); }
            else throw new FormatException($"Porta non valida: '{raw}'");
        }
        return ports;
    }
}

public sealed record SimEvent(DateTimeOffset Time, string Level, string Text);

/// <summary>Registro eventi mostrato nell'interfaccia (cambi di Leader, guasti iniettati, esiti degli scenari).</summary>
public sealed class EventLog
{
    private readonly LinkedList<SimEvent> _items = new();
    private readonly ILogger<EventLog> _log;
    public EventLog(ILogger<EventLog> log) => _log = log;

    public void Add(string level, string text)
    {
        lock (_items)
        {
            _items.AddFirst(new SimEvent(DateTimeOffset.Now, level, text));
            while (_items.Count > 300) _items.RemoveLast();
        }
        _log.Log(level == "error" ? LogLevel.Warning : LogLevel.Information, "{Text}", text);
    }

    public void Info(string text) => Add("info", text);
    public void Warn(string text) => Add("warn", text);
    public void Error(string text) => Add("error", text);
    public void Ok(string text) => Add("ok", text);

    public IReadOnlyList<SimEvent> Recent(int n = 100) { lock (_items) return _items.Take(n).ToList(); }
}

public sealed record PostResult(int Status, bool Replay, string Body, IReadOnlyDictionary<string, string> Headers, long ElapsedMs, string? Error)
{
    public bool NetworkError => Status == 0;
}

/// <summary>Client HTTP verso i nodi. Lo status 0 indica un errore di rete (connessione rifiutata, reset, timeout).</summary>
public sealed class DataClient
{
    private readonly HttpClient _http = new(new SocketsHttpHandler
    {
        ConnectTimeout = TimeSpan.FromSeconds(2),
        PooledConnectionLifetime = TimeSpan.FromSeconds(30),
        PooledConnectionIdleTimeout = TimeSpan.FromSeconds(5),
        MaxConnectionsPerServer = 512,
        UseProxy = false
    }) { Timeout = Timeout.InfiniteTimeSpan };

    public HttpClient Http => _http;

    public async Task<PostResult> PostDataAsync(string baseUrl, string? requestId, byte[] body, int timeoutMs,
        IEnumerable<KeyValuePair<string, string>>? extraHeaders = null, CancellationToken ct = default)
    {
        var sw = Stopwatch.StartNew();
        using var req = new HttpRequestMessage(HttpMethod.Post, baseUrl.TrimEnd('/') + "/api/v1/data");
        req.Content = new ByteArrayContent(body);                                   // stessi byte a ogni tentativo (REQ-IDM-8)
        req.Content.Headers.TryAddWithoutValidation("Content-Type", "application/json");
        if (requestId != null) req.Headers.TryAddWithoutValidation("X-Request-ID", requestId);
        if (extraHeaders != null) foreach (var h in extraHeaders) req.Headers.TryAddWithoutValidation(h.Key, h.Value);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeoutMs);
        try
        {
            using var res = await _http.SendAsync(req, cts.Token);
            var text = await res.Content.ReadAsStringAsync(cts.Token);
            var headers = res.Headers.Concat(res.Content.Headers)
                .ToDictionary(h => h.Key, h => string.Join(", ", h.Value), StringComparer.OrdinalIgnoreCase);
            bool replay = headers.TryGetValue("X-Idempotent-Replay", out var r) && r.Equals("true", StringComparison.OrdinalIgnoreCase);
            return new PostResult((int)res.StatusCode, replay, Truncate(Readable(text), 400), headers, sw.ElapsedMilliseconds, null);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            string err = ex is OperationCanceledException ? $"timeout del client ({timeoutMs} ms)" : Describe(ex);
            return new PostResult(0, false, "", new Dictionary<string, string>(), sw.ElapsedMilliseconds, err);
        }
    }

    private static string Describe(Exception ex)
    {
        for (var e = ex; e != null; e = e.InnerException)
            if (e is System.Net.Sockets.SocketException se) return se.SocketErrorCode switch
            {
                System.Net.Sockets.SocketError.ConnectionRefused => "connessione rifiutata",
                System.Net.Sockets.SocketError.ConnectionReset => "connessione interrotta",
                _ => se.SocketErrorCode.ToString()
            };
        return ex.InnerException?.Message ?? ex.Message;
    }

    private static readonly System.Text.Json.JsonSerializerOptions RelaxedJson =
        new() { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    /// <summary>JSON con caratteri non ASCII leggibili (il nodo li restituisce come \u00e0).</summary>
    private static string Readable(string text)
    {
        if (text.Length == 0 || text.Length > 8192 || !text.Contains("\\u")) return text;
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(text);
            return System.Text.Json.JsonSerializer.Serialize(doc.RootElement, RelaxedJson);
        }
        catch (System.Text.Json.JsonException) { return text; }
    }

    public static string Truncate(string s, int max) => s.Length <= max ? s : s[..max] + "…";
}
