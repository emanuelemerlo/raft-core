using System.Diagnostics;
using System.Text;
using RaftCore.Configuration;

namespace RaftCore.Hosting;

/// <summary>
/// Cluster locale di sviluppo (solo ambiente Development, DevCluster:Instances &gt; 1).
/// Il processo avviato (es. F5 in Visual Studio, l'unico sotto debugger) è il primo nodo; gli altri membri vengono
/// avviati come processi figli dello stesso eseguibile, con NodeId diverso e la stessa configurazione.
/// I figli sorvegliano il padre (<see cref="ParentWatchdog"/>) e si chiudono quando termina, anche se ucciso dal debugger.
/// </summary>
public sealed class DevClusterLauncher : IDisposable
{
    private readonly List<Process> _children = new();
    private readonly ILogger _log;

    private DevClusterLauncher(ILogger log) => _log = log;

    public static DevClusterLauncher? StartIfConfigured(RunningNode node, string[] args)
    {
        var cfg = node.Config;
        var env = node.App.Environment;
        var log = node.App.Logger;
        if (cfg.DevCluster.ParentPid != 0 || cfg.DevCluster.Instances <= 1) return null;
        if (!env.IsDevelopment())
        {
            log.LogWarning("DevCluster:Instances = {N} ignorato: il cluster locale si avvia solo nell'ambiente Development.", cfg.DevCluster.Instances);
            return null;
        }
        var others = cfg.Members.Select(ClusterConfig.Norm).Where(m => m != ClusterConfig.Norm(cfg.NodeId)).Take(cfg.DevCluster.Instances - 1).ToList();
        if (others.Count < cfg.DevCluster.Instances - 1)
            log.LogWarning("DevCluster:Instances = {N} ma Members contiene solo {M} altri nodi.", cfg.DevCluster.Instances, others.Count);

        var launcher = new DevClusterLauncher(log);
        string mode = ResolveOutputMode(cfg.DevCluster.ChildOutput);
        int k = 1;
        foreach (var id in others)
        {
            if (k > 1) Thread.Sleep(400);                    // avvio scaglionato: meno contese sulla stessa porta
            launcher.StartChild(id, ++k, args, mode, cfg);
        }
        log.LogInformation("Cluster di sviluppo: questo processo è il nodo 1 (debug); avviati {N} nodi figli ({Mode}). Si chiudono con questo processo.",
            launcher._children.Count, mode);
        node.App.Lifetime.ApplicationStopping.Register(launcher.Dispose);
        return launcher;
    }

    private static string ResolveOutputMode(string configured) => configured.Trim().ToLowerInvariant() switch
    {
        "window" => OperatingSystem.IsWindows() ? "Window" : "Prefix",
        "file" => "File",
        "prefix" => "Prefix",
        _ => OperatingSystem.IsWindows() ? "Window" : "Prefix"
    };

    private void StartChild(string nodeId, int ordinal, string[] parentArgs, string mode, ClusterConfig cfg)
    {
        var (exe, prefixArgs) = CurrentExecutable();
        var childArgs = new List<string>(prefixArgs);
        childArgs.AddRange(parentArgs);
        childArgs.Add($"--RaftCluster:NodeId={nodeId}");
        childArgs.Add("--RaftCluster:DevCluster:Instances=1");
        childArgs.Add($"--RaftCluster:DevCluster:ParentPid={Environment.ProcessId}");

        var psi = new ProcessStartInfo(exe) { WorkingDirectory = Directory.GetCurrentDirectory() };
        if (mode == "Window")
        {
            // nuova finestra console per ogni nodo; l'ambiente (ASPNETCORE_ENVIRONMENT, CLUSTER_SECRET, ...) è ereditato
            psi.UseShellExecute = true;
            psi.Arguments = JoinArguments(childArgs);
        }
        else
        {
            psi.UseShellExecute = false;
            foreach (var a in childArgs) psi.ArgumentList.Add(a);
            psi.RedirectStandardOutput = true;
            psi.RedirectStandardError = true;
        }

        var p = Process.Start(psi) ?? throw new InvalidOperationException("Avvio del nodo figlio fallito");
        _children.Add(p);
        if (mode != "Window")
        {
            string tag = $"[nodo{ordinal} {nodeId[..8]}] ";
            StreamWriter? file = null;
            if (mode == "File")
            {
                var dir = Path.Combine(Directory.GetCurrentDirectory(), "devcluster", "logs");
                Directory.CreateDirectory(dir);
                file = new StreamWriter(Path.Combine(dir, $"nodo{ordinal}.log"), append: true) { AutoFlush = true };
            }
            void Line(string? l)
            {
                if (l == null) return;
                if (file != null) lock (file) { try { file.WriteLine(l); } catch (ObjectDisposedException) { } }
                else Console.WriteLine(tag + l);
            }
            p.OutputDataReceived += (_, e) => Line(e.Data);
            p.ErrorDataReceived += (_, e) => Line(e.Data);
            p.BeginOutputReadLine(); p.BeginErrorReadLine();
            if (file != null) p.Exited += (_, _) => { lock (file) file.Dispose(); };
            p.EnableRaisingEvents = file != null;
        }
        _log.LogInformation("Avviato il nodo {Ordinal} ({Node}), PID {Pid}", ordinal, nodeId, p.Id);
    }

    /// <summary>Eseguibile corrente: l'apphost (RaftCore.exe / RaftCore) oppure "dotnet RaftCore.dll".</summary>
    internal static (string Exe, IReadOnlyList<string> PrefixArgs) CurrentExecutable()
    {
        var path = Environment.ProcessPath ?? throw new InvalidOperationException("Percorso del processo non disponibile");
        var name = Path.GetFileNameWithoutExtension(path);
        if (name.Equals("dotnet", StringComparison.OrdinalIgnoreCase))
        {
            var dll = typeof(DevClusterLauncher).Assembly.Location;
            return (path, new[] { dll });
        }
        return (path, Array.Empty<string>());
    }

    /// <summary>Quoting secondo le regole di CommandLineToArgvW (usato con UseShellExecute).</summary>
    internal static string JoinArguments(IEnumerable<string> args)
    {
        var sb = new StringBuilder();
        foreach (var a in args)
        {
            if (sb.Length > 0) sb.Append(' ');
            if (a.Length > 0 && a.IndexOfAny(new[] { ' ', '\t', '"' }) < 0) { sb.Append(a); continue; }
            sb.Append('"');
            int backslashes = 0;
            foreach (var c in a)
            {
                if (c == '\\') { backslashes++; continue; }
                if (c == '"') { sb.Append('\\', backslashes * 2 + 1); sb.Append('"'); }
                else { sb.Append('\\', backslashes); sb.Append(c); }
                backslashes = 0;
            }
            sb.Append('\\', backslashes * 2);
            sb.Append('"');
        }
        return sb.ToString();
    }

    public void Dispose()
    {
        // i figli si chiudono da soli quando il padre termina (watchdog); qui li si attende brevemente
        foreach (var p in _children)
        {
            try { if (!p.HasExited) p.WaitForExit(3000); } catch { }
        }
    }
}

/// <summary>Nei nodi figli del cluster di sviluppo: chiude il nodo quando il processo padre termina.</summary>
public static class ParentWatchdog
{
    public static void Start(RunningNode node)
    {
        int pid = node.Config.DevCluster.ParentPid;
        if (pid <= 0) return;
        if (OperatingSystem.IsWindows())
        {
            try { Console.Title = $"RaftCore nodo {ClusterConfig.Norm(node.Config.NodeId)[..8]} - porta {node.Config.RestPort}"; } catch { }
        }
        Process? parent;
        try { parent = Process.GetProcessById(pid); } catch (ArgumentException) { parent = null; }
        _ = Task.Run(async () =>
        {
            while (true)
            {
                bool alive;
                try { alive = parent != null && !parent.HasExited; } catch { alive = false; }
                if (!alive)
                {
                    node.App.Logger.LogInformation("Il processo padre (PID {Pid}) è terminato: chiusura del nodo.", pid);
                    node.App.Lifetime.StopApplication();
                    return;
                }
                await Task.Delay(500);
            }
        });
    }
}
