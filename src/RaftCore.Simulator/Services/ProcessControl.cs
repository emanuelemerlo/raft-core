using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace RaftCore.Simulator.Services;

/// <summary>Come rilanciare un nodo: eseguibile, argomenti e directory di lavoro letti dal processo in esecuzione.</summary>
public sealed record LaunchInfo(string Executable, string? ArgumentString, IReadOnlyList<string>? Arguments, string? WorkingDirectory)
{
    /// <summary>PID del nodo 1 sorvegliato da questo nodo figlio: se è terminato, il figlio rilanciato si chiuderebbe subito.</summary>
    public int? ParentPid
    {
        get
        {
            var m = System.Text.RegularExpressions.Regex.Match(AllText, @"DevCluster:ParentPid=(\d+)", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            return m.Success ? int.Parse(m.Groups[1].Value) : null;
        }
    }
    public string AllText => ArgumentString ?? string.Join(' ', Arguments ?? Array.Empty<string>());
}

/// <summary>
/// Guasti a livello di processo sui nodi locali: individuazione del PID dalla porta in ascolto, kill brutale
/// (equivalente a kill -9), pausa (SIGSTOP / NtSuspendProcess) e riavvio con la stessa riga di comando.
/// Agisce solo su processi RaftCore.
/// </summary>
public sealed class ProcessControl : IDisposable
{
    private readonly ConcurrentDictionary<int, CancellationTokenSource> _paused = new();
    private readonly ILogger<ProcessControl> _log;

    public ProcessControl(ILogger<ProcessControl> log) => _log = log;

    public bool IsPaused(int pid) => _paused.ContainsKey(pid);

    // ------------------------------------------------------------------ PID dalla porta
    public int? FindListeningPid(int port)
    {
        try
        {
            if (OperatingSystem.IsWindows()) return WindowsTcp.FindListeningPid(port);
            if (OperatingSystem.IsLinux()) return LinuxProc.FindListeningPid(port);
            return Lsof(port);
        }
        catch (Exception ex) { _log.LogDebug(ex, "PID della porta {Port} non individuato", port); return null; }
    }

    private static int? Lsof(int port)
    {
        var psi = new ProcessStartInfo("lsof", $"-nP -iTCP:{port} -sTCP:LISTEN -t") { RedirectStandardOutput = true, UseShellExecute = false };
        using var p = Process.Start(psi);
        if (p == null) return null;
        var output = p.StandardOutput.ReadToEnd();
        p.WaitForExit(2000);
        var first = output.Split('\n', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();
        return int.TryParse(first, out var pid) ? pid : null;
    }

    // ------------------------------------------------------------------ riga di comando
    public LaunchInfo? GetLaunchInfo(int pid, string fallbackWorkingDir)
    {
        try
        {
            if (OperatingSystem.IsWindows())
            {
                var (exe, cmd) = WindowsCommandLine(pid);
                if (exe == null || cmd == null) return null;
                return new LaunchInfo(exe, StripFirstToken(cmd), null, fallbackWorkingDir);
            }
            if (OperatingSystem.IsLinux())
            {
                var argv = File.ReadAllText($"/proc/{pid}/cmdline").Split('\0', StringSplitOptions.RemoveEmptyEntries);
                if (argv.Length == 0) return null;
                string exe = new FileInfo($"/proc/{pid}/exe").LinkTarget ?? argv[0];
                string? cwd = new DirectoryInfo($"/proc/{pid}/cwd").LinkTarget;
                return new LaunchInfo(exe, null, argv.Skip(1).ToList(), cwd ?? fallbackWorkingDir);
            }
            // macOS: ps restituisce la riga di comando completa
            var psi = new ProcessStartInfo("ps", $"-o command= -p {pid}") { RedirectStandardOutput = true, UseShellExecute = false };
            using var p = Process.Start(psi)!;
            var line = p.StandardOutput.ReadToEnd().Trim();
            p.WaitForExit(2000);
            if (line.Length == 0) return null;
            using var proc = Process.GetProcessById(pid);
            return new LaunchInfo(proc.MainModule?.FileName ?? line.Split(' ')[0], StripFirstToken(line), null, fallbackWorkingDir);
        }
        catch (Exception ex) { _log.LogDebug(ex, "Riga di comando del PID {Pid} non disponibile", pid); return null; }
    }

    [SupportedOSPlatform("windows")]
    private static (string? Exe, string? CommandLine) WindowsCommandLine(int pid)
    {
        using var s = new System.Management.ManagementObjectSearcher(
            $"SELECT ExecutablePath, CommandLine FROM Win32_Process WHERE ProcessId = {pid}");
        foreach (var o in s.Get())
            using (o) return (o["ExecutablePath"] as string, o["CommandLine"] as string);
        return (null, null);
    }

    /// <summary>Toglie l'eseguibile (primo token, eventualmente tra virgolette) e lascia gli argomenti così come sono.</summary>
    internal static string StripFirstToken(string commandLine)
    {
        var s = commandLine.TrimStart();
        if (s.StartsWith('"'))
        {
            int end = s.IndexOf('"', 1);
            return end < 0 ? "" : s[(end + 1)..].TrimStart();
        }
        int sp = s.IndexOf(' ');
        return sp < 0 ? "" : s[(sp + 1)..].TrimStart();
    }

    // ------------------------------------------------------------------ azioni
    /// <summary>Verifica che il PID sia un nodo RaftCore prima di toccarlo.</summary>
    public bool IsRaftNode(int pid, LaunchInfo? info)
    {
        try
        {
            using var p = Process.GetProcessById(pid);
            if (p.ProcessName.Equals("RaftCore", StringComparison.OrdinalIgnoreCase)) return true;
            return info != null && (info.Executable.Contains("RaftCore", StringComparison.OrdinalIgnoreCase)
                                    || info.AllText.Contains("RaftCore.dll", StringComparison.OrdinalIgnoreCase));
        }
        catch { return false; }
    }

    /// <summary>Terminazione immediata, senza shutdown ordinato (TerminateProcess / SIGKILL).</summary>
    public void Kill(int pid)
    {
        Resume(pid);
        using var p = Process.GetProcessById(pid);
        p.Kill(entireProcessTree: false);
        p.WaitForExit(5000);
    }

    public void Pause(int pid, int seconds, Action? onResumed = null)
    {
        if (_paused.ContainsKey(pid)) return;
        Signal(pid, stop: true);
        var cts = new CancellationTokenSource();
        _paused[pid] = cts;
        _ = Task.Run(async () =>
        {
            try { await Task.Delay(TimeSpan.FromSeconds(seconds), cts.Token); } catch (OperationCanceledException) { }
            if (_paused.TryRemove(pid, out _))
            {
                try { Signal(pid, stop: false); } catch { }
                onResumed?.Invoke();
            }
        });
    }

    public void Resume(int pid)
    {
        if (_paused.TryRemove(pid, out var cts))
        {
            try { Signal(pid, stop: false); } catch { }
            cts.Cancel();
        }
    }

    public Process Start(LaunchInfo info)
    {
        var psi = new ProcessStartInfo(info.Executable)
        {
            WorkingDirectory = info.WorkingDirectory ?? Directory.GetCurrentDirectory(),
        };
        if (OperatingSystem.IsWindows())
        {
            // come il DevClusterLauncher: una finestra console per nodo, ambiente ereditato dal simulatore
            psi.UseShellExecute = true;
            psi.Arguments = info.ArgumentString ?? "";
        }
        else
        {
            psi.UseShellExecute = false;
            if (info.Arguments != null) foreach (var a in info.Arguments) psi.ArgumentList.Add(a);
            else psi.Arguments = info.ArgumentString ?? "";
            psi.RedirectStandardOutput = true;
            psi.RedirectStandardError = true;
        }
        var p = Process.Start(psi) ?? throw new InvalidOperationException("Avvio del processo fallito");
        if (!OperatingSystem.IsWindows())
        {
            // senza finestra propria l'output va su file (letto di continuo, altrimenti il nodo si blocca a pipe piena)
            var dir = Path.Combine(psi.WorkingDirectory, "devcluster", "logs");
            Directory.CreateDirectory(dir);
            var writer = new StreamWriter(Path.Combine(dir, $"riavviato-{p.Id}.log"), append: true) { AutoFlush = true };
            void Line(string? l) { if (l != null) lock (writer) { try { writer.WriteLine(l); } catch (ObjectDisposedException) { } } }
            p.OutputDataReceived += (_, e) => Line(e.Data);
            p.ErrorDataReceived += (_, e) => Line(e.Data);
            p.EnableRaisingEvents = true;
            p.Exited += (_, _) => { lock (writer) writer.Dispose(); };
            p.BeginOutputReadLine(); p.BeginErrorReadLine();
        }
        return p;
    }

    private static void Signal(int pid, bool stop)
    {
        if (OperatingSystem.IsWindows())
        {
            using var p = Process.GetProcessById(pid);
            int rc = stop ? WindowsTcp.NtSuspendProcess(p.Handle) : WindowsTcp.NtResumeProcess(p.Handle);
            if (rc != 0) throw new InvalidOperationException($"NtSuspend/ResumeProcess: 0x{rc:X8}");
        }
        else
        {
            int sig = OperatingSystem.IsMacOS() ? (stop ? 17 : 19) : (stop ? 19 : 18);   // SIGSTOP / SIGCONT
            if (Posix.kill(pid, sig) != 0) throw new InvalidOperationException($"kill({pid}, {sig}) fallito: errno {Marshal.GetLastPInvokeError()}");
        }
    }

    /// <summary>Alla chiusura del simulatore nessun nodo deve restare sospeso.</summary>
    public void Dispose()
    {
        foreach (var pid in _paused.Keys.ToList()) Resume(pid);
    }

    private static class Posix
    {
        [DllImport("libc", SetLastError = true)]
        public static extern int kill(int pid, int sig);
    }

    private static class LinuxProc
    {
        public static int? FindListeningPid(int port)
        {
            var inodes = new HashSet<string>();
            foreach (var file in new[] { "/proc/net/tcp", "/proc/net/tcp6" })
            {
                if (!File.Exists(file)) continue;
                foreach (var line in File.ReadLines(file).Skip(1))
                {
                    var f = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                    if (f.Length < 10 || f[3] != "0A") continue;                         // 0A = LISTEN
                    var local = f[1];
                    int colon = local.LastIndexOf(':');
                    if (colon < 0 || int.Parse(local[(colon + 1)..], NumberStyles.HexNumber) != port) continue;
                    inodes.Add(f[9]);
                }
            }
            if (inodes.Count == 0) return null;
            foreach (var dir in Directory.EnumerateDirectories("/proc"))
            {
                if (!int.TryParse(Path.GetFileName(dir), out var pid)) continue;
                try
                {
                    foreach (var fd in Directory.EnumerateFiles(Path.Combine(dir, "fd")))
                    {
                        var target = new FileInfo(fd).LinkTarget;
                        if (target != null && target.StartsWith("socket:[") && inodes.Contains(target[8..^1])) return pid;
                    }
                }
                catch { /* processo di un altro utente o già terminato */ }
            }
            return null;
        }
    }

    private static class WindowsTcp
    {
        private const int AF_INET = 2, TCP_TABLE_OWNER_PID_LISTENER = 3;

        [DllImport("iphlpapi.dll", SetLastError = true)]
        private static extern uint GetExtendedTcpTable(IntPtr table, ref int size, bool order, int af, int tableClass, uint reserved);

        [DllImport("ntdll.dll")] public static extern int NtSuspendProcess(IntPtr processHandle);
        [DllImport("ntdll.dll")] public static extern int NtResumeProcess(IntPtr processHandle);

        [StructLayout(LayoutKind.Sequential)]
        private struct Row { public uint State, LocalAddr, LocalPort, RemoteAddr, RemotePort, OwningPid; }

        public static int? FindListeningPid(int port)
        {
            int size = 0;
            GetExtendedTcpTable(IntPtr.Zero, ref size, false, AF_INET, TCP_TABLE_OWNER_PID_LISTENER, 0);
            var buf = Marshal.AllocHGlobal(size);
            try
            {
                if (GetExtendedTcpTable(buf, ref size, false, AF_INET, TCP_TABLE_OWNER_PID_LISTENER, 0) != 0) return null;
                int count = Marshal.ReadInt32(buf);
                var rowPtr = buf + 4;
                int rowSize = Marshal.SizeOf<Row>();
                for (int i = 0; i < count; i++, rowPtr += rowSize)
                {
                    var row = Marshal.PtrToStructure<Row>(rowPtr);
                    int p = (int)(((row.LocalPort & 0xFF) << 8) | ((row.LocalPort >> 8) & 0xFF));    // network byte order
                    if (p == port) return (int)row.OwningPid;
                }
                return null;
            }
            finally { Marshal.FreeHGlobal(buf); }
        }
    }
}
