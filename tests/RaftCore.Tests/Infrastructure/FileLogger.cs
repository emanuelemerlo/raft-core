using Microsoft.Extensions.Logging;

namespace RaftCore.Tests.Infrastructure;

/// <summary>Log di ogni nodo su file (diagnostica dei test di cluster).</summary>
public sealed class FileLoggerProvider : ILoggerProvider
{
    private readonly StreamWriter _w;
    private readonly object _l = new();
    public FileLoggerProvider(string path) { _w = new StreamWriter(new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { AutoFlush = true }; }
    public ILogger CreateLogger(string category) => new L(this, category);
    public void Dispose() { lock (_l) _w.Dispose(); }
    private sealed class L : ILogger
    {
        private readonly FileLoggerProvider _p; private readonly string _c;
        public L(FileLoggerProvider p, string c) { _p = p; _c = c.Split('.').Last(); }
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel level) => level >= LogLevel.Debug && (_c is "RaftNode" or "HttpPeerTransport" or "ClientApi" || level >= LogLevel.Information);
        public void Log<TState>(LogLevel level, EventId id, TState state, Exception? ex, Func<TState, Exception?, string> f)
        {
            if (!IsEnabled(level)) return;
            lock (_p._l) { try { _p._w.WriteLine($"{DateTime.Now:HH:mm:ss.fff} {level,-11} {_c}: {f(state, ex)}{(ex != null ? " " + ex.Message : "")}"); } catch (ObjectDisposedException) { } }
        }
    }
}
