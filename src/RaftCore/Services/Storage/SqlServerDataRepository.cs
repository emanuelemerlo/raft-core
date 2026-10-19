using System.Data;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using RaftCore.Abstractions;
using RaftCore.Configuration;
using RaftCore.Models;

namespace RaftCore.Services.Storage;

/// <summary>
/// Scrittura applicativa di una voce dentro la transazione dell'apply. Implementare questa interfaccia per scrivere
/// nelle proprie tabelle. Per un errore di business permanente lanciare <see cref="PayloadRejectedException"/>
/// (oppure lasciare propagare un errore SQL di vincolo/conversione): la voce viene registrata come Rejected.
/// </summary>
public interface ISqlPayloadWriter
{
    Task WriteAsync(SqlConnection connection, SqlTransaction transaction, LogEntry entry, string clusterId, CancellationToken ct);
}

/// <summary>Writer di default: salva il JSON grezzo in StateMachine:SqlServer:DataTable.</summary>
public sealed class JsonPayloadWriter : ISqlPayloadWriter
{
    private readonly string _table;
    private readonly int _timeout;
    public JsonPayloadWriter(SqlServerConfig cfg) { _table = SqlName.Quote(cfg.DataTable); _timeout = cfg.CommandTimeoutSeconds; }

    public async Task WriteAsync(SqlConnection c, SqlTransaction tx, LogEntry e, string clusterId, CancellationToken ct)
    {
        string json;
        try
        {
            using var doc = JsonDocument.Parse(e.Payload);
            if (doc.RootElement.ValueKind != JsonValueKind.Object) throw new PayloadRejectedException("il payload deve essere un oggetto JSON");
            json = Encoding.UTF8.GetString(e.Payload);
        }
        catch (JsonException ex) { throw new PayloadRejectedException("payload JSON non valido: " + ex.Message, ex); }

        await using var cmd = new SqlCommand(
            $"INSERT INTO {_table} (ClusterId, RequestId, LogIndex, ReceivedUtc, Payload) VALUES (@c, @r, @i, @t, @p)", c, tx)
        { CommandTimeout = _timeout };
        cmd.Parameters.Add("@c", SqlDbType.NVarChar, 100).Value = clusterId;
        cmd.Parameters.Add("@r", SqlDbType.UniqueIdentifier).Value = Guid.Parse(e.RequestId);
        cmd.Parameters.Add("@i", SqlDbType.BigInt).Value = e.Index;
        cmd.Parameters.Add("@t", SqlDbType.DateTime2).Value = e.ReceivedUtc;
        cmd.Parameters.Add("@p", SqlDbType.NVarChar, -1).Value = json;
        await cmd.ExecuteNonQueryAsync(ct);
    }
}

/// <summary>
/// State machine su SQL Server condiviso. Ogni apply è UNA transazione che:
/// 1) blocca la riga di ApplyState (UPDLOCK, HOLDLOCK) e verifica LastApplied = LogIndex - 1;
/// 2) decide l'esito (duplicato, conflitto, scaduta, scrittura applicativa) e lo registra in AppliedEntries/Requests;
/// 3) avanza LastApplied.
/// Un ex Leader o un nodo rientrato dopo settimane trova LastApplied già avanzato e non riscrive nulla.
/// </summary>
public sealed class SqlServerDataRepository : IDataRepository
{
    // Errori che non si risolvono ritentando: violazioni di vincolo, troncamenti, conversioni, NULL, errori utente (RAISERROR >= 50000).
    private static readonly HashSet<int> PermanentNumbers = new() { 515, 547, 2601, 2627, 2628, 8152, 245, 8114, 8115, 241, 242, 220, 13609, 3609 };
    // Errori transitori noti: timeout, deadlock, connessione, failover, risorse.
    private static readonly HashSet<int> TransientNumbers = new()
    { -2, -1, 2, 20, 53, 64, 121, 233, 1205, 1222, 4060, 4221, 10053, 10054, 10060, 10928, 10929, 40143, 40197, 40501, 40540, 40613, 49918, 49919, 49920, 615, 926, 18456 };

    private readonly SqlServerConfig _cfg;
    private readonly string _clusterId, _nodeId, _s;
    private readonly ISqlPayloadWriter _writer;

    public SqlServerDataRepository(SqlServerConfig cfg, string clusterId, string nodeId, ISqlPayloadWriter? writer = null)
    {
        _cfg = cfg; _clusterId = clusterId; _nodeId = nodeId; _s = "[" + cfg.Schema + "]";
        _writer = writer ?? new JsonPayloadWriter(cfg);
    }

    public static string SchemaScript(string schema)
    {
        using var s = typeof(SqlServerDataRepository).Assembly.GetManifestResourceStream("RaftCore.sql.001_init.sql")
            ?? throw new InvalidOperationException("Script SQL incorporato mancante.");
        using var r = new StreamReader(s);
        return r.ReadToEnd().Replace("[raft]", "[" + schema + "]").Replace("N'raft'", "N'" + schema + "'");
    }

    public async Task InitializeAsync(CancellationToken ct)
    {
        await using var c = await OpenAsync(ct);
        if (_cfg.AutoCreateSchema)
        {
            foreach (var batch in SplitBatches(SchemaScript(_cfg.Schema)))
            {
                await using var cmd = new SqlCommand(batch, c) { CommandTimeout = _cfg.CommandTimeoutSeconds };
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }
        await using var ins = new SqlCommand(
            $"IF NOT EXISTS (SELECT 1 FROM {_s}.[ApplyState] WHERE ClusterId=@c) INSERT INTO {_s}.[ApplyState](ClusterId, LastApplied, UpdatedUtc, UpdatedBy) VALUES (@c, 0, SYSUTCDATETIME(), @n)", c)
        { CommandTimeout = _cfg.CommandTimeoutSeconds };
        ins.Parameters.Add("@c", SqlDbType.NVarChar, 100).Value = _clusterId;
        ins.Parameters.Add("@n", SqlDbType.NVarChar, 64).Value = _nodeId;
        await ins.ExecuteNonQueryAsync(ct);
    }

    internal static IEnumerable<string> SplitBatches(string script) =>
        System.Text.RegularExpressions.Regex.Split(script, @"^\s*GO\s*$", System.Text.RegularExpressions.RegexOptions.Multiline | System.Text.RegularExpressions.RegexOptions.IgnoreCase)
            .Select(b => b.Trim()).Where(b => b.Length > 0 && !b.Split('\n').All(l => l.TrimStart().StartsWith("--") || l.Trim().Length == 0));

    public async Task<long> GetLastAppliedIndexAsync(CancellationToken ct)
    {
        await using var c = await OpenAsync(ct);
        await using var cmd = Cmd($"SELECT LastApplied FROM {_s}.[ApplyState] WHERE ClusterId=@c", c, null);
        var v = await cmd.ExecuteScalarAsync(ct);
        return v is long l ? l : 0;
    }

    public async Task<ApplyOutcome?> GetOutcomeAsync(string requestId, CancellationToken ct)
    {
        if (!Guid.TryParse(requestId, out var rid)) return null;
        await using var c = await OpenAsync(ct);
        await using var cmd = Cmd($@"SELECT a.LogIndex, a.Status, a.HttpStatus, a.ResponseBody FROM {_s}.[Requests] r
            JOIN {_s}.[AppliedEntries] a ON a.ClusterId = r.ClusterId AND a.LogIndex = r.LogIndex
            WHERE r.ClusterId=@c AND r.RequestId=@r", c, null);
        cmd.Parameters.Add("@r", SqlDbType.UniqueIdentifier).Value = rid;
        await using var rd = await cmd.ExecuteReaderAsync(ct);
        return await rd.ReadAsync(ct) ? ReadOutcome(rd).AsReplay() : null;
    }

    public async Task<ApplyOutcome> ApplyAsync(LogEntry e, ApplyContext ctx, CancellationToken ct)
    {
        Exception? business = null;
        await using (var c = await OpenAsync(ct))
        await using (var tx = (SqlTransaction)await c.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct))
        {
            var pre = await CheckCursorAsync(c, tx, e.Index, ct);
            if (pre != null) { await tx.CommitAsync(ct); return pre; }

            ApplyOutcome outcome;
            if (e.EntryType == LogEntryType.NoOp) outcome = Outcomes.NoOp(e.Index);
            else
            {
                var first = await FindRequestAsync(c, tx, e.RequestId, ct);
                if (first != null)
                {
                    outcome = first.Value.Hash != e.PayloadHash
                        ? Outcomes.Conflict(e.Index, e.RequestId)
                        : first.Value.Outcome with { Status = ApplyStatus.Duplicate, LogIndex = e.Index, Replay = true };
                }
                else if (ctx.MaxPendingAge > TimeSpan.Zero && ctx.NowUtc - e.ReceivedUtc > ctx.MaxPendingAge)
                    outcome = Outcomes.Expired(e.Index, e.RequestId, e.ReceivedUtc);
                else
                {
                    try
                    {
                        await _writer.WriteAsync(c, tx, e, _clusterId, ct);
                        outcome = Outcomes.Applied(e.Index, e.RequestId);
                    }
                    catch (Exception ex) when (IsPermanent(ex)) { business = ex; outcome = null!; }
                }
            }
            if (business == null)
            {
                await RecordAsync(c, tx, e, outcome, ct);
                await tx.CommitAsync(ct);
                return outcome;
            }
            await tx.RollbackAsync(ct);   // la transazione può essere compromessa: si registra il rifiuto in una nuova
        }
        return await RecordTerminalAsync(e, Outcomes.Rejected(e.Index, e.RequestId, business.Message), null, ct);
    }

    public Task<ApplyOutcome> QuarantineAsync(LogEntry e, string reason, ApplyContext ctx, CancellationToken ct) =>
        RecordTerminalAsync(e, Outcomes.Quarantined(e.Index, e.RequestId, reason), reason, ct);

    /// <summary>Registra un esito terminale senza scrivere dati applicativi (rifiuto o quarantena), con la stessa condizione su LastApplied.</summary>
    private async Task<ApplyOutcome> RecordTerminalAsync(LogEntry e, ApplyOutcome outcome, string? quarantineReason, CancellationToken ct)
    {
        await using var c = await OpenAsync(ct);
        await using var tx = (SqlTransaction)await c.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
        var pre = await CheckCursorAsync(c, tx, e.Index, ct);
        if (pre != null) { await tx.CommitAsync(ct); return pre; }
        if (quarantineReason != null)
        {
            await using var q = Cmd($"INSERT INTO {_s}.[Quarantine](ClusterId, LogIndex, RequestId, Payload, Reason, CreatedUtc) VALUES (@c, @i, @r, @p, @why, SYSUTCDATETIME())", c, tx);
            q.Parameters.Add("@i", SqlDbType.BigInt).Value = e.Index;
            q.Parameters.Add("@r", SqlDbType.UniqueIdentifier).Value = Guid.TryParse(e.RequestId, out var g) ? g : DBNull.Value;
            q.Parameters.Add("@p", SqlDbType.VarBinary, -1).Value = e.Payload;
            q.Parameters.Add("@why", SqlDbType.NVarChar, 2000).Value = Outcomes.Truncate(quarantineReason);
            await q.ExecuteNonQueryAsync(ct);
        }
        await RecordAsync(c, tx, e, outcome, ct);
        await tx.CommitAsync(ct);
        return outcome;
    }

    /// <summary>Blocca ApplyState: ritorna l'esito già registrato se la voce è già applicata, lancia ApplyGapException se manca un predecessore.</summary>
    private async Task<ApplyOutcome?> CheckCursorAsync(SqlConnection c, SqlTransaction tx, long index, CancellationToken ct)
    {
        await using var cmd = Cmd($"SELECT LastApplied FROM {_s}.[ApplyState] WITH (UPDLOCK, HOLDLOCK) WHERE ClusterId=@c", c, tx);
        var v = await cmd.ExecuteScalarAsync(ct);
        long last = v is long l ? l : throw new InvalidOperationException("Riga ApplyState assente: eseguire InitializeAsync.");
        if (index > last + 1) throw new ApplyGapException(last, index);
        if (index <= last)
        {
            await using var q = Cmd($"SELECT LogIndex, Status, HttpStatus, ResponseBody FROM {_s}.[AppliedEntries] WHERE ClusterId=@c AND LogIndex=@i", c, tx);
            q.Parameters.Add("@i", SqlDbType.BigInt).Value = index;
            await using var rd = await q.ExecuteReaderAsync(ct);
            return await rd.ReadAsync(ct) ? ReadOutcome(rd).AsReplay() : Outcomes.NoOp(index).AsReplay();
        }
        return null;
    }

    private async Task<(string Hash, ApplyOutcome Outcome)?> FindRequestAsync(SqlConnection c, SqlTransaction tx, string requestId, CancellationToken ct)
    {
        if (!Guid.TryParse(requestId, out var rid)) return null;
        await using var cmd = Cmd($@"SELECT r.PayloadHash, a.LogIndex, a.Status, a.HttpStatus, a.ResponseBody FROM {_s}.[Requests] r
            JOIN {_s}.[AppliedEntries] a ON a.ClusterId = r.ClusterId AND a.LogIndex = r.LogIndex
            WHERE r.ClusterId=@c AND r.RequestId=@r", c, tx);
        cmd.Parameters.Add("@r", SqlDbType.UniqueIdentifier).Value = rid;
        await using var rd = await cmd.ExecuteReaderAsync(ct);
        if (!await rd.ReadAsync(ct)) return null;
        var hash = rd.GetString(0);
        var o = new ApplyOutcome((ApplyStatus)rd.GetByte(2), rd.GetInt16(3), rd.IsDBNull(4) ? Array.Empty<byte>() : (byte[])rd[4], rd.GetInt64(1));
        return (hash, o);
    }

    private async Task RecordAsync(SqlConnection c, SqlTransaction tx, LogEntry e, ApplyOutcome o, CancellationToken ct)
    {
        Guid? rid = Guid.TryParse(e.RequestId, out var g) ? g : null;
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO {_s}.[AppliedEntries](ClusterId, LogIndex, RequestId, Status, HttpStatus, ResponseBody, AppliedUtc) VALUES (@c, @i, @r, @st, @http, @body, SYSUTCDATETIME());");
        // la prima occorrenza di un RequestId (con qualunque esito terminale) entra nell'indice di idempotenza
        if (e.EntryType == LogEntryType.Data && rid != null && o.Status is ApplyStatus.Applied or ApplyStatus.Rejected or ApplyStatus.Expired or ApplyStatus.Quarantined)
            sb.Append($"INSERT INTO {_s}.[Requests](ClusterId, RequestId, PayloadHash, LogIndex, Status, ReceivedUtc) VALUES (@c, @r, @h, @i, @st, @rcv);");
        sb.Append($"UPDATE {_s}.[ApplyState] SET LastApplied=@i, UpdatedUtc=SYSUTCDATETIME(), UpdatedBy=@n WHERE ClusterId=@c AND LastApplied=@i - 1;");
        sb.Append("IF @@ROWCOUNT <> 1 THROW 51000, 'ApplyState: LastApplied modificato da un altro applicatore', 1;");
        await using var cmd = Cmd(sb.ToString(), c, tx);
        cmd.Parameters.Add("@i", SqlDbType.BigInt).Value = e.Index;
        cmd.Parameters.Add("@r", SqlDbType.UniqueIdentifier).Value = (object?)rid ?? DBNull.Value;
        cmd.Parameters.Add("@st", SqlDbType.TinyInt).Value = (byte)o.Status;
        cmd.Parameters.Add("@http", SqlDbType.SmallInt).Value = (short)o.HttpStatus;
        cmd.Parameters.Add("@body", SqlDbType.VarBinary, -1).Value = o.Body;
        cmd.Parameters.Add("@h", SqlDbType.Char, 64).Value = e.PayloadHash;
        cmd.Parameters.Add("@rcv", SqlDbType.DateTime2).Value = e.ReceivedUtc == default ? DateTime.UtcNow : e.ReceivedUtc;
        cmd.Parameters.Add("@n", SqlDbType.NVarChar, 64).Value = _nodeId;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static ApplyOutcome ReadOutcome(SqlDataReader rd) =>
        new((ApplyStatus)rd.GetByte(1), rd.GetInt16(2), rd.IsDBNull(3) ? Array.Empty<byte>() : (byte[])rd[3], rd.GetInt64(0));

    private SqlCommand Cmd(string sql, SqlConnection c, SqlTransaction? tx)
    {
        var cmd = new SqlCommand(sql, c, tx) { CommandTimeout = _cfg.CommandTimeoutSeconds };
        cmd.Parameters.Add("@c", SqlDbType.NVarChar, 100).Value = _clusterId;
        return cmd;
    }

    private async Task<SqlConnection> OpenAsync(CancellationToken ct)
    {
        var c = new SqlConnection(_cfg.ConnectionString);
        try { await c.OpenAsync(ct); return c; }
        catch { await c.DisposeAsync(); throw; }
    }

    // ------------------------------------------------------------------ classificazione errori (poison entry)
    internal static bool IsPermanent(Exception ex) => ex switch
    {
        PayloadRejectedException or JsonException or FormatException => true,
        SqlException s => s.Errors.Cast<SqlError>().Any(e => PermanentNumbers.Contains(e.Number) || (e.Number >= 50000 && e.Number != 51000 && e.Class == 16)),
        _ => false
    };

    public bool IsTransient(Exception ex) => ex switch
    {
        SqlException s => s.Class >= 20 || s.Errors.Cast<SqlError>().Any(e => TransientNumbers.Contains(e.Number) || e.Number == 51000),
        TimeoutException or IOException or SocketException => true,
        InvalidOperationException ioe => ioe.Message.Contains("pool", StringComparison.OrdinalIgnoreCase)
                                         || ioe.Message.Contains("connection", StringComparison.OrdinalIgnoreCase),
        _ => ex.InnerException != null && IsTransient(ex.InnerException)
    };
}
