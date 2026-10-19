# SQL Server

Tutti i nodi scrivono sullo **stesso** database. Il servizio garantisce che ogni voce del log venga scritta una sola volta, in ordine, anche durante un failover. L'alta affidabilità del database stesso (es. Always On) va gestita a parte: se il database non è raggiungibile le scritture restano in attesa e il cluster non perde nulla.

## 1. Preparare il database

Lo schema viene creato automaticamente al primo apply (`AutoCreateSchema=true`). In produzione si può invece eseguire lo script e disattivare l'opzione:

```bash
sqlcmd -S sql01 -d Ingest -i sql/001_init.sql
```

| Tabella | Contenuto |
| --- | --- |
| `raft.ApplyState` | Una riga per cluster: `LastApplied`, l'indice dell'ultima voce scritta |
| `raft.AppliedEntries` | Esito di ogni voce (status HTTP e body della risposta): fonte dei replay |
| `raft.Requests` | Prima occorrenza di ogni `RequestId`: idempotenza permanente |
| `raft.Quarantine` | Voci in quarantena, con payload e motivo |
| `raft.IngestedData` | Dati del writer di default (JSON grezzo) |

Permessi minimi dell'utente del servizio: `SELECT, INSERT, UPDATE` sullo schema `raft` e sulle tabelle applicative; `CREATE TABLE`/`CREATE SCHEMA` solo se `AutoCreateSchema=true`.

## 2. Configurare i nodi

```json
"StateMachine": { "Provider": "SqlServer", "SqlServer": { "Schema": "raft", "DataTable": "raft.IngestedData" } }
```

La stringa di connessione si passa tramite variabile d'ambiente: `RAFT_SQL_CONNECTION="Server=sql01;Database=Ingest;Integrated Security=True;TrustServerCertificate=True"`.

## 3. Scrivere nelle proprie tabelle

Il writer di default salva il JSON grezzo. Per scrivere nelle proprie tabelle si implementa `ISqlPayloadWriter`, che riceve connessione e transazione: **non** si apre una transazione propria e non si fa commit.

```csharp
public sealed class MisureWriter : ISqlPayloadWriter
{
    public async Task WriteAsync(SqlConnection c, SqlTransaction tx, LogEntry e, string clusterId, CancellationToken ct)
    {
        var m = JsonSerializer.Deserialize<Misura>(e.Payload)
                ?? throw new PayloadRejectedException("payload vuoto");
        if (m.Sensore is null) throw new PayloadRejectedException("sensore mancante");   // errore di business → 422
        await using var cmd = new SqlCommand(
            "INSERT INTO dbo.Misure (RequestId, Sensore, Valore, RicevutoUtc) VALUES (@r, @s, @v, @t)", c, tx);
        cmd.Parameters.AddWithValue("@r", Guid.Parse(e.RequestId));
        cmd.Parameters.AddWithValue("@s", m.Sensore);
        cmd.Parameters.AddWithValue("@v", m.Valore);
        cmd.Parameters.AddWithValue("@t", e.ReceivedUtc);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
```

Registrazione in `Program.cs`:

```csharp
await using var node = await NodeHost.StartAsync(args, (services, cfg) => services.AddSingleton<ISqlPayloadWriter, MisureWriter>());
```

Regole:
- La scrittura deve essere **deterministica**: dipendere solo dal contenuto della voce, mai dall'ora corrente (usare `e.ReceivedUtc`).
- **Niente effetti esterni** (email, chiamate HTTP): se la transazione viene annullata non si possono annullare.

## 4. Errori e poison entry

| Tipo di errore | Esempi | Comportamento |
| --- | --- | --- |
| Business permanente | `PayloadRejectedException`, JSON non valido, violazione di vincolo (547, 2627), troncamento (2628, 8152), conversione (245, 8114), `THROW 50001…` | Esito **Rejected** (422) registrato; il cluster prosegue |
| Transitorio | Timeout, deadlock (1205), connessione persa, failover (40613, 4060…) | Ritentato **senza limite** con backoff fino a `ApplyRetryMaxDelayMs`. Le richieste attendono; `/health/ready` del Leader risulta Not Ready |
| Altro (bug, dato imprevisto) | `NullReferenceException`, `InvalidCastException` | Ritentato `MaxApplyAttempts` volte, poi **quarantena** (422 `quarantined`); il cluster prosegue |

Per riprocessare una voce in quarantena dopo aver corretto il problema: leggere il payload da `raft.Quarantine` e reinviarlo al servizio con un **nuovo** `X-Request-ID`.

```sql
SELECT LogIndex, RequestId, Reason, CreatedUtc, CAST(Payload AS NVARCHAR(MAX)) AS PayloadJson
FROM raft.Quarantine ORDER BY LogIndex DESC;
```

## 5. Voci obsolete

Se una voce viene committata più di `MaxPendingAgeMinutes` dopo la ricezione, ad esempio perché il cluster è rimasto senza quorum per giorni e un vecchio Leader la ripropone al riavvio, **non** viene scritta. L'esito registrato è `Expired`, e un retry con lo stesso ID riceve 410. Il client non aveva mai ricevuto 200 per quella richiesta.

## 6. Backup e ripristino

`LastApplied` vive nel database. Ripristinare un backup **più vecchio** riporta indietro `LastApplied`:
- se le voci mancanti sono ancora nel log dei nodi (entro `RetainEntries`), il Leader le riapplica automaticamente;
- se sono già state compattate, il Leader registra un evento critico ("LastApplied del database precede l'inizio del log compattato") e quelle voci vanno recuperate dal backup applicativo.

Per finestre di ripristino ampie conviene aumentare `Log:RetainEntries`.
