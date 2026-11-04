# SQL Server

Tutti i nodi scrivono sullo **stesso** database. Il servizio garantisce che ogni voce del log venga scritta una sola volta, in ordine, anche durante un failover. L'alta affidabilità del database stesso (es. Always On) va gestita a parte: se il database non è raggiungibile le scritture restano in attesa e il cluster non perde nulla.

## 1. Preparare il database

Le tabelle vengono create automaticamente al primo apply (`AutoCreateSchema=true`): quelle di servizio nello schema `Schema` e quella di ogni endpoint (`Endpoints[].Table`). In produzione si possono invece eseguire gli script e disattivare l'opzione:

```bash
sqlcmd -S sql01 -d Ingest -i sql/001_init.sql
# una volta per ogni endpoint con il writer JSON di default
sqlcmd -S sql01 -d Ingest -i sql/002_endpoint_table.sql -v Schema=dbo Table=Misure
sqlcmd -S sql01 -d Ingest -i sql/002_endpoint_table.sql -v Schema=dbo Table=Eventi
```

| Tabella | Contenuto |
| --- | --- |
| `raft.SchemaVersion` | Versione dello schema delle tabelle di servizio (attuale: 2) |
| `raft.ApplyState` | Una riga per cluster: `LastApplied`, l'indice dell'ultima voce scritta |
| `raft.AppliedEntries` | Esito di ogni voce, con l'endpoint (`PayloadType`), lo status HTTP e il body della risposta: fonte dei replay |
| `raft.Requests` | Prima occorrenza di ogni `RequestId`, con l'endpoint: idempotenza permanente |
| `raft.Quarantine` | Voci in quarantena, con endpoint, payload e motivo |
| `Endpoints[].Table` | Una tabella per endpoint: dati del writer di default (JSON grezzo) |

Ogni tabella di endpoint ha le colonne `Id`, `ClusterId`, `RequestId`, `LogIndex`, `ReceivedUtc` e `Payload` (JSON, `ISJSON = 1`), con `RequestId` unico per cluster.

Permessi minimi dell'utente del servizio: `SELECT, INSERT, UPDATE` sullo schema `raft` e sulle tabelle degli endpoint; `CREATE TABLE`/`CREATE SCHEMA` solo se `AutoCreateSchema=true`.

**Versione dello schema.** Il servizio non migra database di versioni precedenti. Se trova tabelle di servizio senza `SchemaVersion` o con una versione diversa, il Leader non applica nulla: `GET /api/v1/cluster/status` riporta l'errore in `stateMachineError` e `/health/ready` risponde 503. Si usa un database (o uno `Schema`) nuovo, oppure si eliminano le tabelle di servizio dopo averne salvato i dati.

## 2. Configurare i nodi

```json
"StateMachine": { "Provider": "SqlServer", "SqlServer": { "Schema": "raft" } },
"Endpoints": [
  { "Name": "misure", "Path": "/api/v1/misure", "Table": "dbo.Misure" },
  { "Name": "eventi", "Path": "/api/v1/eventi", "Table": "dbo.Eventi" }
]
```

La stringa di connessione si passa tramite variabile d'ambiente: `RAFT_SQL_CONNECTION="Server=sql01;Database=Ingest;Integrated Security=True;TrustServerCertificate=True"`.

## 3. Scrivere nelle proprie tabelle

Il writer di default salva il JSON grezzo nella `Table` dell'endpoint. Per scrivere nelle proprie tabelle si implementa `ISqlPayloadWriter` e lo si registra **con il `Name` dell'endpoint**. Il writer riceve connessione e transazione: **non** si apre una transazione propria e non si fa commit.

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
await using var node = await NodeHost.StartAsync(args, (services, cfg) => services.AddPayloadWriter<MisureWriter>("misure"));
```

Gli endpoint senza un writer registrato usano il writer JSON di default, che richiede `Table`. Un endpoint senza writer e senza `Table` impedisce l'avvio (codice 1).

Regole:
- La scrittura deve essere **deterministica**: dipendere solo dal contenuto della voce, mai dall'ora corrente (usare `e.ReceivedUtc`).
- **Niente effetti esterni** (email, chiamate HTTP): se la transazione viene annullata non si possono annullare.
- Il writer riceve solo le voci del proprio endpoint (`e.PayloadType`); un writer può essere registrato per più nomi.

## 4. Errori e poison entry

| Tipo di errore | Esempi | Comportamento |
| --- | --- | --- |
| Business permanente | `PayloadRejectedException`, JSON non valido, violazione di vincolo (547, 2627), troncamento (2628, 8152), conversione (245, 8114), `THROW 50001…` | Esito **Rejected** (422) registrato; il cluster prosegue |
| Transitorio | Timeout, deadlock (1205), connessione persa, failover (40613, 4060…) | Ritentato **senza limite** con backoff fino a `ApplyRetryMaxDelayMs`. Le richieste attendono; `/health/ready` del Leader risulta Not Ready |
| Endpoint sconosciuto all'applicatore | Voce di un `PayloadType` senza writer su questo nodo (configurazione disallineata, endpoint rimosso con voci in sospeso) | Esito **Rejected** (422) registrato; metrica `applyUnknownType`; il cluster prosegue |
| Altro (bug, dato imprevisto) | `NullReferenceException`, `InvalidCastException` | Ritentato `MaxApplyAttempts` volte, poi **quarantena** (422 `quarantined`); il cluster prosegue |

Per riprocessare una voce in quarantena dopo aver corretto il problema: leggere il payload da `raft.Quarantine` e reinviarlo all'endpoint indicato da `PayloadType` con un **nuovo** `X-Request-ID`.

```sql
SELECT LogIndex, RequestId, PayloadType, Reason, CreatedUtc, CAST(Payload AS NVARCHAR(MAX)) AS PayloadJson
FROM raft.Quarantine ORDER BY LogIndex DESC;
```

## 5. Voci obsolete

Se una voce viene committata più di `MaxPendingAgeMinutes` dopo la ricezione, ad esempio perché il cluster è rimasto senza quorum per giorni e un vecchio Leader la ripropone al riavvio, **non** viene scritta. L'esito registrato è `Expired`, e un retry con lo stesso ID riceve 410. Il client non aveva mai ricevuto 200 per quella richiesta.

## 6. Backup e ripristino

`LastApplied` vive nel database. Ripristinare un backup **più vecchio** riporta indietro `LastApplied`:
- se le voci mancanti sono ancora nel log dei nodi (entro `RetainEntries`), il Leader le riapplica automaticamente;
- se sono già state compattate, il Leader registra un evento critico ("LastApplied del database precede l'inizio del log compattato") e quelle voci vanno recuperate dal backup applicativo.

Per finestre di ripristino ampie conviene aumentare `Log:RetainEntries`.
