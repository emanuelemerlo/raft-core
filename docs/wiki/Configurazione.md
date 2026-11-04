# Configurazione

La configurazione si trova nella sezione `RaftCluster` di `appsettings.json` (accanto all'eseguibile). Ogni valore si può sovrascrivere:
- con variabili d'ambiente, usando `__` come separatore: `RaftCluster__Storage__DataDir=/var/lib/raftcore/data`;
- dalla riga di comando: `--RaftCluster:RestPort=5001`.

All'avvio tutti i vincoli vengono verificati: se uno è violato il nodo **non parte** ed elenca le violazioni (codice di uscita 1).

**Segreti (mai nel repository):**
- `CLUSTER_SECRET`: chiave HMAC condivisa dai nodi, almeno 16 caratteri;
- `RAFT_SQL_CONNECTION`: stringa di connessione a SQL Server.

## Cluster e identità

| Parametro | Default | Note |
| --- | --- | --- |
| `ClusterId` | — | Isola cluster diversi nella stessa rete |
| `ClusterSize` | — | 1-7. Il quorum è `ClusterSize/2+1` |
| `MembershipMode` | `Static` | `Static` (produzione): `NodeId` e `Members` espliciti. `Bootstrap`: solo sviluppo |
| `NodeId` | — | GUID del nodo; deve comparire in `Members` |
| `Members` | — | Esattamente `ClusterSize` GUID distinti, identici su tutti i nodi |
| `BaseDirectory` | — | Base dei percorsi relativi. Vuoto: directory corrente; directory dell'eseguibile se in esecuzione come servizio |

## Endpoint di ingestione (`Endpoints`)

Il servizio espone uno o più endpoint `POST`, ciascuno per un tipo di dato con la propria tabella. Tutti condividono lo stesso log Raft, la stessa idempotenza e lo stesso contratto client (vedi [Architettura](Architettura) → endpoint multipli).

```json
"Endpoints": [
  { "Name": "misure", "Path": "/api/v1/misure", "Table": "dbo.Misure" },
  { "Name": "eventi", "Path": "/api/v1/eventi", "Table": "dbo.Eventi" }
]
```

| Campo | Note |
| --- | --- |
| `Name` | Identificatore registrato in ogni voce del log: minuscole, cifre, `-` e `_`, massimo 64 caratteri, unico. **Non va rinominato** finché ci sono voci in sospeso: per il servizio un nome nuovo è un altro tipo |
| `Path` | Path del `POST`, ad esempio `/api/v1/misure`. Unico (senza distinzione tra maiuscole e minuscole), senza parametri né `/` finale. Non può iniziare con `/internal`, `/health`, `/api/v1/cluster` o `/api/v1/metrics` |
| `Table` | Tabella `schema.tabella` del writer JSON di default (solo `Provider=SqlServer`). Facoltativa se per quel `Name` è registrato un writer personalizzato (vedi [SQL Server](SQL-Server)) |

Vincoli: almeno un endpoint, al massimo 64. La configurazione deve essere **identica su tutti i nodi**: se differisce, il cluster continua a funzionare ma lo segnala (metrica `endpointsHashMismatch`). Per aggiungere o rimuovere un endpoint vedere [Operazioni](Operazioni).

Da variabili d'ambiente gli elementi si indicano con l'indice: `RaftCluster__Endpoints__0__Name=misure`, `RaftCluster__Endpoints__0__Path=/api/v1/misure`, e così via.

## Rete

| Parametro | Default | Note |
| --- | --- | --- |
| `RestPort` | 5000 | Porta HTTP (API pubblica e RPC interne) |
| `RestPorts` | — | Porte da provare in ordine: intervallo `5001-5005`, lista `5001,5003,5007` o misto `5001-5005;5010;5012`. Il nodo usa la prima libera (prevale su `RestPort`; `RestPortRange` è un alias) |
| `AdvertiseUrl` | automatico | URL con cui gli altri nodi raggiungono questo. Supporta `{port}`. Obbligatorio dietro NAT o in container |
| `BindInterface` | — | Interfaccia o IP da usare per l'URL automatico |
| `UdpMode` | `Broadcast` | `Broadcast`, `Multicast` (consigliato con più istanze sulla stessa macchina Windows) o `Disabled` |
| `UdpDiscoveryPort` | 8888 | Condivisa tra le istanze della stessa macchina |
| `Peers` | `{}` | NodeId → URL. Opzionali con UDP funzionante; senza UDP basta che ogni nodo ne conosca uno |

## Discovery a sondaggio (`Discovery`)

Finché un membro non ha un indirizzo noto, o non risponde più, il nodo prova tutti gli host di `SeedHosts` (più gli host di `Peers`) su tutte le porte ammesse. Si registra solo l'indirizzo che ha risposto con una firma valida del membro atteso. Le istanze si trovano così anche se ciascuna ha preso una porta diversa dell'elenco, e anche senza UDP.

| Parametro | Default | Note |
| --- | --- | --- |
| `SeedHosts` | `127.0.0.1` | Host su cui cercare gli altri nodi (es. i nomi dei server) |
| `ProbePorts` | = `RestPorts` | Porte da sondare, stesso formato di `RestPorts` |
| `ProbeEnabled` | true | |
| `ProbeIntervalMs` | 2000 | Frequenza mentre manca qualche membro |
| `ProbeIdleIntervalMs` | 30000 | Frequenza minima con tutti i membri raggiungibili (in pratica non si sonda) |
| `ProbeTimeoutMs` | 500 | Timeout per singolo indirizzo |
| `UnreachableAfterFailures` | 3 | RPC fallite consecutive dopo cui un membro viene cercato di nuovo |

## Cluster di sviluppo (`DevCluster`, solo ambiente Development)

| Parametro | Default | Note |
| --- | --- | --- |
| `Instances` | 1 | Nodi totali da avviare con un solo avvio (es. F5): questo processo è il nodo 1, gli altri `Members` diventano processi figli |
| `ChildOutput` | `Auto` | `Window` (una console per nodo, Windows), `Prefix` (output nel processo principale con `[nodoN]`), `File` (`devcluster/logs`). `Auto` = Window su Windows, Prefix altrove |
| `ParentPid` | — | Impostato automaticamente nei figli: il nodo si chiude quando il padre termina |

## Tempi

| Parametro | Default | Vincolo |
| --- | --- | --- |
| `HeartbeatIntervalMs` | 250 | |
| `ElectionTimeoutMinMs` / `MaxMs` | 1000 / 1800 | Min ≥ 4×heartbeat; Max−Min ≥ 2×heartbeat |
| `LeaderLeaseMs` | 750 | 3×heartbeat ≤ lease < ElectionTimeoutMin |
| `ReplicationTimeoutMs` | 1500 | < RequestDeadlineMs |
| `RequestDeadlineMs` | 10000 | Deadline unico di una richiesta, inoltri compresi |
| `FailoverWaitMs` | 8000 | Attesa massima di un Leader, entro il deadline |
| `ShutdownGraceMs` | 5000 | Completamento delle richieste in corso allo stop |

## Limiti

| Parametro | Default | Note |
| --- | --- | --- |
| `MaxBodyBytes` | 1 MiB | Massimo 64 MiB. Oltre: 413 |
| `MaxConcurrentRequests` | 256 | Oltre: 503 con `Retry-After`, senza leggere il body |
| `RpcClockToleranceMs` | 60000 | Finestra anti-replay delle RPC: richiede orologi sincronizzati (NTP) |

## Storage locale (`Storage`)

| Parametro | Default | Note |
| --- | --- | --- |
| `DataDir` | `./data/{nodeId}` | WAL e stato Raft del nodo. Supporta `{nodeId}` e `{port}`. Una sola istanza per directory (lock) |
| `FsyncOnCommit` | true | `false` solo con `TestMode` |
| `MinFreeBytes` | 1 GiB | Sotto la soglia il nodo smette di scrivere e risulta `unhealthy` |
| `AllowEmptyState` | false | Solo al **primo** avvio di un nodo (o di un nuovo NodeId), poi rimuovere |

## Log Raft (`Log`)

| Parametro | Default | Note |
| --- | --- | --- |
| `SegmentBytes` | 64 MiB | Dimensione dei segmenti del WAL |
| `RetainEntries` | 10000 | Voci già applicate tenute per i Follower in ritardo; oltre si usa InstallSnapshot |
| `CompactionBatch` | 5000 | Voci minime per eseguire una compattazione |
| `CompactionIntervalMs` | 5000 | Frequenza del controllo |

La RAM usata dal log è proporzionale a `RetainEntries` più le voci non ancora applicate, non alla storia del cluster.

## State machine (`StateMachine`)

| Parametro | Default | Note |
| --- | --- | --- |
| `Provider` | `SharedFile` | `SqlServer` in produzione; `SharedFile` solo sviluppo/test sulla stessa macchina |
| `MaxPendingAgeMinutes` | 1440 | Una voce committata oltre questa età **non** viene scritta (esito 410). 0 = disattivato |
| `MaxApplyAttempts` | 10 | Tentativi per un errore non classificato prima della quarantena |
| `ApplyRetryMaxDelayMs` | 5000 | Backoff massimo tra i tentativi |
| `SharedFile:Directory` | `./shared-state` | Directory condivisa tra le istanze |
| `SqlServer:ConnectionString` | — | Preferibilmente via `RAFT_SQL_CONNECTION` |
| `SqlServer:Schema` | `raft` | Schema delle tabelle di servizio |
| `SqlServer:CommandTimeoutSeconds` | 30 | |
| `SqlServer:AutoCreateSchema` | true | Crea le tabelle di servizio e quelle degli endpoint (`Table`) mancanti al primo apply; in produzione si può disattivare ed eseguire gli script (vedi [SQL Server](SQL-Server)) |

## Codici di uscita

| Codice | Significato |
| --- | --- |
| 1 | Configurazione non valida |
| 2 | Avvio bloccato (stato assente senza `AllowEmptyState`, DataDir di un altro NodeId, WAL di una versione precedente) |
| 3 | DataDir già in uso da un altro processo |
| 4 | Nessuna porta libera in `RestPorts` |
