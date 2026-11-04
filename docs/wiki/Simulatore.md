# Simulatore client

`src/RaftCore.Simulator` è un'applicazione web di sviluppo che fa da client del cluster locale. Serve a:

- inviare dati in modo ciclico;
- eseguire i casi del contratto client;
- iniettare guasti sui processi dei nodi;
- verificare che nessuna scrittura vada persa o sia duplicata.

L'interfaccia è su **http://localhost:5100**. Non fa parte del servizio e non viene pubblicata nei rilasci.

## Avvio da Visual Studio

La soluzione contiene il profilo di avvio multiplo **Cluster + Simulatore** (file `RaftCore.slnLaunch`):

1. scegliere **Cluster + Simulatore** nel menu a tendina dei profili, accanto al pulsante di avvio;
2. premere **F5**:
   - il nodo 1 va in debug e avvia i nodi 2 e 3 (vedi [Avvio rapido](Avvio-rapido));
   - il simulatore parte anche lui in debug e apre il browser;
3. **Shift+F5** chiude tutto.

Il profilo **Cluster + Simulatore (simulatore senza debug)** è identico, ma lascia sotto debugger solo il nodo 1.

I profili multipli richiedono Visual Studio 2022 **17.11** o successivo; nella 17.11 la funzione va abilitata tra le funzionalità in anteprima. Con versioni precedenti si usa *Soluzione → Configura progetti di avvio → Progetti di avvio multipli*:

- `RaftCore`: Avvia, profilo *Cluster 3 nodi (debug nodo 1)*;
- `RaftCore.Simulator`: Avvia, profilo *Simulatore*.

Da riga di comando:

```bash
cd src/RaftCore && dotnet run --launch-profile "Cluster 3 nodi (debug nodo 1)"
# in un altro terminale
cd src/RaftCore.Simulator && dotnet run --launch-profile Simulatore
```

## Cosa si vede

**Cluster.** Il simulatore sonda `127.0.0.1` sulle porte `5001-5010` (le stesse di `RestPorts` nella configurazione di sviluppo) e mostra una scheda per nodo con:

- ruolo, term, Leader noto;
- `commitIndex` / `lastApplied`, indice del log e dello snapshot;
- endpoint configurati sul nodo, con un avviso se differiscono tra i nodi;
- stato di `/health/ready`;
- PID del processo.

Il nodo 1 ha il badge **debug**.

**Invio ciclico.** Gli endpoint sono letti da `GET /api/v1/cluster/status` dei nodi (campo `endpoints`), quindi il simulatore ne gestisce quanti ne espone il cluster, senza configurarli. Ogni operazione:

- sceglie un endpoint in proporzione ai **pesi**;
- invia il JSON del **modello** di quell'endpoint;
- resta sullo stesso endpoint in tutti i ritenti.

Il body contiene sempre i campi usati dalla verifica, più quelli del modello. Con i modelli d'esempio, un'operazione verso `misure` invia:

```json
{"seq": 42, "timestamp": "2026-11-04T14:32:25.1188645+01:00", "source": "simulatore", "runId": "6f2bf38c-…",
 "sensore": "S-02", "unita": "°C", "valore": 91.28}
```

`seq` è progressivo, `runId` identifica la sessione del simulatore. Si possono impostare:

- **intervallo** in ms;
- **destinazione**: tutti a rotazione, solo il Leader, solo i Follower per provare l'inoltro, oppure un nodo preciso;
- **riempimento**: un campo `pad` per simulare payload più grandi.

Ci sono anche *Invia una* e *Raffica* (N operazioni contemporanee).

**Endpoint.** La tabella *Endpoint* ha una riga per endpoint con:

- il **peso**, da 0 a 100: 0 esclude l'endpoint, e un endpoint nuovo parte con peso 1. Con pesi 3 e 1, su quattro operazioni tre vanno al primo endpoint e una al secondo;
- la **quota** di traffico che ne risulta;
- contatori e latenze (p50/p95) dell'endpoint;
- il pulsante **Modello**, che apre l'editor del JSON dell'endpoint.

Il modello è un oggetto JSON, anche annidato, i cui valori stringa possono contenere segnaposto:

| Segnaposto | Valore |
| --- | --- |
| `{seq}` | numero progressivo dell'operazione |
| `{now}` | istante di invio (ISO 8601) |
| `{uuid}` | GUID nuovo |
| `{random}` | decimale da 0 a 100, con 2 cifre |
| `{randomInt}` | intero da 0 a 1000 |
| `{choice:a\|b\|c}` | uno dei valori elencati |

Una stringa composta solo da `{seq}`, `{random}` o `{randomInt}` diventa un numero JSON. I valori casuali dipendono da `seq`: la stessa operazione produce sempre gli stessi byte. I campi `seq`, `timestamp`, `source`, `runId`, `scenario` e `pad` sono riservati. *Anteprima* mostra un body d'esempio. I modelli modificati dall'interfaccia valgono fino al riavvio del simulatore; quelli iniziali si impostano in `Simulator:Payloads` (vedi Configurazione).

Il simulatore si comporta come un client conforme al [Contratto client](Contratto-client):

- `X-Request-ID` generato una volta per operazione;
- stessi byte del body a ogni ritento;
- retry su errore di rete, 503 e 504 con backoff esponenziale e jitter, entro 60 s;
- mai retry su 4xx;
- su errore di rete prova un altro nodo.

Contatori e latenze (p50/p95/max) sono complessivi; quelli di ogni endpoint stanno nella tabella *Endpoint*. Il grafico degli ultimi 120 s mostra tutti gli endpoint oppure, con il selettore *Grafico*, uno solo. La tabella delle ultime operazioni riporta l'endpoint e il percorso dei tentativi, ad esempio `5002✕ → 5003:200`. *Esporta CSV* scarica lo storico completo, con la colonna `endpoint`.

## Guasti

| Azione | Effetto | Scenario della specifica |
| --- | --- | --- |
| **Kill** (di un nodo o del Leader) | Terminazione immediata del processo (TerminateProcess / SIGKILL), senza shutdown ordinato | T5, T6, T20 |
| **Pausa** per N secondi | Processo congelato (NtSuspendProcess / SIGSTOP) e poi ripreso | T14 |
| **Riavvia** | Rilancia un nodo terminato con la stessa riga di comando: si riallinea dal Leader | T11, T16 |

Dopo un guasto, con l'invio ciclico attivo, il simulatore misura il **tempo di failover**: dal guasto al primo `200` di un'operazione iniziata dopo il guasto (REQ-FOV-1). Il valore compare sotto il grafico e negli eventi.

Il simulatore agisce solo su processi `RaftCore` individuati dalla porta in ascolto. Il **nodo 1** è il processo sotto debugger e il padre degli altri nodi, quindi:

- se viene terminato, si chiudono anche i nodi 2 e 3 e il cluster si riavvia con F5;
- per provare il failover quando il Leader è il nodo 1 conviene usare la **pausa**, oppure aspettare che diventi Leader un altro nodo.

## Scenari

Ogni scenario si esegue con un clic (o *Esegui tutti*) e riporta l'esito atteso, i passi e il verdetto. Il selettore *Endpoint* sceglie su quale endpoint eseguirli; con **tutti** ogni scenario gira su ciascun endpoint e mostra un verdetto per endpoint. I body seguono il modello dell'endpoint, così un writer che valida il proprio schema non rifiuta le richieste degli scenari; queste scritture hanno un `runId` proprio e non entrano nella verifica della sessione. Fa eccezione «Path non configurato», che non dipende dall'endpoint e gira una sola volta.

| Scenario | Atteso |
| --- | --- |
| Retry con stesso ID e stesso body | 200, poi 200 con `X-Idempotent-Replay: true`; una sola scrittura (T7) |
| Stesso ID, body diverso per uno spazio | 200, poi 422 (T9, T42) |
| 10 richieste identiche in parallelo | 10 × 200, una sola voce applicata (T33) |
| Scrittura tramite un Follower | 200, nessun header interno nella risposta (T4) |
| Senza `X-Request-ID` / ID non UUID / JSON non valido | 400 (T34) |
| Body oltre `MaxBodyBytes` | 413, nessuna scrittura (T34) |
| `X-Forward-Hops` e `X-Forwarded-By` inviati dal client | 200: header non firmati ignorati |
| Body senza `Content-Type` | 200 |
| Stesso ID su un altro endpoint | 200, poi 422 sull'endpoint successivo; una sola scrittura (T59) |
| Path non configurato | 404 (T60) |

## Verifica delle scritture

*Verifica ora* legge `applied.log` della state machine SharedFile (`src/RaftCore/devcluster/shared-state/applied.log`) e lo confronta con le operazioni della sessione:

- **violazioni**:
  - un `seq` o un `RequestId` applicato più volte;
  - un'operazione confermata con 200 ma assente;
  - un'operazione applicata con un endpoint diverso da quello a cui è stata inviata;
- **ammesso** (REQ-API-3): operazioni abbandonate dal client ma scritte comunque;
- totali per esito (applicate, deduplicate, rifiutate, scadute, in quarantena, no-op), voci applicate per endpoint e continuità degli indici del log.

Con la state machine SQL Server la verifica va fatta sul database, ad esempio per ogni tabella di endpoint e tra endpoint diversi:

```sql
SELECT RequestId, COUNT(*) FROM dbo.Misure GROUP BY RequestId HAVING COUNT(*) > 1;
SELECT RequestId FROM dbo.Misure INTERSECT SELECT RequestId FROM dbo.Eventi;   -- deve essere vuoto
```

## Configurazione

`src/RaftCore.Simulator/appsettings.json`, sezione `Simulator`:

| Parametro | Default | Note |
| --- | --- | --- |
| `Hosts` | `127.0.0.1` | Host da sondare |
| `Ports` | `5001-5010` | Stesso formato di `RestPorts` |
| `StatusPollMs` / `StatusTimeoutMs` | 1000 / 500 | Sondaggio di `/api/v1/cluster/status` e `/health/ready` |
| `DefaultIntervalMs` / `DefaultTarget` | 200 / `RoundRobin` | Valori iniziali dell'invio ciclico |
| `DefaultEndpointWeights` | {} | Pesi iniziali per `Name` (0-100, 0 = escluso); un endpoint non elencato ha peso 1 |
| `Payloads` | modelli di `misure` ed `eventi` | Modello iniziale per `Name`: campo → valore con segnaposto. Numeri e `true`/`false` restano tipizzati; per oggetti annidati usare l'editor |
| `MaxInFlight` | 64 | Oltre, i tick dell'invio ciclico vengono saltati e contati |
| `RequestTimeoutMs` | 15000 | Timeout per tentativo (REQ-CLI-2) |
| `RetryBudgetMs` | 60000 | Tempo massimo dei ritenti di un'operazione |
| `MaxBodyBytes` | 1048576 | Usato dallo scenario «Body oltre MaxBodyBytes» |
| `AppliedLogPath` | `../RaftCore/devcluster/shared-state/applied.log` | Relativo alla cartella del progetto del simulatore |
| `NodeWorkingDirectory` | `../RaftCore` | Directory di lavoro dei nodi riavviati (Windows) |
| `NodeEnvironment` | — | Variabili ereditate dai nodi riavviati. In Development contiene `ASPNETCORE_ENVIRONMENT` e il segreto di sviluppo |

API del simulatore, utili anche per script: `GET /sim/state`, `POST /sim/generator/start|stop|one|burst|reset`, `POST /sim/scenarios/{id}?endpoint=<nome>|*` (un esito per endpoint), `PUT /sim/payloads/{endpoint}` (body: il modello JSON), `POST /sim/payloads/{endpoint}/preview`, `POST /sim/nodes/{nodeId}/kill|pause|resume|restart`, `POST /sim/leader/kill|pause`, `POST /sim/verify`, `GET /sim/ops.csv`.
