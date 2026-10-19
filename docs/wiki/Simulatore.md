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
- stato di `/health/ready`;
- PID del processo.

Il nodo 1 ha il badge **debug**.

**Invio ciclico.** Ogni operazione invia un JSON del tipo:

```json
{"seq": 42, "timestamp": "2026-10-19T16:28:14.2157204+02:00", "source": "simulatore", "runId": "6f2bf38c-…"}
```

`seq` è progressivo, `runId` identifica la sessione del simulatore. Si possono impostare:

- **intervallo** in ms;
- **destinazione**: tutti a rotazione, solo il Leader, solo i Follower per provare l'inoltro, oppure un nodo preciso;
- **riempimento**: un campo `pad` per simulare payload più grandi.

Ci sono anche *Invia una* e *Raffica* (N operazioni contemporanee).

Il simulatore si comporta come un client conforme al [Contratto client](Contratto-client):

- `X-Request-ID` generato una volta per operazione;
- stessi byte del body a ogni ritento;
- retry su errore di rete, 503 e 504 con backoff esponenziale e jitter, entro 60 s;
- mai retry su 4xx;
- su errore di rete prova un altro nodo.

Contatori, latenze (p50/p95/max), grafico degli ultimi 120 s e tabella delle ultime operazioni mostrano per ognuna il percorso dei tentativi, ad esempio `5002✕ → 5003:200`. *Esporta CSV* scarica lo storico completo.

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

Ogni scenario si esegue con un clic (o *Esegui tutti*) e riporta l'esito atteso, i passi e il verdetto:

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

## Verifica delle scritture

*Verifica ora* legge `applied.log` della state machine SharedFile (`src/RaftCore/devcluster/shared-state/applied.log`) e lo confronta con le operazioni della sessione:

- **violazioni**:
  - un `seq` o un `RequestId` applicato più volte;
  - un'operazione confermata con 200 ma assente;
- **ammesso** (REQ-API-3): operazioni abbandonate dal client ma scritte comunque;
- totali per esito (applicate, deduplicate, rifiutate, scadute, in quarantena, no-op) e continuità degli indici del log.

Con la state machine SQL Server la verifica va fatta sul database, ad esempio:

```sql
SELECT RequestId, COUNT(*) FROM raft.IngestedData GROUP BY RequestId HAVING COUNT(*) > 1
```

## Configurazione

`src/RaftCore.Simulator/appsettings.json`, sezione `Simulator`:

| Parametro | Default | Note |
| --- | --- | --- |
| `Hosts` | `127.0.0.1` | Host da sondare |
| `Ports` | `5001-5010` | Stesso formato di `RestPorts` |
| `StatusPollMs` / `StatusTimeoutMs` | 1000 / 500 | Sondaggio di `/api/v1/cluster/status` e `/health/ready` |
| `DefaultIntervalMs` / `DefaultTarget` | 200 / `RoundRobin` | Valori iniziali dell'invio ciclico |
| `MaxInFlight` | 64 | Oltre, i tick dell'invio ciclico vengono saltati e contati |
| `RequestTimeoutMs` | 15000 | Timeout per tentativo (REQ-CLI-2) |
| `RetryBudgetMs` | 60000 | Tempo massimo dei ritenti di un'operazione |
| `MaxBodyBytes` | 1048576 | Usato dallo scenario «Body oltre MaxBodyBytes» |
| `AppliedLogPath` | `../RaftCore/devcluster/shared-state/applied.log` | Relativo alla cartella del progetto del simulatore |
| `NodeWorkingDirectory` | `../RaftCore` | Directory di lavoro dei nodi riavviati (Windows) |
| `NodeEnvironment` | — | Variabili ereditate dai nodi riavviati. In Development contiene `ASPNETCORE_ENVIRONMENT` e il segreto di sviluppo |

API del simulatore, utili anche per script: `GET /sim/state`, `POST /sim/generator/start|stop|one|burst|reset`, `POST /sim/scenarios/{id}`, `POST /sim/nodes/{nodeId}/kill|pause|resume|restart`, `POST /sim/leader/kill|pause`, `POST /sim/verify`, `GET /sim/ops.csv`.
