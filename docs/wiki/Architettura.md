# Architettura

## Componenti

| Componente | Ruolo |
| --- | --- |
| `RaftNode` | Elezione (PreVote, voto), replica, commit, lease, group commit, apply sul database, compattazione |
| `WalLog` | Log su disco a segmenti, append-only, CRC32C, troncamento logico, snapshot |
| `ClientApi` | Endpoint di ingestione configurati (`Endpoints`): validazione, inoltro al Leader (Smart Proxy), deadline unico |
| `SecurityMiddleware` + `RpcVerifier` | Autenticazione HMAC delle RPC e degli inoltri, firma delle risposte |
| `PeerRegistry` + `UdpDiscoveryService` | Indirizzi dei nodi (UDP, configurazione, RPC, gossip del Leader). Non influiscono mai sul quorum |
| `IDataRepository` | State machine su database condiviso: `SqlServerDataRepository` (un `ISqlPayloadWriter` per endpoint), `SharedFileDataRepository` |

## Percorso di una scrittura

1. Il client invia `POST` al path di un endpoint, su un nodo qualsiasi. Il body viene letto una volta e validato; l'hash SHA-256 copre il nome dell'endpoint e il body.
2. Se il nodo non è Leader, inoltra la richiesta **sullo stesso path** con firma HMAC (che copre il path), numero di hop e tempo residuo del deadline.
3. Il Leader verifica l'idempotenza nella finestra di log. Se l'ID è già in corso, la richiesta attende la stessa voce. Altrimenti la accoda al **group commit**, che scrive più richieste con un solo fsync fuori dal lock globale. La voce contiene il nome dell'endpoint (`PayloadType`).
4. La voce viene replicata. È *committed* quando è durevole su un quorum e appartiene al term corrente.
5. Il Leader la **applica al database** in una transazione che: verifica `LastApplied = LogIndex - 1`; decide l'esito (scritta, duplicata, in conflitto, scaduta, rifiutata); per una scrittura usa il writer dell'endpoint indicato nella voce; registra l'esito; avanza `LastApplied`.
6. La risposta HTTP è l'esito registrato. Ogni replay, anche dopo un failover, restituisce lo stesso status e lo stesso body.

## Garanzie

- **Safety di Raft**: al più un Leader per term; le voci committed non vengono mai perse né sovrascritte.
- **Una sola scrittura per voce**: la condizione su `LastApplied` rende sicuri anche due applicatori concorrenti (ex Leader e nuovo Leader).
- **Una sola scrittura per RequestId**: indice permanente `raft.Requests`, indipendente dalla compattazione del log e comune a tutti gli endpoint.
- **Determinismo**: ogni decisione (scadenza, rifiuto, quarantena) viene presa una volta sola e registrata nella stessa transazione. Non dipende quindi dall'orologio dei singoli nodi.

## Endpoint multipli

Gli endpoint configurati (`Endpoints`) sono porte d'ingresso diverse dello **stesso** log: Raft, WAL, replica e cursore `LastApplied` non distinguono i tipi di dato. Il tipo viaggia nella voce (`PayloadType`), perché il nodo che applica può non essere quello che ha ricevuto la richiesta, ad esempio dopo un failover.

Conseguenze di un log unico:
- **Ordine totale**: le scritture di tutti gli endpoint sono applicate nell'ordine di commit. Se un tipo di dato dipende da un altro (es. un ordine che riferisce un cliente), l'ordine è rispettato.
- **Throughput condiviso**: l'apply è sequenziale per tutto il cluster.
- **Blocco in testa alla coda**: una voce lenta o in errore ritarda tutte le successive, di qualunque endpoint, finché non riceve un esito. Un errore transitorio del database ferma tutto finché il database non torna disponibile; un errore non classificato al più per `MaxApplyAttempts` tentativi, poi la voce va in quarantena.

Quando un gruppo di endpoint deve essere isolato dagli altri (tabelle lente, carichi molto diversi), si avviano **cluster distinti** con `ClusterId` diversi, anche sullo stesso database: `ApplyState`, `Requests` e `AppliedEntries` sono partizionate per `ClusterId`.

## Versioni e compatibilità

- Ogni RPC e ogni annuncio UDP contengono la **versione di protocollo**. Un nodo di un'altra versione viene rifiutato: non vota, non riceve voci e non conta nel quorum. Un aggiornamento che cambia la versione di protocollo richiede quindi il riavvio di tutti i nodi (vedi [Operazioni](Operazioni)).
- Ogni RPC e ogni risposta firmata contengono anche l'**impronta della configurazione degli endpoint**. Una differenza non blocca il cluster, per permettere di aggiornare la configurazione un nodo alla volta, ma viene segnalata.
- Il WAL ha un formato versionato (attuale: 2) e lo schema del database una versione (attuale: 2). Non è prevista migrazione da formati precedenti: un WAL di formato diverso blocca l'avvio del nodo, uno schema di versione diversa blocca l'apply.

## Elezione

- **PreVote obbligatorio**: un nodo isolato non incrementa il term e non disturba il cluster al rientro.
- **Leader stickiness**: un nodo che ha sentito il Leader negli ultimi `ElectionTimeoutMinMs` nega PreVote e voto.
- **Lease**: il Leader smette di scrivere se un quorum non ha confermato messaggi inviati negli ultimi `LeaderLeaseMs`, con `LeaderLeaseMs < ElectionTimeoutMinMs`.
- **Quorum senza attese inutili**: PreVote e voto procedono appena è raggiunto il quorum, senza attendere i membri lenti o spenti. Le risposte tardive vengono comunque elaborate, ad esempio per osservare un term più alto. Così un nodo spento, che su Windows non rifiuta la connessione ma la lascia scadere, non allunga i round di elezione, e due candidature simultanee diventano improbabili.

## Indirizzi e identità

Un nodo può conoscere l'URL di un altro da quattro fonti, in ordine di priorità:
1. dall'ultima RPC autenticata ricevuta da quel nodo;
2. dall'annuncio UDP firmato;
3. dal gossip del Leader, che negli heartbeat invia gli URL di tutti i membri;
4. dalla configurazione `Peers`.

Ogni RPC indica il **destinatario** e ogni risposta è **firmata dal nodo che risponde** ed è legata al nonce della richiesta. Se un URL porta al nodo sbagliato, la risposta viene scartata e l'URL invalidato. Due nodi non possono così essere contati come uno solo, e una risposta registrata non può essere riprodotta.

## Compattazione e snapshot

Il WAL è diviso in segmenti. Quando le voci applicate superano `RetainEntries`:
1. il nodo scrive `wal/snapshot.json` con `SnapshotIndex` e `SnapshotTerm`, in modo durevole;
2. elimina i segmenti interamente precedenti;
3. libera la RAM corrispondente.

Poiché lo stato applicativo vive nel database condiviso, lo snapshot non contiene dati. Un Follower più indietro dello snapshot riceve `InstallSnapshot` e riparte da quel punto.

## File di un nodo (`DataDir`)

```text
.lock              lock esclusivo del processo
raft_state.json    term, voto, membri (scrittura atomica e durevole)
commit.idx         commitIndex (limite inferiore, non sincronizzato)
wal/00000001.wal   segmenti del log (formato 2)
wal/snapshot.json  punto di compattazione
```
