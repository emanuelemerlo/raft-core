# Architettura

## Componenti

| Componente | Ruolo |
| --- | --- |
| `RaftNode` | Elezione (PreVote, voto), replica, commit, lease, group commit, apply sul database, compattazione |
| `WalLog` | Log su disco a segmenti, append-only, CRC32C, troncamento logico, snapshot |
| `ClientApi` | API pubblica: validazione, inoltro al Leader (Smart Proxy), deadline unico |
| `SecurityMiddleware` + `RpcVerifier` | Autenticazione HMAC delle RPC e degli inoltri, firma delle risposte |
| `PeerRegistry` + `UdpDiscoveryService` | Indirizzi dei nodi (UDP, configurazione, RPC, gossip del Leader). Non influiscono mai sul quorum |
| `IDataRepository` | State machine su database condiviso: `SqlServerDataRepository`, `SharedFileDataRepository` |

## Percorso di una scrittura

1. Il client invia `POST /api/v1/data` a un nodo qualsiasi. Il body viene letto una volta, validato e ne viene calcolato lo SHA-256.
2. Se il nodo non è Leader, inoltra la richiesta con firma HMAC, numero di hop e tempo residuo del deadline.
3. Il Leader verifica l'idempotenza nella finestra di log. Se l'ID è già in corso, la richiesta attende la stessa voce. Altrimenti la accoda al **group commit**, che scrive più richieste con un solo fsync fuori dal lock globale.
4. La voce viene replicata. È *committed* quando è durevole su un quorum e appartiene al term corrente.
5. Il Leader la **applica al database** in una transazione che: verifica `LastApplied = LogIndex - 1`; decide l'esito (scritta, duplicata, in conflitto, scaduta, rifiutata); registra l'esito; avanza `LastApplied`.
6. La risposta HTTP è l'esito registrato. Ogni replay, anche dopo un failover, restituisce lo stesso status e lo stesso body.

## Garanzie

- **Safety di Raft**: al più un Leader per term; le voci committed non vengono mai perse né sovrascritte.
- **Una sola scrittura per voce**: la condizione su `LastApplied` rende sicuri anche due applicatori concorrenti (ex Leader e nuovo Leader).
- **Una sola scrittura per RequestId**: indice permanente `raft.Requests`, indipendente dalla compattazione del log.
- **Determinismo**: ogni decisione (scadenza, rifiuto, quarantena) viene presa una volta sola e registrata nella stessa transazione. Non dipende quindi dall'orologio dei singoli nodi.

## Elezione

- **PreVote obbligatorio**: un nodo isolato non incrementa il term e non disturba il cluster al rientro.
- **Leader stickiness**: un nodo che ha sentito il Leader negli ultimi `ElectionTimeoutMinMs` nega PreVote e voto.
- **Lease**: il Leader smette di scrivere se un quorum non ha confermato messaggi inviati negli ultimi `LeaderLeaseMs`, con `LeaderLeaseMs < ElectionTimeoutMinMs`.

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
wal/00000001.wal   segmenti del log
wal/snapshot.json  punto di compattazione
```
