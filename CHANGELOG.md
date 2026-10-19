# Changelog

Formato basato su [Keep a Changelog](https://keepachangelog.com/it/1.1.0/); il progetto segue il [Semantic Versioning](https://semver.org/lang/it/).
Il workflow di release usa la sezione della versione come note di rilascio.

## [Unreleased]

### Aggiunto
- **Cluster di sviluppo con un solo avvio**:
  - in ambiente Development con `DevCluster:Instances` > 1, il processo avviato (es. F5 in Visual Studio, l'unico in debug) è il nodo 1 e lancia gli altri membri come processi figli;
  - i figli si aprono in una finestra console per nodo su Windows, oppure con l'output prefissato;
  - i figli si chiudono quando il padre termina, anche con lo stop del debugger.
- **Configurazione di sviluppo preimpostata** (`appsettings.Development.json`) e **profili di avvio** di Visual Studio: cluster da 3 nodi, nodo singolo, singoli nodi 1/2/3.
- **`RestPorts`**: porte in formato intervallo (`5001-5005`), lista (`5001,5003,5007`) o misto (`5001-5005;5010;5012`). Il nodo usa la prima libera. `RestPortRange` resta come alias.
- **Discovery a sondaggio**:
  - finché un membro non ha un indirizzo noto, o non risponde più, il nodo prova gli host di `Discovery:SeedHosts` (e di `Peers`) su tutte le porte ammesse;
  - l'identità di chi risponde è verificata dalla firma;
  - le istanze si trovano anche con porte dinamiche e senza UDP, e un nodo riavviato su un'altra porta viene ritrovato.
- Metriche `probeRounds` e `probeFound`.

### Modificato
- Un segreto `dev-only-…` viene rifiutato fuori dall'ambiente Development.
- Log della console senza gli scope di tracing (`SpanId`/`TraceId`).

### Corretto
- **Failover lento su Windows dopo la caduta del Leader.** Il candidato aspettava la risposta di *tutti* i membri, compreso quello spento, sia nel PreVote sia nel voto. Su Windows una connessione verso una porta chiusa arriva al timeout invece di essere rifiutata subito. I round si allungavano e due nodi si candidavano insieme: si osservavano più voti contesi in serie e un failover oltre i 7 s. Ora il candidato procede appena raggiunge il quorum: nello scenario di prova il failover è passato da 1,3-1,5 s a 0,6-0,9 s.
- Test: metriche degli errori transitori sommate su tutti i nodi, perché la leadership può cambiare durante il test; test del failover con le RPC verso il nodo spento appese fino al timeout.

## [0.2.0] - 2026-10-19

Prima versione orientata alla produzione su SQL Server condiviso. **Contiene modifiche incompatibili** con la configurazione e lo storage della 0.1 (vedi "Migrazione").

### Aggiunto
- **State machine su database condiviso**. SQL Server per la produzione, `SharedFile` per lo sviluppo. Solo il Leader applica le voci, in una transazione condizionata a `LastApplied = LogIndex - 1`: un ex Leader o un nodo rientrato dopo settimane non riscrive mai dati già scritti.
- **Voci obsolete**: una voce committata oltre `StateMachine:MaxPendingAgeMinutes` dalla ricezione (es. dopo un cluster rimasto spento) viene registrata come `Expired` (HTTP 410) e non viene scritta.
- **Poison entry**:
  - gli errori di business permanenti (vincoli, conversioni, payload non valido) diventano `Rejected` (HTTP 422);
  - gli errori transitori (rete, deadlock, failover di SQL Server) vengono ritentati senza limite con backoff;
  - gli altri errori vengono ritentati `MaxApplyAttempts` volte, poi la voce va in quarantena (`raft.Quarantine`) e il cluster prosegue.
- **Compattazione del log e InstallSnapshot**: WAL a segmenti; RAM e disco restano limitati alla finestra non compattata (`Log:RetainEntries`). Un Follower troppo indietro si riallinea con uno snapshot invece di ricevere settimane di log.
- **Identità del risponditore**: ogni RPC indica il destinatario, e ogni risposta è firmata dal nodo che risponde ed è legata al nonce della richiesta. Un URL che porta al nodo sbagliato viene rilevato e invalidato, e non viene mai contato due volte nel quorum.
- **Propagazione degli URL dei membri** negli heartbeat del Leader, più un warning esplicito quando un membro non ha un URL noto.
- **Range di porte REST** (`RestPortRange`, es. `5000-5010`): il nodo usa la prima porta libera. Segnaposto `{port}` e `{nodeId}` in `AdvertiseUrl`, `DataDir` e negli altri percorsi.
- **Esecuzione come Windows Service e unità systemd**, con script di installazione, launchd per macOS, Dockerfile e docker-compose con SQL Server.
- **Group commit** del Leader: più richieste concorrenti con un solo fsync, eseguito fuori dal lock globale.
- **Lock esclusivo della DataDir**: un secondo processo sulla stessa directory non parte (codice di uscita 3).
- **Test automatici**:
  - unit test;
  - cluster in-process con partizioni di rete;
  - E2E con `kill -9` di processi reali;
  - integrazione SQL Server.
- **CI** su Windows, Linux e macOS, **release** su tag con pacchetti self-contained e immagine GHCR, sincronizzazione della Wiki.
- **Metriche**: `walFsyncLatencyAvgUs`/`MaxUs`, `applyLag`, `applyQuarantined`, `applyExpired`, `applyRejected`, `applyTransientErrors`, `snapshotIndex`, `snapshotsInstalled`, `rpcWrongDestination`, `rpcResponseAuthFailed`, `peerUrlUnknown`, `writeBatches`.

### Modificato
- **Log molto meno rumorosi**: filtrati i log `Information` di `HttpClient` (4 righe per ogni RPC interna); i messaggi ripetuti sono limitati nel tempo.
- **Proxy**:
  - il deadline viene propagato al Leader;
  - il 504 viene restituito solo allo scadere del deadline unico, anche se il Leader cambia;
  - un 503 per perdita di leadership viene ritentato sul nuovo Leader.
- **Header di inoltro** (`X-Forward-Hops`, `X-Raft-Deadline-Ms`): valgono solo se firmati da un membro. Un client non può più ottenere 421.
- **Autenticazione delle RPC**: le RPC non autenticate vengono rifiutate prima di leggere il body.
- **`commitIndex`** è salvato in un file separato (`commit.idx`), così un crash non può rendere illeggibili term e voto.
- **Thread pool**: viene impostato un minimo all'avvio. Su macchine con pochi core le pause dovute a fsync sincroni facevano scadere il lease del Leader.

### Corretto
- Windows: la sostituzione atomica di `raft_state.json` poteva fallire con "accesso negato" se un altro processo (tipicamente l'antivirus) teneva aperto il file per un istante, rendendo il nodo `unhealthy` al primo voto. Ora vengono fatti tentativi ripetuti per un tempo limitato.
- **Nodo bloccato in PreVote** dopo la caduta del Leader quando la discovery UDP era incompleta (ad esempio più istanze sulla stessa macchina Windows).
- **Deadlock in modalità Bootstrap** (inversione dell'ordine dei lock tra membri e nodo), e ammissione di nodi con `ClusterSize` diverso.
- **Voci committed troncate al riavvio** dopo aver ridotto `MaxBodyBytes`.
- **CPU al 100%** quando un client si disconnetteva durante l'attesa di un Leader.
- **Verifica del candidato**: il `CandidateId` del voto non era verificato rispetto al mittente.
- **Allarmi disco**: l'allarme al 70% veniva emesso dopo quello all'85%.
- **`SO_REUSEPORT` su macOS**: veniva usata la costante di Linux.

### Migrazione dalla 0.1
- Il progetto è in `src/RaftCore`.
- Il vecchio `data/raft.wal` viene convertito automaticamente nel primo segmento di `data/wal/`.
- `Storage:OutputFilePath` e `Storage:IdempotencyRetentionHours` sono stati rimossi. L'idempotenza è permanente nel database (`raft.Requests`); la state machine si configura in `StateMachine`.
- Il default di `Storage:DataDir` è `./data/{nodeId}`.
- I percorsi relativi sono risolti rispetto alla directory corrente, oppure alla directory dell'eseguibile quando si esegue come servizio, oppure a `RaftCluster:BaseDirectory`.

## [0.1.0] - 2026-10-19
- Prima versione: elezione con PreVote, replica, lease, WAL, idempotenza, Smart Proxy, state machine su file locale.
