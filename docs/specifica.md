# Specifiche Tecniche — Servizio .NET ad Alta Affidabilità (RaftCore)

## Revisione 6 — Specifica normativa per implementazione e collaudo

*Data: 4 novembre 2026. Base: revisione 5 (8 ottobre 2026). Stato: proposta di baseline, allineata all'implementazione v0.3 (non ancora rilasciata).*

> **Nota normativa.** DEVE / NON DEVE indicano requisiti vincolanti (MUST). DOVREBBE indica una raccomandazione tecnica (SHOULD). PUÒ indica una scelta ammessa (MAY). Ogni requisito ha un identificativo `REQ-<area>-<n>` richiamato dalla matrice dei test (§21). I valori numerici sono baseline da confermare con i test di accettazione, ma non possono essere modificati in modo da violare i vincoli di sicurezza o temporali della specifica.

### Modifiche rispetto alla rev. 5

La rev. 5 prevedeva una state machine replicata su ogni nodo e vietava gli storage condivisi. Il target applicativo è invece **un database SQL Server condiviso**, che riceve dati di più tipi da **più endpoint REST**. Ne seguono queste modifiche.

| Area | Modifica |
| --- | --- |
| State machine | Database **condiviso**; solo il Leader applica, in modo condizionale su `LastApplied` (§11). Abbandonati la replica locale della state machine e il file di output |
| Decisioni deterministiche | Scadenza delle voci obsolete e quarantena delle *poison entry* prese una sola volta e registrate nella transazione di apply (REQ-SM-5, REQ-SM-6) |
| Idempotenza | Indice **permanente** nel database, indipendente dalla compattazione; `IdempotencyRetentionHours` rimosso (§12) |
| Compattazione | WAL a segmenti compattato, snapshot senza dati, `InstallSnapshot` (§13) |
| Endpoint | **N endpoint di ingestione** configurabili, ciascuno con il proprio tipo di dato e la propria tabella; un solo log e un solo ordine di scrittura (§14) |
| Inoltro | Header di inoltro validi solo se firmati; deadline propagato; inoltro sullo stesso path dell'endpoint (§15) |
| Sicurezza | Destinatario nella firma delle RPC, risposte firmate, versione di protocollo nelle firme (§16) |
| Discovery | Gossip degli URL dal Leader, discovery a sondaggio su elenchi di porte (§5) |
| Versioni | Versione di protocollo, formato del WAL e versione dello schema del database; nessuna migrazione dalla v0.2 (§18) |
| Operatività | Lock della `DataDir`, cluster di sviluppo con un solo avvio, endpoint informativi ristretti a loopback (§17) |
| Test | Aggiunti T45-T66; T30 rimosso (§21) |

Requisiti della rev. 5 **rimossi**: REQ-IDM-5/6 (retention dell'idempotenza). Identificativi **riusati con un nuovo significato**: REQ-SM-3 (prima file di output con `LogIndex`, ora cursore partizionato per `ClusterId`), REQ-SM-5 (prima rotazione del file di output, ora poison entry), REQ-SNP-1/2 (prima WAL mai compattato, ora compattazione e `InstallSnapshot`), REQ-SNP-4 (prima requisiti futuri degli snapshot, ora dimensionamento). Gli altri requisiti modificati sono riscritti nella loro sezione.

## 1. Obiettivo e garanzie

**REQ-GEN-1** Il sistema DEVE essere un servizio .NET 8+ eseguibile in N istanze (da 1 a 7; in produzione 3 o 5) su server distinti della stessa rete L2. Riceve dati JSON su uno o più endpoint REST e li scrive su un database condiviso.

**REQ-GEN-2** Il sistema NON DEVE dipendere da **componenti di coordinamento** esterni (Consul, Redis, ZooKeeper, HAProxy o equivalenti). La state machine PUÒ essere un database condiviso (`StateMachine:Provider`, §11). WAL e stato Raft restano locali a ogni nodo.

**REQ-GEN-3** L'architettura DEVE essere un sottoinsieme rigoroso di Raft: elezione del Leader, replica del log, compattazione con `InstallSnapshot`. La membership dinamica è esclusa (§24).

**REQ-GEN-4** Per ogni term DEVE esistere al più un Leader. Solo il Leader elabora le scritture e le applica al database; i Follower agiscono da Smart Proxy (§15).

### 1.1 Garanzie

Il sistema DEVE garantire:

1. **Safety:** non esistono due Leader validi nello stesso term.
2. **Durabilità:** una risposta `200` a una scrittura implica che la voce sia persistita in modo durevole (fsync) su un quorum **e** applicata al database.
3. **Ordine delle scritture:** le scritture accettate, di qualunque endpoint, sono ordinate totalmente dal log Raft e applicate in quell'ordine.
4. **Una sola scrittura per voce:** ogni voce committed è applicata al database al più una volta, anche con due applicatori concorrenti (ex Leader e nuovo Leader).
5. **Disponibilità condizionata:** se un quorum di nodi è operativo e connesso, un Leader può essere eletto e le scritture proseguono, purché il database sia raggiungibile.
6. **Idempotenza applicativa:** lo stesso `RequestId` non genera due scritture, su nessun endpoint.
7. **Convergenza:** dopo la fine di una partizione, i nodi recuperabili convergono sul log del Leader corrente.

Il cluster tollera la perdita simultanea di al più floor((N-1)/2) nodi senza perdere voci committed. L'alta affidabilità del database è un prerequisito esterno (§24): un database irraggiungibile ferma le scritture, ma non causa perdite né elezioni.

### 1.2 Limite della garanzia «exactly-once»

Il sistema NON DEVE essere descritto come exactly-once in senso generale. La garanzia è:

> **effectively-once per le richieste HTTP**: un `RequestId` produce al più una scrittura nel database, perché l'effetto applicativo è eseguito solo dalla state machine, nella stessa transazione che registra l'esito e avanza `LastApplied`.

Gli effetti esterni non transazionali (email, chiamate HTTP a terzi, messaggi su sistemi terzi, comandi hardware) non sono coperti.

## 2. Assunzioni di rete e di deploy

**REQ-NET-1** Tutti i nodi DOVREBBERO trovarsi nello stesso segmento L2 se si usa la discovery UDP: il broadcast `255.255.255.255` non attraversa router o VLAN. Oltre L2 si usano `Peers` e la discovery a sondaggio (§5).

**REQ-NET-2** Più istanze sulla stessa macchina (sviluppo e test) DEVONO essere supportate: porta UDP condivisa con `SO_REUSEADDR`/`SO_REUSEPORT`, porta REST e `DataDir` distinte per istanza.

**REQ-NET-3** L'URL pubblicato DEVE essere `AdvertiseUrl` se configurato (con il segnaposto `{port}`); altrimenti l'indirizzo dell'interfaccia `BindInterface`; altrimenti quello della route predefinita.

**REQ-NET-4** Il firewall DEVE consentire UDP su `UdpDiscoveryPort` (se si usa la discovery UDP) e TCP sulle porte REST tra tutti i nodi, e TCP verso il database dal Leader (di fatto da ogni nodo).

**REQ-NET-5** La rete è considerata non fidata: annunci e RPC interne DEVONO essere autenticati (§16).

**REQ-NET-6 (elenco di porte).** Con `RestPorts` (intervallo `X-Y`, lista `X,Y,Z` o misto `X-Y;A;B`, al massimo 1000 porte per intervallo) il nodo DEVE usare la prima porta TCP libera dell'elenco e pubblicare l'URL effettivo. Se nessuna è libera NON DEVE partire (codice 4).

## 3. Membership e quorum

```text
Quorum = floor(ClusterSize / 2) + 1
```

| N | Quorum | Nodi perdibili senza bloccare le scritture |
| --: | --: | --: |
| 1 | 1 | 0 |
| 2 | 2 | 0 |
| 3 | 2 | 1 |
| 4 | 3 | 1 |
| 5 | 3 | 2 |
| 6 | 4 | 2 |
| 7 | 4 | 3 |

**REQ-CLU-1** Il quorum DEVE essere calcolato solo su `ClusterSize` configurato. NON DEVE mai essere calcolato sul numero di peer visibili.

**REQ-CLU-2** L'insieme dei votanti DEVE essere composto da esattamente `ClusterSize` `NodeId` distinti, noto a ogni nodo e persistente. Nessun altro nodo può votare, candidarsi o essere contato nel quorum.

**REQ-CLU-3** Sono ammesse due modalità (`MembershipMode`):

- `Static` (obbligatoria in produzione): ogni nodo riceve in configurazione il proprio `NodeId` e la lista `Members` completa.
- `Bootstrap` (solo sviluppo o prima inizializzazione): il nodo ammette i primi `ClusterSize` `NodeId` distinti visti con HMAC valido e li persiste. Una volta raggiunto `ClusterSize` l'insieme è immutabile. Finché non è completo il nodo NON DEVE votare né candidarsi.

**REQ-CLU-4** Ogni annuncio UDP e ogni RPC DEVE contenere `ClusterId`, `ClusterSize` e `MembersHash` (hash dell'insieme ordinato dei `NodeId`). Un messaggio con valori diversi da quelli locali DEVE essere rifiutato (HTTP 409 per le RPC), contato nella metrica `membersHashMismatch` e segnalato con un evento di severità alta.

**REQ-CLU-5** Una RPC proveniente da un `NodeId` non appartenente all'insieme dei votanti DEVE essere rifiutata (HTTP 403) e contata in `nonMemberRejected`.

**REQ-CLU-6** Sostituzione di un nodo con disco perso: il nuovo nodo DEVE avere un nuovo `NodeId`. In modalità `Static` la procedura è: (1) confermare che il vecchio nodo è definitivamente spento (fencing manuale); (2) aggiornare `Members` su tutti i nodi; (3) riavviare i nodi uno alla volta, il nuovo con `AllowEmptyState=true`. Poiché `MembersHash` cambia, durante la procedura i nodi con insiemi diversi si rifiutano a vicenda: è prevista una **breve indisponibilità pianificata**.

**REQ-CLU-7** Un cluster che perde il quorum DEVE continuare a servire `/health/live` e NON DEVE accettare nuove scritture.

## 4. Identità e stato persistente

**REQ-ID-1** Ogni nodo ha un `NodeId` (GUID). In modalità `Static` è assegnato in configurazione; in `Bootstrap` è generato al primo avvio e salvato in `NodeIdFile`.

**REQ-ID-2** Il `NodeId` NON DEVE cambiare in un normale riavvio.

**REQ-ID-3** Un nodo senza stato persistente (WAL e stato Raft assenti) DEVE avviarsi solo se `Storage.AllowEmptyState=true`. L'operatore imposta il flag al primo deploy o per un nuovo `NodeId` e lo rimuove subito dopo. Senza il flag il nodo NON DEVE partire (codice 2). Uno stato Raft assente con un WAL presente, o una `DataDir` di un altro `NodeId`, DEVONO richiedere un intervento esplicito.

**REQ-ID-4** Lo stato persistente del nodo è:

```text
raft_state.json    currentTerm, votedFor, membri, NodeId     (scrittura atomica e durevole)
commit.idx         commitIndex, con CRC                       (limite inferiore, non sincronizzato a ogni commit)
wal/*.wal          segmenti del log
wal/snapshot.json  SnapshotIndex, SnapshotTerm                (scrittura durevole)
```

`LastApplied` è conservato dalla state machine condivisa (§11), non dal nodo.

**REQ-ID-5** `currentTerm`, `votedFor` e le modifiche al log DEVONO essere persistiti in modo crash-safe. Un nodo NON DEVE inviare un voto o una conferma RPC prima che lo stato richiesto da quella decisione sia stato scritto durevolmente.

**REQ-STO-1 (lock della DataDir).** Un nodo DEVE acquisire un lock esclusivo sulla propria `DataDir`; se è in uso da un altro processo, NON DEVE partire (codice 3).

## 5. Discovery

**REQ-DIS-1** La discovery serve esclusivamente a scoprire gli URL dei peer. NON DEVE essere usata per elezione, voto, replica del log, commit o rilevazione della leadership.

Ogni annuncio UDP (broadcast, oppure multicast se `UdpMode=Multicast`, ogni `AnnounceIntervalMs`, all'avvio e a ogni cambio di ruolo) contiene:

| Campo | Descrizione |
| --- | --- |
| `ProtocolVersion` | Verificata secondo REQ-VER-1. |
| `ClusterId`, `ClusterSize`, `MembersHash` | Verificati secondo REQ-CLU-4. |
| `NodeId`, `RestUrl` | Identità ed endpoint del nodo. |
| `Role`, `Term` | Solo informativi. |
| `Incarnation` | Valore casuale a 128 bit generato a ogni avvio. |
| `Sequence` | Contatore monotono all'interno di un'incarnation. |
| `Hmac` | HMAC-SHA256 di tutti i campi con `ClusterSecret`. |

**REQ-DIS-2** `Role` e `Term` dell'annuncio NON DEVONO mai determinare una decisione Raft.

**REQ-DIS-3** La freschezza di un peer DEVE essere misurata con l'istante di ricezione locale (clock monotono).

**REQ-DIS-4** Il registro dei peer contiene solo membri validi. Un nodo conosce l'URL di un membro da quattro fonti, in ordine di priorità: (1) l'ultima RPC autenticata ricevuta da quel membro; (2) l'annuncio UDP firmato; (3) il **gossip del Leader**, che negli `AppendEntries` invia gli URL di tutti i membri (`MemberUrls`); (4) la configurazione `Peers`. Un URL che porta a un nodo diverso da quello atteso DEVE essere invalidato (metrica `rpcWrongDestination`). Il registro NON influisce sul quorum. Annunci di non membri sono ignorati e generano un allarme.

**REQ-DIS-5** Anti-replay degli annunci: per ogni `NodeId` il ricevente accetta una sola `Incarnation` alla volta con `Sequence` strettamente crescente; una `Incarnation` nuova sostituisce la precedente.

**Threat model.** Poiché le `Incarnation` non sono ordinabili, un attaccante che ha catturato pacchetti validi può far accettare un annuncio obsoleto, con un URL stantio nel registro. Senza verifica dell'identità del risponditore, un URL che porta a un *altro* membro causerebbe un **doppio conteggio** di voti e conferme (rischio di safety con N ≥ 5). Con la firma delle risposte (REQ-SEC-3) la risposta di un nodo diverso da quello atteso viene scartata: il rischio è eliminato e l'impatto resta un degrado della discovery.

**REQ-DIS-6** Durante la fase di Discovery (`DiscoveryPhaseMs`) il nodo NON vota e NON si candida, ma DEVE accettare un `AppendEntries` valido, passando a Follower.

**REQ-DIS-7 (discovery a sondaggio).** Finché un membro non ha un URL noto, o dopo `UnreachableAfterFailures` RPC fallite consecutive, il nodo DEVE sondare gli host di `Discovery:SeedHosts` e di `Peers` su tutte le porte ammesse (`ProbePorts`, per default quelle di `RestPorts`), con `POST /internal/v1/hello`. Il destinatario jolly `*` è ammesso solo su questo endpoint, che non modifica stato. Un indirizzo viene registrato solo se la risposta è firmata dal membro che la invia.

## 6. Stati e transizioni

Gli stati sono `Discovery`, `Follower`, `Candidate`, `Leader`.

| Da | A | Condizione |
| --- | --- | --- |
| Avvio | Discovery | Sempre, dopo il recovery (§10). |
| Discovery | Follower | Fine di `DiscoveryPhaseMs`, oppure `AppendEntries` valido. Con `ClusterSize=1` passa direttamente a Leader. |
| Follower | Pre-candidatura | Timeout di elezione casuale in `[ElectionTimeoutMinMs, ElectionTimeoutMaxMs]` senza `AppendEntries` valido e senza voto concesso. |
| Pre-candidatura | Candidate | PreVote accolto da un quorum (§7). Altrimenti torna a Follower e riarma il timer. |
| Candidate | Leader | Quorum di voti validi per il proprio term. |
| Candidate | Candidate | Timeout senza esito: nuovo giro (nuovo PreVote). |
| Candidate | Follower | RPC valida con term maggiore (si aggiorna e persiste il term), oppure `AppendEntries` valido dello stesso term. |
| Leader | Follower | RPC con term maggiore; perdita del lease (§8); errore persistente di storage che impedisce di garantire la durabilità. |

**REQ-STA-1** Al passaggio da Leader a Follower le richieste in corso senza esito DEVONO terminare con 503 o 504. L'esito reale è determinato dal log e dal database: il client ritenta con lo stesso `RequestId`.

## 7. PreVote e RequestVote

**REQ-VOT-1** Il PreVote è **obbligatorio**. Prima di incrementare `currentTerm`, il nodo invia `POST /internal/v1/prevote` con `{Term = currentTerm+1, CandidateId, LastLogIndex, LastLogTerm}` a tutti i votanti. Il PreVote NON modifica `currentTerm` né `votedFor`.

**REQ-VOT-2** Un nodo concede un PreVote se e solo se: (a) non è in fase di Discovery e può partecipare (storage sano, recovery completato); (b) il term proposto è maggiore di `currentTerm`; (c) il log del candidato è almeno aggiornato quanto il proprio; (d) **non ha ricevuto un `AppendEntries` valido da un Leader negli ultimi `ElectionTimeoutMinMs`** (leader stickiness); (e) se è Leader, il suo lease è scaduto.

**REQ-VOT-3** Solo dopo un PreVote accolto da un quorum (voto proprio incluso) il nodo incrementa `currentTerm`, vota per sé, persiste e invia `RequestVote` (`POST /internal/v1/vote`).

**REQ-VOT-4** Un `RequestVote` è concesso se e solo se: il term richiesto non è inferiore a quello locale (se maggiore, il nodo lo aggiorna e lo persiste); non ha già votato un altro candidato nello stesso term; il log del candidato è almeno aggiornato quanto il proprio (confronto per `LastLogTerm`, a parità per `LastLogIndex`). Il voto è valido solo dopo la persistenza di `votedFor`.

**REQ-VOT-5** Il nodo DOVREBBE ignorare un `RequestVote` ricevuto entro `ElectionTimeoutMinMs` dall'ultimo `AppendEntries` valido del Leader, senza aggiornare il proprio term.

**REQ-VOT-6** In caso di voto conteso nessuno vince: allo scadere di un nuovo timeout casuale si ripete il ciclo con un nuovo term.

**REQ-VOT-7 (quorum senza attese).** PreVote e RequestVote DEVONO concludere il round appena è raggiunto il quorum, senza attendere i membri che non rispondono; le risposte tardive DEVONO comunque essere elaborate (term più alto). Un membro spento non deve allungare i round di elezione.

## 8. Leader, no-op e lease

**REQ-LDR-1** Dopo l'elezione il Leader DEVE appendere una voce `NoOp` nel proprio term, replicata e committata come una voce normale.

**REQ-LDR-2** Il Leader NON DEVE considerare committed una voce di un term precedente solo contando i nodi che la possiedono. Una voce del term corrente committata rende committed anche le voci che la precedono.

**REQ-LDR-3** Il Leader DEVE mantenere per ogni Follower l'istante (clock monotono) di **invio** dell'ultimo `AppendEntries` per cui ha ricevuto risposta valida. Il **lease** è valido finché un quorum (Leader incluso) ha risposto a messaggi inviati negli ultimi `LeaderLeaseMs`.

**REQ-LDR-4** Se il lease scade il Leader DEVE immediatamente smettere di accettare scritture, passare a Follower e NON produrre nuovi commit.

**REQ-LDR-5** Il lease è una protezione di disponibilità, NON la fonte primaria della safety Raft. La safety NON DEVE dipendere da timestamp UTC sincronizzati; tutti i timer DEVONO usare clock monotoni locali.

**REQ-LDR-6** DEVE valere `LeaderLeaseMs < ElectionTimeoutMinMs`, così che il vecchio Leader si ritiri prima che i Follower possano eleggerne un altro.

## 9. Replica del log

Ogni voce contiene:

```text
Index, Term, EntryType (NoOp | Data), RequestId, PayloadType, PayloadHash, Payload, ReceivedUtc
```

- `PayloadType` è il nome dell'endpoint che ha ricevuto la richiesta (REQ-EPT-2); è vuoto per le voci `NoOp`.
- `PayloadHash` è definito in REQ-IDM-8.
- `ReceivedUtc` è assegnato dal Leader e replicato. È usato esclusivamente per la scadenza delle voci obsolete (REQ-SM-6), mai per il consenso.

**REQ-REP-1** Il Leader DEVE mantenere per ogni Follower `nextIndex`, `matchIndex` e l'istante di ultima conferma. `matchIndex` è l'ultimo indice noto come persistito durevolmente sul Follower.

**REQ-REP-2** `AppendEntries` (`POST /internal/v1/append`) contiene `Term`, `LeaderId`, `PrevLogIndex`, `PrevLogTerm`, `Entries[]`, `LeaderCommit`, `LeaderApplied` (ultimo indice applicato al database, che consente la compattazione anche sui Follower) e `MemberUrls` (gossip, REQ-DIS-4). La risposta contiene `Term`, `Success`, `MatchIndex` e un suggerimento di conflitto (`ConflictIndex`, `ConflictTerm`).

**REQ-REP-3** Il Follower DEVE: (1) rifiutare term inferiori; (2) aggiornare e persistere un term maggiore; (3) verificare `PrevLogIndex/PrevLogTerm`; (4) in caso di conflitto eliminare la voce in conflitto e **tutte le successive**, sempre e solo non committed; (5) aggiungere le nuove voci; (6) persisterle durevolmente prima di rispondere; (7) rispondere con l'indice massimo effettivamente persistito; (8) aggiornare `commitIndex = min(LeaderCommit, indice dell'ultima voce nuova)`. Il valore NON DEVE mai superare l'ultima voce verificata come coerente con il Leader.

**REQ-REP-4** Una voce committed NON PUÒ essere sovrascritta.

**REQ-REP-5** Una voce è committed quando il Leader l'ha persistita, un quorum di nodi (Leader incluso) l'ha persistita durevolmente, e vale REQ-LDR-2.

**REQ-REP-6** `commitIndex` e `LastApplied` sono distinti. Il Leader DEVE applicare in ordine esclusivamente le voci `LastApplied+1 … commitIndex`. Una voce non committed NON DEVE produrre effetti applicativi.

**REQ-REP-7** Gli `AppendEntries` vuoti inviati ogni `HeartbeatIntervalMs` sono l'heartbeat. Il Follower azzera il timer di elezione solo su `AppendEntries` valido del Leader del term corrente.

## 10. Persistenza crash-safe

**REQ-WAL-1** Il WAL DEVE essere append-only e diviso in segmenti. Ogni record contiene: magic, versione del formato, tipo di record, lunghezza, `Index`, `Term`, corpo, checksum CRC32C. Un record è valido solo se checksum e lunghezza sono coerenti. Il troncamento di voci in conflitto è un record `Truncate` appeso: i file non vengono riscritti. Il formato corrente è il **2** (corpo con `PayloadType`, REQ-EPT-2).

**REQ-WAL-2** Per ottenere la persistenza il nodo DEVE usare `FileStream.Flush(true)` (equivalente a fsync su Windows e Linux). Alla creazione di un segmento DEVE essere sincronizzata anche la directory. Il group commit (più voci per fsync, fuori dal lock globale) PUÒ essere usato, purché nessuna conferma preceda l'fsync.

**REQ-WAL-3** Recovery all'avvio, in ordine:

1. lettura di `snapshot.json` e validazione dei segmenti;
2. un segmento in un **altro formato** del WAL DEVE bloccare l'avvio (codice 2), senza troncamenti (REQ-VER-2);
3. troncamento di un record finale incompleto dell'ultimo segmento;
4. un record corrotto **non finale** DEVE portare il nodo in stato `unhealthy` con richiesta di recovery esplicita (il nodo non vota e non accetta voci);
5. ricostruzione del log e dell'indice delle richieste della finestra non compattata (§12);
6. `commitIndex = max(valore persistito, SnapshotIndex)`;
7. nessun apply all'avvio: lo esegue il Leader, leggendo `LastApplied` dal database. L'avvio NON dipende dalla raggiungibilità del database.

**REQ-WAL-4** Il nodo NON DEVE diventare Ready prima del completamento del recovery.

**REQ-WAL-5** Se il nodo non può garantire la durabilità (disco pieno, errore di I/O, spazio libero sotto `MinFreeBytes`): NON accetta nuove scritture, NON conferma `AppendEntries` come persistiti, NON vota, risulta `unhealthy`, genera un evento ad alta severità e, se Leader, passa a Follower.

## 11. State machine sul database condiviso

La state machine è un database **condiviso** tra i nodi (`IDataRepository`). Interfaccia:

```csharp
Task InitializeAsync(CancellationToken ct);
Task<long> GetLastAppliedIndexAsync(CancellationToken ct);
Task<ApplyOutcome> ApplyAsync(LogEntry entry, ApplyContext ctx, CancellationToken ct);
Task<ApplyOutcome> QuarantineAsync(LogEntry entry, string reason, ApplyContext ctx, CancellationToken ct);
Task<ApplyOutcome?> GetOutcomeAsync(string requestId, CancellationToken ct);
bool IsTransient(Exception ex);
```

`ApplyOutcome` contiene l'esito (`Applied`, `Duplicate`, `Conflict`, `Rejected`, `Expired`, `Quarantined`, `NoOp`), lo status HTTP e il body della risposta, ed è registrato nel database.

Provider: `SqlServer` (produzione) e `SharedFile` (solo sviluppo e test sulla stessa macchina: un file `applied.log` condiviso, con un lock tra processi al posto della transazione).

**REQ-SM-1 (apply condizionale).** Ogni apply DEVE essere una transazione che: (1) blocca il cursore `LastApplied` del cluster e verifica `LastApplied = LogIndex − 1`; (2) decide l'esito; (3) per una scrittura esegue il writer dell'endpoint (REQ-EPT-4); (4) registra l'esito; (5) avanza `LastApplied`. Una voce con `LogIndex ≤ LastApplied` restituisce l'esito già registrato (replay); una voce con un predecessore mancante richiede il riallineamento del cursore. Un ex Leader o un nodo rientrato dopo settimane non può quindi riscrivere dati già scritti.

**REQ-SM-2** **Solo il Leader** applica le voci committed al database. I Follower non applicano.

**REQ-SM-3** Il cursore `LastApplied` è partizionato per `ClusterId`: più cluster possono condividere lo stesso database.

**REQ-SM-4** La state machine NON DEVE produrre effetti esterni irreversibili. I writer NON DEVONO eseguire effetti esterni (email, chiamate HTTP) e NON DEVONO aprire transazioni proprie né fare commit.

**REQ-SM-5 (poison entry).** Il Leader DEVE classificare gli errori di apply:
- **permanente di business** (errore sollevato dal writer, JSON non valido, violazione di vincolo, troncamento, conversione) → esito `Rejected` (422) registrato, `LastApplied` avanza;
- **transitorio** (timeout, deadlock, connessione, failover del database) → ritentato senza limite con backoff limitato da `ApplyRetryMaxDelayMs`; nel frattempo il Leader è Not Ready;
- **altro** → ritentato `MaxApplyAttempts` volte, poi esito `Quarantined` (422) registrato con il payload in una tabella di quarantena, e `LastApplied` avanza.

Il cluster NON DEVE fermarsi per una singola voce.

**REQ-SM-6 (voci obsolete).** Se all'apply `now − ReceivedUtc > MaxPendingAgeMinutes`, la voce NON DEVE essere scritta: esito `Expired` (410) registrato. La decisione è presa una sola volta dall'applicatore e registrata nella transazione condizionale, quindi è deterministica. Poiché la risposta 200 arriva solo dopo l'apply, una voce scaduta non ha mai ricevuto 200. La documentazione client DEVE fissare l'intervallo massimo di retry sotto questa soglia.

**REQ-SM-7 (ripristino del database).** Se `LastApplied` del database precede l'inizio del log compattato, il Leader DEVE segnalarlo con un evento di severità alta e NON DEVE applicare voci fuori sequenza.

**REQ-SM-8 (schema).** Il database contiene le tabelle di servizio (`SchemaVersion`, `ApplyState`, `AppliedEntries`, `Requests`, `Quarantine`) nello schema configurato, più una tabella per ogni endpoint che usa il writer di default. Con `AutoCreateSchema=true` il servizio le crea; altrimenti si usano gli script `sql/001_init.sql` e `sql/002_endpoint_table.sql`. La versione dello schema è regolata da REQ-VER-2.

## 12. Idempotenza

**REQ-IDM-1** `X-Request-ID` è obbligatorio e DEVE essere un UUID valido; altrimenti la risposta è 400.

**REQ-IDM-2** L'indice delle richieste è **permanente** e risiede nel database (tabella `Requests`): registra la prima occorrenza di ogni `RequestId` con `PayloadType`, `PayloadHash`, `LogIndex` ed esito. È indipendente dalla compattazione del log. Il nodo mantiene in memoria solo l'indice della finestra di log non compattata, per le richieste ancora in corso.

**REQ-IDM-3** Comportamento del Leader alla ricezione di una richiesta, in base allo stato del `RequestId`:

| Stato | Significato | Comportamento |
| --- | --- | --- |
| Unknown | ID mai visto | Appende una nuova voce. |
| Pending | Voce nel log, non ancora applicata | La richiesta attende l'esito della stessa voce; NON crea una seconda voce. |
| Applied | Esito registrato nel database | Esito originale con `X-Idempotent-Replay: true`, senza nuova voce. |
| Qualsiasi, con `PayloadHash` diverso | ID riusato con payload o endpoint diverso | 422, nessuna scrittura. |

**REQ-IDM-4** *Dedup deterministica in apply (difesa in profondità).* Se all'apply la tabella `Requests` contiene già il `RequestId`: con lo stesso `PayloadHash` la voce ha esito `Duplicate` e restituisce l'esito della prima; con un `PayloadHash` diverso ha esito `Conflict` (422). In entrambi i casi non scrive dati.

**REQ-IDM-7** Crash dopo il commit e prima della risposta: il nuovo Leader applica la voce (o trova l'esito già registrato) e restituisce l'esito originale al retry. Crash prima del commit su quorum: la voce può essere scartata; il client non ha ricevuto 200, ritenta con lo stesso ID e il dato viene scritto una sola volta.

**REQ-IDM-9** *Scope dell'ID.* `RequestId` è unico per cluster, **su tutti gli endpoint** (REQ-EPT-3).

**REQ-IDM-8** Il `PayloadHash` DEVE essere SHA-256 di `nome dell'endpoint ‖ 0x00 ‖ byte grezzi del body`. Il body DEVE essere letto una sola volta e usato sia per l'hash sia per la voce di log, senza ri-serializzazione. Un ritento con lo stesso `RequestId` ma body diverso anche di un solo byte, o su un altro endpoint, ottiene quindi 422: il client DEVE reinviare lo stesso body byte per byte allo stesso endpoint.

## 13. Compattazione, snapshot e capacità

**REQ-SNP-1** Il WAL DEVE essere compattato: quando le voci già applicate superano `Log:RetainEntries` (e la parte compattabile supera `Log:CompactionBatch`), il nodo scrive in modo durevole `snapshot.json` con `SnapshotIndex` e `SnapshotTerm`, poi elimina i segmenti interamente precedenti e libera la memoria corrispondente. Poiché lo stato applicativo vive nel database, lo snapshot NON contiene dati.

**REQ-SNP-2** Un Follower più indietro dello snapshot del Leader DEVE essere riallineato con `InstallSnapshot` (`POST /internal/v1/snapshot`): scarta il prefisso del log fino a `SnapshotIndex` e riprende la replica da lì.

**REQ-SNP-3** Il nodo DEVE esporre `storageAvailableBytes` e generare allarmi al 70% e all'85% di occupazione. Sotto `MinFreeBytes` (default 1 GiB) applica REQ-WAL-5 prima che il disco si riempia.

**REQ-SNP-4** Il documento di deploy DEVE riportare il dimensionamento atteso: circa `(RetainEntries + voci non ancora applicate) × dimensione media della richiesta`, più un segmento in scrittura (`SegmentBytes`). La RAM usata dal log è proporzionale alla stessa finestra, non alla storia del cluster.

## 14. API pubbliche, endpoint di ingestione e contratto con il client

| Endpoint | Descrizione |
| --- | --- |
| `POST <Endpoints[i].Path>` | Ingestione di un tipo di dato (REQ-EPT-1). Header `X-Request-ID` (UUID) obbligatorio. Body JSON, max `MaxBodyBytes`. |
| `GET /health/live`, `GET /health/ready` | Vedi §17. |
| `GET /api/v1/cluster/status` | `nodeId`, ruolo, term, Leader noto, `commitIndex`, `lastApplied`, snapshot, `membersHash`, `protocolVersion`, endpoint configurati, `endpointsHash`, `endpointsMismatch`, stato di storage e database, peer con le fonti dei loro URL. |
| `GET /api/v1/metrics` | Metriche (REQ-OPS-3). |
| `POST /internal/v1/prevote`, `/vote`, `/append`, `/snapshot`, `/hello` | Solo tra nodi, autenticate (§16). |

### 14.1 Endpoint di ingestione

Il servizio espone N endpoint REST, ognuno dei quali riceve un JSON di forma diversa diretto a una diversa destinazione nel database. Il protocollo Raft non distingue gli endpoint: tutti alimentano **un solo log**, con un solo ordine totale e un solo cursore `LastApplied`.

**REQ-EPT-1 (configurazione).** Gli endpoint DEVONO essere dichiarati in `RaftCluster:Endpoints`, almeno uno e al massimo 64. Ogni endpoint ha:
- `Name`: identificatore stabile, registrato in ogni voce del log (`[a-z0-9][a-z0-9_-]{0,63}`, unico);
- `Path`: path assoluto del `POST`, unico senza distinzione tra maiuscole e minuscole, con segmenti di caratteri non riservati, senza parametri né `/` finale, fuori dai prefissi riservati `/internal`, `/health`, `/api/v1/cluster`, `/api/v1/metrics`;
- `Table`: tabella `schema.tabella` del writer JSON di default, unica per endpoint e diversa dalle tabelle di servizio; obbligatoria con `Provider=SqlServer` se per quel nome non è registrato un writer personalizzato.

Una configurazione non valida, un writer registrato senza nome di endpoint o per un nome assente da `Endpoints` impediscono l'avvio (codice 1). La configurazione degli endpoint DEVE essere identica su tutti i nodi. Un path non configurato risponde 404 senza creare voci.

**REQ-EPT-2 (tipo nella voce).** Ogni voce `Data` DEVE contenere il nome dell'endpoint (`PayloadType`), scritto nel WAL e replicato con la voce. L'applicatore sceglie il writer da `PayloadType`, mai dal contesto HTTP, perché il nodo che applica può essere diverso da quello che ha ricevuto la richiesta.

**REQ-EPT-3 (idempotenza globale).** `RequestId` resta unico per cluster, indipendentemente dall'endpoint. Il tipo è incluso nell'hash (REQ-IDM-8): un `RequestId` già usato su un altro endpoint produce `Conflict` (422) e nessuna scrittura.

**REQ-EPT-4 (apply per tipo).** L'apply DEVE eseguire, nella stessa transazione condizionale su `LastApplied`, il writer associato a `PayloadType`: un writer personalizzato registrato con il nome dell'endpoint, oppure il writer JSON di default sulla `Table` dell'endpoint. Se l'applicatore non ha un writer per quel tipo (configurazione disallineata o endpoint rimosso con voci in sospeso), la voce DEVE ricevere l'esito `Rejected` (422), `LastApplied` avanza, e l'evento è contato in `applyUnknownType`: l'errore è deterministico e non va ritentato.

**REQ-EPT-5 (inoltro).** Il Follower DEVE inoltrare la richiesta al Leader **sullo stesso path dell'endpoint**. La firma HMAC copre il path (REQ-SEC-3), quindi un inoltro non può essere dirottato su un altro endpoint.

**REQ-EPT-6 (ordine condiviso).** Le voci di tutti gli endpoint sono applicate in un unico ordine. Conseguenze accettate:
- il throughput di apply è condiviso;
- una voce lenta o in errore di un endpoint ritarda le altre fino all'esito (al più `MaxApplyAttempts` tentativi per un errore non classificato; senza limite per un errore transitorio del database);
- le scritture di endpoint diversi rispettano l'ordine di commit.

Quando serve isolamento si avviano **cluster distinti**, ciascuno con un proprio `ClusterId` e i propri endpoint, anche sullo stesso database (REQ-SM-3).

### 14.2 Risposte e contratto con il client

**REQ-API-1** Il formato delle risposte è definito da questa specifica (tabella di REQ-API-2 e body JSON degli esiti). Lo schema del JSON di ciascun endpoint è definito dall'applicazione che lo usa: il writer lo valida e lo rifiuta con `PayloadRejectedException` (422). Il writer JSON di default accetta solo un oggetto JSON.

**REQ-API-2** Codici di risposta:

| Codice | Significato |
| --- | --- |
| 200 | Richiesta committed e applicata, oppure replay idempotente (`X-Idempotent-Replay: true`). |
| 400 | `X-Request-ID` mancante o non UUID, oppure JSON sintatticamente non valido. |
| 404 | Path che non corrisponde a nessun endpoint configurato. |
| 410 | Voce rimasta in sospeso oltre `MaxPendingAgeMinutes` e non scritta (REQ-SM-6). |
| 413 | Body oltre `MaxBodyBytes` (rifiutato prima dell'inoltro). |
| 421 | Solo tra nodi: destinatario sbagliato o anti-loop (§15). Il client NON lo vede mai. |
| 422 | `RequestId` già associato a un payload o a un endpoint diverso; dato rifiutato (`rejected`); voce in quarantena (`quarantined`). |
| 503 | Nessun Leader con quorum entro il deadline, sovraccarico, oppure leadership persa prima dell'esito. Include `Retry-After: 1`. |
| 504 | Richiesta inoltrata, esito non osservato entro il deadline. |

**REQ-API-3** Un timeout, un 503 o un 504 NON implicano che l'operazione non sia stata scritta.

**REQ-CLI-1** Il client DEVE: generare il `RequestId` una sola volta per operazione logica e riusarlo in ogni ritento insieme allo stesso endpoint e allo stesso body byte per byte; inviare a qualsiasi nodo e, in caso di errore di connessione, provarne un altro; ritentare su errore di rete, timeout, 503 e 504 (non su 4xx) con backoff esponenziale e jitter; smettere di ritentare prima di `MaxPendingAgeMinutes`.

**REQ-CLI-2** Il timeout del client DEVE essere almeno `RequestDeadlineMs` + 5 s (15 s di default). Il client NON deve conoscere il Leader né interpretare la causa degli errori.

## 15. Smart Proxy e deadline

**REQ-PRX-1** Un Follower DEVE inoltrare ogni richiesta di ingestione al Leader noto in modo trasparente per il client, sullo stesso path (REQ-EPT-5).

**REQ-PRX-2** Il Follower DEVE bufferizzare il body (fino a `MaxBodyBytes`, oltre risponde 413) per poterlo reinviare in caso di ritentativo.

**REQ-PRX-3** Il proxy DEVE preservare metodo, body, header applicativi (esclusi gli header hop-by-hop e quelli interni) e `X-Request-ID`, e NON DEVE generare un nuovo `RequestId`. L'inoltro è **firmato** dal Follower (REQ-SEC-3) e porta `X-Forwarded-By`, `X-Forward-Hops` e il tempo residuo del deadline (`X-Raft-Deadline-Ms`), anch'essi coperti dalla firma insieme al `RequestId`. Il ricevente considera validi gli header di inoltro **solo** se la firma è valida; altrimenti li ignora e tratta la richiesta come proveniente da un client (metrica `forwardUntrustedHeaders`).

**REQ-PRX-4** Il Leader noto è il `LeaderId` dell'ultimo `AppendEntries` valido del term corrente; il suo URL è preso dal registro dei peer (REQ-DIS-4). Senza Leader noto il Follower attende.

**REQ-PRX-5** Su fallimento dell'inoltro (connessione rifiutata, timeout del tentativo, 503, 421, 504) il Follower DEVE attendere un nuovo Leader e ritentare con backoff da 100 ms a 500 ms con jitter, entro il deadline. Il retry è sicuro grazie all'idempotenza.

**REQ-PRX-6** Anti-loop: un nodo che riceve un inoltro autenticato con `X-Forward-Hops` maggiore o uguale a `MaxForwardHops` e non è Leader DEVE rispondere 421 al richiedente senza inoltrare ulteriormente. Un client che invia questi header senza firma non riceve mai 421.

**REQ-PRX-7** Il Follower DEVE restituire al client status, header e body del Leader senza modifiche (esclusi gli header interni) e NON DEVE trasformare una risposta del Leader in un successo locale.

**REQ-PRX-8** Esiste un unico deadline per richiesta, `RequestDeadlineMs`, condiviso da tutti i retry interni e **propagato** al Leader nell'inoltro. `FailoverWaitMs` è il tempo massimo di attesa di un Leader all'interno del deadline. A scadenza: 503 se la richiesta non è mai stata consegnata a un Leader; 504 se è stata inoltrata ed esito non osservato. Il proxy NON DEVE ritentare oltre il deadline.

**REQ-PRX-9** Percorso sul Leader: valida, controlla l'idempotenza (§12), appende con il group commit, replica, attende il commit, applica al database, risponde con l'esito registrato. Se perde la leadership prima dell'esito, una richiesta proveniente direttamente da un client attende il nuovo Leader entro il deadline; un inoltro riceve 503 e viene ritentato dal Follower.

**REQ-PRX-10** Il nodo DEVE limitare le richieste in corso a `MaxConcurrentRequests` (default 256). Oltre la soglia risponde 503 con `Retry-After: 1` prima di leggere il body. Il body DEVE essere letto una sola volta in un buffer riutilizzabile e usato per `PayloadHash`, voce di log e ogni tentativo di inoltro. Il dimensionamento DOVREBBE garantire che `MaxConcurrentRequests × MaxBodyBytes` stia nella memoria disponibile.

## 16. Sicurezza

**REQ-SEC-1** `ClusterSecret` (almeno 16 caratteri) NON DEVE essere presente nel repository; DEVE essere fornito da variabile d'ambiente (`CLUSTER_SECRET`) o secret store. Un segreto `dev-only-…` NON DEVE essere accettato fuori dall'ambiente Development. La stringa di connessione al database segue la stessa regola (`RAFT_SQL_CONNECTION`).

**REQ-SEC-2** Gli annunci UDP DEVONO usare HMAC-SHA256 su tutti i campi, inclusa `ProtocolVersion` (§5).

**REQ-SEC-3** Le RPC interne e gli inoltri DEVONO essere autenticati con HMAC su: versione di protocollo, metodo HTTP, path, timestamp, hash del body, `NodeId` mittente, nonce, `NodeId` **destinatario** e, per gli inoltri, hop, deadline e `RequestId`. Il ricevente DEVE rifiutare con 421 una richiesta destinata a un altro nodo. Ogni **risposta** DEVE essere firmata dal risponditore su versione di protocollo, `NodeId` del risponditore, nonce della richiesta, status, impronta degli endpoint e hash del body: una risposta non firmata, firmata da un altro nodo o non legata al nonce viene scartata (metriche `rpcResponseAuthFailed`, `rpcWrongDestination`).

**REQ-SEC-4** Anti-replay delle RPC: nonce univoco per mittente conservato in cache per almeno il doppio della finestra temporale, più una finestra del timestamp pari a `RpcClockToleranceMs` (default 60000). Le RPC fuori finestra sono rifiutate e contate in `rpcClockSkewRejected`.

**REQ-SEC-5** *Dipendenza dagli orologi.* La **safety** NON dipende dagli orologi: nessuna decisione Raft usa timestamp. La **disponibilità** delle RPC sì: un nodo con orologio oltre la tolleranza risulta isolato senza violare la safety. Gli operatori DEVONO mantenere gli orologi sincronizzati (NTP/chrony).

**REQ-SEC-6** Le RPC sono firmate ma non cifrate: in reti non fidate è raccomandato TLS di rete (proxy o mTLS).

**REQ-SEC-7** L'autenticazione dei client degli endpoint di ingestione è fuori ambito, ma DEVE essere implementabile senza modificare il protocollo Raft. Gli endpoint DOVREBBERO essere esposti solo dietro un gateway.

**REQ-SEC-8** Gli endpoint informativi (`/api/v1/cluster/status`, `/api/v1/metrics`) DEVONO rispondere 403 alle richieste non da loopback, salvo `Management:ExposeStatusEndpoints=true`. Gli endpoint `/health/*` non sono ristretti.

## 17. Health, osservabilità, shutdown e piattaforme

**REQ-OPS-1** `GET /health/live` DEVE rispondere 200 se il processo è vivo, anche in assenza di quorum.

**REQ-OPS-2** `GET /health/ready` DEVE rispondere 200 solo se il nodo può svolgere il ruolo dichiarato: un Follower è Ready se ha completato il recovery e conosce un Leader attivo; un Leader è Ready solo con lease valido, storage sano e database raggiungibile e utilizzabile (schema compatibile). Un nodo con storage non affidabile o in shutdown DEVE essere Not Ready.

**REQ-OPS-3** Metriche minime:

```text
currentTerm, role, commitIndex, lastApplied, logLength, snapshotIndex, logWindowEntries, walSegments,
idempotencyWindowEntries, applyLag, replicationLag,
electionCount, prevoteDenied, stepDownCount,
requestCount, requestRetryCount, requestDuplicateCount, requestFailureCount, requestRejectedOverload,
writeBatches, walFsyncCount, walFsyncLatencyAvgUs, walFsyncLatencyMaxUs, walWriteErrors, storageAvailableBytes,
applyErrors, applyTransientErrors, applyRejected, applyExpired, applyQuarantined, applyUnknownType,
compactions, snapshotsSent, snapshotsInstalled,
udpInvalidHmac, udpReplayRejected, clusterIdMismatch, membersHashMismatch, nonMemberRejected,
rpcInvalidAuth, rpcReplayRejected, rpcClockSkewRejected, rpcWrongDestination, rpcResponseAuthFailed,
rpcProtocolMismatch, endpointsHashMismatch, forwardUntrustedHeaders, peerUrlUnknown, probeRounds, probeFound
```

**REQ-OPS-4** Ogni log applicativo DEVE includere, quando disponibili: `NodeId`, `Term`, `Role`, `RequestId`, endpoint, `LogIndex`.

**REQ-OPS-5** Shutdown (SIGTERM / stop del servizio):

1. il nodo smette di accettare nuove scritture (503) e risulta Not Ready;
2. completa le operazioni in corso entro `ShutdownGraceMs`;
3. NON DEVE perdere una voce già committed;
4. il Leader smette di inviare heartbeat, così i Follower eleggono subito un nuovo Leader (una cessione esplicita con `TimeoutNow` è opzionale e non necessaria alla correttezza);
5. termina dopo il grace period.

**REQ-OPS-6 (piattaforme).** Il servizio DEVE essere eseguibile come processo console, Windows Service e unità systemd (`Type=notify`). I percorsi relativi sono risolti rispetto a `BaseDirectory`, alla directory dell'eseguibile (servizio) o alla directory corrente.

**REQ-OPS-7 (thread pool).** All'avvio il nodo DEVE impostare un minimo di thread del pool adeguato, perché fsync e lock sincroni non introducano pause tali da far scadere il lease.

**REQ-OPS-8 (codici di uscita).** Un avvio fallito DEVE terminare con: 1 configurazione non valida; 2 avvio bloccato (stato assente senza `AllowEmptyState`, `DataDir` di un altro `NodeId`, WAL di un altro formato); 3 `DataDir` in uso; 4 nessuna porta libera.

**REQ-DEV-1 (cluster di sviluppo).** Solo nell'ambiente Development il processo avviato PUÒ lanciare gli altri membri come processi figli (`DevCluster:Instances`). I figli DEVONO terminare quando il padre termina.

## 18. Versioni e compatibilità

**REQ-VER-1 (versione di protocollo).** Ogni RPC tra nodi e ogni annuncio UDP DEVONO contenere `ProtocolVersion` (header `X-Cluster-Protocol` per le RPC), incluso nella firma HMAC di richieste, risposte e annunci. Un nodo DEVE rifiutare le RPC (409) e ignorare gli annunci con una versione diversa dalla propria (metrica `rpcProtocolMismatch`): un nodo di versione incompatibile non vota, non riceve voci e non conta nel quorum. La versione DEVE essere incrementata a ogni modifica incompatibile di RPC, voci o WAL; un aggiornamento che la cambia richiede il riavvio di tutti i nodi.

Ogni RPC e ogni risposta firmata contengono anche `EndpointsHash`, l'impronta della configurazione degli endpoint (nome, path e tabella). Un valore diverso NON blocca le RPC, così da permettere un aggiornamento della configurazione un nodo alla volta, ma DEVE essere segnalato (log, metrica `endpointsHashMismatch` con il numero di membri discordanti, campo `endpointsMismatch` dello stato). Ogni nodo confronta solo le impronte dei membri con cui scambia RPC: il **Leader** le vede tutte, un Follower vede quella del Leader. Lo stato di riferimento è quindi quello del Leader.

**REQ-VER-2 (dati di formato precedente).** Non è prevista migrazione dalla v0.2.
- Un WAL in un altro formato DEVE bloccare l'avvio (codice 2) con un messaggio che indica di svuotare la `DataDir`, senza troncarlo.
- Lo schema del database ha una versione (tabella `SchemaVersion`, attuale: 2). Se il database contiene tabelle di servizio senza `SchemaVersion` o di una versione diversa, il Leader NON DEVE applicare: segnala l'errore in `stateMachineError` e `/health/ready` risulta Not Ready.
- Un `applied.log` della state machine SharedFile in un altro formato DEVE essere rifiutato allo stesso modo.

Versioni correnti: protocollo 2, WAL formato 2, schema 2.

## 19. Configurazione baseline

Sezione `RaftCluster` della configurazione. Valori sovrascrivibili da variabili d'ambiente (`RaftCluster__Sezione__Chiave`) e da riga di comando.

| Parametro | Default | Note |
| --- | --- | --- |
| `ClusterId` | obbligatorio | Isola cluster diversi nella stessa rete e nello stesso database. |
| `ClusterSize` | obbligatorio | Da 1 a 7. Determina il quorum. |
| `ClusterSecret` | obbligatorio | Da `CLUSTER_SECRET` o secret store. |
| `MembershipMode` | Static | `Static` o `Bootstrap` (§3). |
| `NodeId` | obbligatorio in Static | GUID del nodo. |
| `Members` | obbligatorio in Static | `ClusterSize` `NodeId` distinti. |
| `NodeIdFile` | ./data/node-id | Solo in Bootstrap. |
| `BaseDirectory` | — | Base dei percorsi relativi (REQ-OPS-6). |
| `RestPort` | 5000 | |
| `RestPorts` | — | Elenco di porte (REQ-NET-6); prevale su `RestPort`. |
| `AdvertiseUrl` | automatico | Supporta `{port}`. |
| `BindInterface` | automatico | |
| `UdpMode` | Broadcast | `Broadcast`, `Multicast` o `Disabled`. |
| `UdpBroadcastAddress` / `UdpMulticastAddress` | 255.255.255.255 / 239.255.42.99 | |
| `UdpDiscoveryPort` | 8888 | |
| `AnnounceIntervalMs` / `AnnounceTimeoutMs` | 1000 / 4000 | |
| `DiscoveryPhaseMs` | 2000 | Solo all'avvio. |
| `Peers` | {} | `NodeId` → URL. |
| `Discovery:SeedHosts` | 127.0.0.1 | Host della discovery a sondaggio (REQ-DIS-7). |
| `Discovery:ProbePorts` | = `RestPorts` | |
| `Discovery:ProbeEnabled` | true | |
| `Discovery:ProbeIntervalMs` / `ProbeIdleIntervalMs` / `ProbeTimeoutMs` | 2000 / 30000 / 500 | |
| `Discovery:UnreachableAfterFailures` | 3 | |
| `HeartbeatIntervalMs` | 250 | |
| `ElectionTimeoutMinMs` / `MaxMs` | 1000 / 1800 | |
| `LeaderLeaseMs` | 750 | |
| `ReplicationTimeoutMs` | 1500 | |
| `RpcConnectTimeoutMs` | 250 | Timeout della sola connessione TCP verso un peer. |
| `FailoverWaitMs` | 8000 | Entro `RequestDeadlineMs`. |
| `RequestDeadlineMs` | 10000 | |
| `MaxBodyBytes` | 1048576 | Massimo 64 MiB. |
| `MaxForwardHops` | 1 | |
| `MaxConcurrentRequests` | 256 | |
| `RpcClockToleranceMs` | 60000 | |
| `ShutdownGraceMs` | 5000 | |
| `Endpoints` | — | Almeno uno: `Name`, `Path`, `Table` (REQ-EPT-1). |
| `Storage:DataDir` | ./data/{nodeId} | WAL e stato Raft. Supporta `{nodeId}` e `{port}`. |
| `Storage:FsyncOnCommit` | true | `false` consentito solo con `TestMode`. |
| `Storage:MinFreeBytes` | 1073741824 | §13. |
| `Storage:AllowEmptyState` | false | REQ-ID-3. |
| `Log:SegmentBytes` | 67108864 | |
| `Log:RetainEntries` | 10000 | Voci applicate conservate per i Follower in ritardo. |
| `Log:CompactionBatch` / `CompactionIntervalMs` | 5000 / 5000 | |
| `StateMachine:Provider` | SharedFile | `SqlServer` in produzione. |
| `StateMachine:MaxPendingAgeMinutes` | 1440 | REQ-SM-6; 0 disattiva. |
| `StateMachine:MaxApplyAttempts` | 10 | REQ-SM-5. |
| `StateMachine:ApplyRetryMaxDelayMs` | 5000 | |
| `StateMachine:SharedFile:Directory` | ./shared-state | Solo sviluppo e test. |
| `StateMachine:SqlServer:ConnectionString` | — | Da `RAFT_SQL_CONNECTION`. |
| `StateMachine:SqlServer:Schema` | raft | Schema delle tabelle di servizio. |
| `StateMachine:SqlServer:CommandTimeoutSeconds` | 30 | |
| `StateMachine:SqlServer:AutoCreateSchema` | true | REQ-SM-8. |
| `Management:ExposeStatusEndpoints` | false | REQ-SEC-8. |
| `DevCluster:Instances` / `ChildOutput` | 1 / Auto | Solo Development (REQ-DEV-1). |

**REQ-CFG-1** I valori DEVONO essere validati all'avvio; se un vincolo è violato il nodo NON DEVE partire (codice 1) e DEVE elencare tutte le violazioni:

```text
ElectionTimeoutMinMs >= 4 * HeartbeatIntervalMs
ElectionTimeoutMaxMs - ElectionTimeoutMinMs >= 2 * HeartbeatIntervalMs
3 * HeartbeatIntervalMs <= LeaderLeaseMs < ElectionTimeoutMinMs
ReplicationTimeoutMs < RequestDeadlineMs
20 <= RpcConnectTimeoutMs < ReplicationTimeoutMs
FailoverWaitMs < RequestDeadlineMs
AnnounceTimeoutMs >= 3 * AnnounceIntervalMs
1 <= ClusterSize <= 7
1 <= MaxBodyBytes <= 64 MiB; MaxConcurrentRequests >= 1; MaxForwardHops >= 1
Static: Members contiene esattamente ClusterSize NodeId distinti, incluso il proprio
RestPorts e Discovery:ProbePorts in formato valido; Discovery: ProbeIntervalMs >= 200, ProbeIdleIntervalMs >= ProbeIntervalMs, ProbeTimeoutMs >= 50
1 <= DevCluster:Instances <= 7
FsyncOnCommit = false solo con TestMode
Log: SegmentBytes >= 1 MiB, RetainEntries >= 100, CompactionBatch >= 1, CompactionIntervalMs >= 100
StateMachine: MaxPendingAgeMinutes >= 0 e, se > 0, oltre RequestDeadlineMs; MaxApplyAttempts >= 1; ApplyRetryMaxDelayMs >= 100
SqlServer: ConnectionString presente; Schema e Table nomi SQL validi
Endpoints: REQ-EPT-1
```

## 20. Failover e requisiti non funzionali

**REQ-FOV-1** Il tempo di failover si misura dall'istante di arresto effettivo del Leader alla prima richiesta nuova committed, applicata e risposta con 200 dal nuovo Leader. Il test usa una sonda che invia ogni 50 ms una scrittura nuova (nuovo `RequestId`) a un Follower.

| Fase | Tempo atteso |
| --- | --- |
| Rilevazione (timeout di elezione dei Follower) | 1,0 - 1,8 s |
| PreVote e votazione su LAN | < 0,3 s |
| Commit della no-op e lettura di `LastApplied` dal database | < 0,3 s |
| **Totale tipico** | **~1,5 - 2,4 s** |
| Voto conteso (un secondo giro) | fino a ~4 s |

**REQ-FOV-2** Criterio di accettazione: su 30 prove con 3 nodi, p95 ≤ 3 s e massimo ≤ 5 s. Le prove DEVONO essere eseguite e riportate separatamente per: kill immediato, SIGSTOP del Leader, perdita di rete del Leader, crash del processo, perdita dello storage (errore di I/O iniettato), partizione 1+2 (tempo misurato lato maggioranza).

**REQ-NFR-1** Baseline da misurare sull'hardware target: throughput ≥ 200 richieste/s con payload ≤ 10 KB, 3 nodi, fsync attivo, database sulla stessa LAN; latenza p95 ≤ 100 ms in regime normale. Il throughput è condiviso tra tutti gli endpoint (REQ-EPT-6).

**REQ-NFR-2** Nessuna perdita di voce committed. Nessuna doppia scrittura dello stesso `RequestId`.

## 21. Matrice dei test e criteri di accettazione

I test di cluster DEVONO usare l'astrazione di tempo, rete e repository (`IClock`, `IPeerTransport`, `IUdpChannel`, `IDataRepository`) per iniettare perdite, ritardi, partizioni ed errori del database, oltre a prove end-to-end con processi reali e test d'integrazione su SQL Server reale.

| ID | Scenario | Azione | Esito misurabile | Requisiti |
| --- | --- | --- | --- | --- |
| T1 | Boot 3 nodi | Avvio simultaneo | Un solo Leader entro 5 s, 2 Follower, un Leader per term in tutti i log | CLU-1, VOT-4 |
| T2 | Boot sfalsato | Un nodo ogni 3 s | Nessun Leader prima del quorum; Leader entro 5 s dal quorum | CLU-1, DIS-6 |
| T3 | POST al Leader | POST valida | 200; voce presente nel WAL di almeno 2 nodi su 3 e nel database prima della risposta | REP-5, PRX-9 |
| T4 | POST a un Follower | POST a un Follower | Stessi status, header e body del Leader; nessun header interno nella risposta | PRX-1, PRX-3, PRX-7 |
| T5 | Crash del Leader | `kill -9` | Nuovo Leader entro i limiti di §20; nessuna voce committed persa | FOV-2 |
| T6 | Failover durante POST | POST a un Follower mentre il Leader cade | Il client riceve 200 entro `RequestDeadlineMs`; dato presente una sola volta | PRX-5, IDM-3 |
| T7 | Stesso ID due volte | Due POST identiche | Entrambe 200, la seconda con `X-Idempotent-Replay: true`; una sola scrittura | IDM-3 |
| T8 | Crash dopo commit | `kill -9` dopo il commit e prima della risposta; retry | 200 con esito originale; una sola scrittura | IDM-7 |
| T9 | ID con body diverso | Stesso ID, body diverso | 422; nessuna scrittura | IDM-3 |
| T10 | Partizione 1+2 | Isola il Leader dai 2 Follower | Il Leader passa a Follower entro `LeaderLeaseMs` e risponde 503; la maggioranza elegge; nessun commit lato minoranza | LDR-3, LDR-4 |
| T11 | Riunione partizione | Ripristina la rete | L'ex Leader scarta le voci non committed e converge; nessuna voce committed persa | REP-3, REP-4 |
| T12 | Perdita quorum | Spegne 2 nodi su 3 | Il superstite non diventa Leader; scritture 503 con `Retry-After`; riprendono al rientro di un nodo | CLU-7 |
| T13 | Perdita UDP 30% | Scarta il 30% dei datagrammi | Nessuna elezione spuria in 10 minuti | DIS-1 |
| T14 | SIGSTOP del Leader | Pausa di 5 s | Nuovo Leader entro i limiti; al risveglio il vecchio Leader passa a Follower senza committare | LDR-4 |
| T15 | Clock sfasati entro tolleranza | Sfasamento di 30 s su un nodo | Consenso invariato; nessuna RPC rifiutata | SEC-5 |
| T16 | Riavvio completo | Spegne e riavvia tutti i nodi | Nessuna voce committed persa; idempotenza ancora valida | WAL-3, IDM-2 |
| T17 | Autenticazione errata | Annuncio o RPC con HMAC errato o `ClusterId` diverso | Scartati; nessun effetto; contatori incrementati | SEC-2, SEC-3 |
| T18 | Disco pieno | Esaurisce lo spazio sul Leader | Nessun 200 per dati non durevoli; il Leader cede; nodo `unhealthy` | WAL-5 |
| T19 | Forwarding errato | Follower con Leader noto errato | Nessun loop; al massimo un inoltro; 421 e rivalutazione | PRX-6 |
| T20 | Carico durante failover | 100 req/s con kill del Leader | Nessuna richiesta persa o duplicata con retry corretto | PRX-5, IDM-3 |
| T21 | Follower offline a lungo | Nodo spento 1 ora sotto carico, poi riavviato | Recupera dal Leader, con `InstallSnapshot` se necessario; `replicationLag` torna a 0 | SNP-2 |
| T22 | Crash durante fsync | `kill -9` durante la scrittura del WAL | Recovery senza voci committed corrotte; record finale incompleto troncato | WAL-3 |
| T23 | Crash durante apply | `kill -9` del Leader durante la transazione di apply | Il nuovo Leader trova `LastApplied` coerente; nessuna scrittura duplicata | SM-1 |
| T24 | Crash dopo 200 | `kill -9` dopo la risposta; retry | Stesso risultato; nessuna duplicazione | IDM-7 |
| T25 | Due Candidate simultanei | Timeout di elezione coincidenti | Al massimo un Leader per term | VOT-4, VOT-6 |
| T26 | Nodo isolato che rientra | Isola un Follower 30 s, poi ripristina | Term del cluster invariato; PreVote negato; nessuna elezione | VOT-2 |
| T27 | Replica lenta | Ritarda la risposta dei Follower | Il Leader non risponde 200 senza quorum | REP-5 |
| T28 | Conflitto di log | Follower con voci non committed divergenti | Prevale il log del Leader; voci committed intatte | REP-3, REP-4 |
| T29 | Storage non sano | Inietta errore di I/O | `/health/ready` Not Ready; il nodo non vota | OPS-2, WAL-5 |
| T30 | *(rimosso)* | Retention dell'idempotenza eliminata: l'indice è permanente | — | IDM-2 |
| T31 | `ClusterSize` discordante | Un nodo con `ClusterSize` diverso | RPC rifiutate (409); allarme; nessuna elezione con quel nodo | CLU-4 |
| T32 | Nodo non membro | Quarto `NodeId` con segreto valido | RPC rifiutate (403); non conteggiato; nessun doppio Leader | CLU-2, CLU-5 |
| T33 | Stesso ID in parallelo | 10 richieste identiche simultanee | Una sola voce applicata; tutte 200 | IDM-3 |
| T34 | 400 e 413 | Senza `X-Request-ID`; JSON non valido; body oltre il limite | 400 e 413 senza inoltro né scrittura | IDM-1, PRX-2 |
| T35 | Follower senza Leader | Nessun Leader disponibile | 503 con `Retry-After` entro `RequestDeadlineMs` | PRX-8 |
| T36 | `commitIndex` del Follower | `LeaderCommit` oltre l'ultima voce verificata (unit test) | `commitIndex = min(LeaderCommit, ultima voce nuova)` | REP-3 |
| T37 | Leader stickiness | PreVote mentre il Leader è sano | Negato dai Follower che hanno sentito il Leader | VOT-2 |
| T38 | Clock oltre tolleranza | Sfasamento di 10 min su un nodo | RPC del nodo rifiutate; nodo isolato; nessuna violazione di safety; allarme | SEC-4, SEC-5 |
| T39 | Stato vuoto senza flag | Cancella `DataDir`, riavvia con lo stesso `NodeId` | Il nodo non parte (codice 2) | ID-3 |
| T40 | Spazio sotto `MinFreeBytes` | Riduce lo spazio libero | Il nodo smette di accettare scritture prima del disco pieno; allarmi al 70% e 85% | SNP-3 |
| T41 | Sovraccarico | Più di `MaxConcurrentRequests` richieste simultanee con body da 1 MiB | 503 con `Retry-After` senza lettura del body; memoria entro il limite | PRX-10 |
| T42 | Body non identico | Retry con lo stesso ID e body diverso solo per la spaziatura | 422 e nessuna scrittura; il retry identico ottiene 200 con replay | IDM-8, CLI-1 |
| T43 | Recovery di commitIndex | Riavvio dopo una compattazione | `commitIndex >= SnapshotIndex`; nessuna voce riapplicata | WAL-3 |
| T44 | Validazione dei timer | Vincoli dei timer violati | Il nodo non parte (codice 1) e riporta il vincolo violato | CFG-1 |
| T45 | Poison entry | Voce non applicabile per un errore non classificato | Quarantena dopo `MaxApplyAttempts`; scritture successive 200 | SM-5 |
| T46 | Database irraggiungibile | Errori transitori per N tentativi | Nessuna quarantena; scrittura completata al ripristino | SM-5 |
| T47 | Voce obsoleta | Voce in sospeso committata dopo 14 giorni | `Expired`, nessun dato scritto; retry → 410 | SM-6 |
| T48 | Due applicatori concorrenti | Ex Leader e nuovo Leader applicano insieme | Ogni voce scritta esattamente una volta | SM-1 |
| T49 | Follower e compattazione | Follower spento durante la compattazione | Riallineamento tramite `InstallSnapshot` | SNP-1, SNP-2 |
| T50 | URL che porta a un altro membro | URL di un membro configurato verso un altro | Risposta scartata, URL invalidato, nessun doppio conteggio | SEC-3, DIS-4 |
| T51 | Gossip degli URL | Follower che conoscono solo il Leader; il Leader cade | Nuovo Leader eletto grazie agli URL propagati | DIS-4 |
| T52 | DataDir condivisa | Seconda istanza sulla stessa `DataDir` | Non parte (codice 3) | STO-1 |
| T53 | Porta occupata | Porta occupata con `RestPorts` | Il nodo usa la porta libera successiva | NET-6 |
| T54 | Discovery a sondaggio | Tre nodi su un elenco di porte, senza `Peers` né UDP | Si trovano tramite sondaggio; Leader eletto | DIS-7 |
| T55 | Riavvio su altra porta | La porta del nodo è occupata al riavvio | Gli altri lo ritrovano; il nodo si riallinea e inoltra al Leader | DIS-7 |
| T56 | Cluster di sviluppo | Un solo avvio in Development con `DevCluster:Instances=3` | Tre nodi attivi; uccidendo il padre i figli terminano | DEV-1 |
| T57 | RPC appese | RPC verso il Leader spento appese fino al timeout | Nuovo Leader in meno di 3 s, ripetuto in 3 failover consecutivi | VOT-7 |
| T58 | Più endpoint | Scritture su due endpoint, anche tramite inoltro da un Follower | Ogni dato nella tabella del proprio endpoint; indici del log unici e contigui | EPT-2, EPT-4, EPT-5 |
| T59 | ID su due endpoint | Stesso `RequestId` su due endpoint, body identico | 422 `Conflict` sul secondo; nessuna scrittura nella seconda tabella | EPT-3, IDM-8, IDM-9 |
| T60 | Path non configurato | POST a un path sconosciuto (anche `/api/v1/data`) | 404, nessuna voce nel log | EPT-1 |
| T61 | Endpoint sconosciuto all'applicatore | Voce di un tipo senza writer | `Rejected` (422) registrato; le voci successive vengono applicate | EPT-4 |
| T62 | Versione di protocollo diversa | RPC senza versione o con un'altra versione | RPC rifiutate (409): il nodo non vota e non conta nel quorum | VER-1 |
| T63 | `Endpoints` diversi tra i nodi | Un nodo con un endpoint in più | Cluster operativo; la discrepanza è segnalata dal Leader e dal nodo discordante (`endpointsHashMismatch`, `endpointsMismatch`) | VER-1 |
| T64 | WAL di un altro formato | Avvio su un WAL in formato 1 | Avvio bloccato (codice 2), WAL non troncato | VER-2, WAL-3 |
| T65 | `Endpoints` non validi | Vuoti, nome o path duplicato, path riservato, tabella mancante, writer senza nome | Avvio bloccato (codice 1) con l'elenco delle violazioni | EPT-1, CFG-1 |
| T66 | Schema di un'altra versione | Database con `SchemaVersion` diversa o assente | Nessun apply; `stateMachineError` valorizzato, Leader Not Ready | VER-2 |

## 22. Invarianti da verificare automaticamente

**REQ-INV-1** I test NON DEVONO limitarsi agli esiti HTTP: DEVONO verificare le invarianti interne seguenti, sul log dei nodi e sul contenuto della state machine. Il collaudo DOVREBBE usare un harness con tempo e rete simulati e un checker delle operazioni per verificare l'ordine delle scritture.

```text
Safety:       per ogni term, count(Leader valido) <= 1
Commit:       Committed(index) => persisted_on_quorum(index)
Apply:        LastApplied <= commitIndex <= lastLogIndex
Log:          le voci committed non vengono mai sovrascritte
Apply unico:  gli indici applicati sono contigui e ciascuno compare una sola volta
Idempotenza:  stesso RequestId => al massimo una voce con esito Applied, su tutti gli endpoint
Conflitto ID: stesso RequestId + PayloadHash diverso (body o endpoint) => 422
Endpoint:     ogni dato scritto sta nella destinazione del PayloadType della sua voce
Quorum:       writes_allowed => nodi_durevoli_disponibili >= quorum
Leadership:   writes_allowed => leader_has_valid_lease
Membership:   voti e conteggi di commit includono solo NodeId in Members
```

## 23. Criterio di completamento dell'implementazione

L'implementazione può essere dichiarata conforme solo quando:

1. tutti i requisiti marcati DEVE / NON DEVE sono implementati e tracciati ai test (§21);
2. tutti i test T1-T66 (escluso T30) sono automatizzati o documentati come prove operative;
3. le invarianti del §22 sono verificabili;
4. sono disponibili log e metriche sufficienti a diagnosticare un failover e un errore di apply;
5. i test includono fault injection su rete, processo, storage, database e orologi;
6. è dimostrata la recovery dopo crash nei punti critici (WAL, apply, risposta);
7. throughput e latenza sono misurati sull'hardware target;
8. il formato delle risposte (REQ-API-2) e lo schema dei payload di ogni endpoint sono documentati;
9. il documento di deploy riporta il piano di capacità del disco (REQ-SNP-4), la procedura di sostituzione di un nodo (REQ-CLU-6), di aggiornamento di versione e di modifica degli endpoint;
10. non esistono dipendenze implicite da componenti di coordinamento, clock sincronizzati per la safety, o discovery per il consenso.

## 24. Fuori ambito

Restano fuori dalla presente revisione:

- membership dinamica e sostituzione automatica di un `NodeId` perso;
- discovery oltre L2 senza configurazione (mitigata da `Peers`, sondaggio e gossip del Leader);
- autenticazione e autorizzazione dei client degli endpoint di ingestione;
- letture linearizzabili da Follower;
- multi-datacenter;
- effetti esterni exactly-once;
- alta affidabilità del database (Always On o equivalente);
- cifratura delle RPC (raccomandato TLS di rete);
- un log separato per endpoint nello stesso processo (multi-Raft): l'isolamento si ottiene con cluster distinti (REQ-EPT-6);
- migrazione dei dati da versioni precedenti (REQ-VER-2).
