# Specifica — proposta di revisione 6 (delta rispetto alla rev. 5)

*19 ottobre 2026. Stato: proposta, allineata all'implementazione v0.2.0. Elenca solo i requisiti nuovi, modificati o rimossi; gli altri restano quelli della rev. 5.*

## Motivazione

La rev. 5 prevedeva una state machine replicata su ogni nodo (REQ-SM-2) e vietava storage condivisi (REQ-GEN-2). Il target applicativo è invece **un database SQL Server condiviso**. Con un unico database:

- la state machine non va replicata: basta garantire che ogni voce committed venga scritta **una volta e in ordine**;
- le decisioni non deterministiche (scadenza, quarantena) possono essere prese una sola volta e registrate nella stessa transazione della scrittura, senza estendere il protocollo Raft;
- la compattazione del log diventa semplice: lo snapshot non contiene dati, solo `(SnapshotIndex, SnapshotTerm)`.

L'alta affidabilità complessiva dipende ora anche da quella del database (fuori ambito: Always On o equivalente). Il consenso tra i nodi resta indipendente dal database: un database irraggiungibile ferma le scritture, ma non causa perdite né elezioni.

## Requisiti modificati

| Requisito | Rev. 5 | Rev. 6 |
| --- | --- | --- |
| REQ-GEN-2 | Nessun database o file system condiviso | Nessun **componente di coordinamento** esterno. La state machine PUÒ essere un database condiviso (`StateMachine:Provider`). WAL e stato Raft restano locali a ogni nodo |
| REQ-SM-2 | Ogni nodo applica tutte le voci alla propria replica | **Solo il Leader** applica le voci committed al database condiviso. I Follower non applicano |
| REQ-SM-1/SM-3 | Apply idempotente rispetto a `LogIndex` | Apply **condizionale**: nella stessa transazione della scrittura si verifica `LastApplied = LogIndex − 1` e si aggiorna `LastApplied`. Voce già applicata → esito registrato (replay); predecessore mancante → riallineamento del cursore |
| REQ-WAL-3 punti 5-6 | `commitIndex = max(persistito, lastApplied)`; riallineamento locale della state machine | `commitIndex = max(persistito, SnapshotIndex)`. Nessun apply all'avvio: lo esegue il Leader leggendo `LastApplied` dal database. L'avvio non dipende dalla raggiungibilità del database |
| REQ-ID-4 | `commitIndex` nello stato persistente | `commitIndex` in un file separato (`commit.idx`, con CRC, non sincronizzato). `raft_state.json` è scritto solo in modo durevole |
| REQ-IDM-2/5/6 | Indice delle richieste derivato dal log; retention a tempo logico | Indice **permanente** nel database (`Requests`), indipendente dalla compattazione. Nel nodo resta solo l'indice della finestra di log non compattata. `IdempotencyRetentionHours` rimosso |
| REQ-IDM-4 | Dedup deterministica in apply | Invariato nel principio; eseguito dal database (prima occorrenza in `Requests`) |
| REQ-SNP-1/2 | WAL mai compattato, crescita illimitata | WAL a segmenti **compattato** oltre `Log:RetainEntries` voci applicate; `InstallSnapshot` per i Follower in ritardo |
| REQ-PRX-8 | Deadline unico | Il tempo residuo è **propagato** al Leader nell'inoltro (`X-Raft-Deadline-Ms`). Il 504 viene restituito solo a deadline scaduto; il proxy ritenta anche su 504 e su 503 per perdita di leadership |
| REQ-PRX-3/6 | `X-Forward-Hops` dal richiedente | Gli header di inoltro valgono solo se l'inoltro è **firmato** da un membro (HMAC su metodo, path, timestamp, body, mittente, nonce, destinatario, hop, deadline, RequestId). Altrimenti vengono ignorati |
| REQ-SEC-3 | HMAC della richiesta | HMAC della richiesta **incluso il destinatario** (`DestNodeId`). Il ricevente rifiuta con 421 se il destinatario non è sé stesso. Ogni **risposta** è firmata dal risponditore e legata al nonce della richiesta; una risposta non firmata o di un altro nodo viene scartata |
| Threat model §5 | Un `RestUrl` stantio degrada solo la discovery | Corretto: senza verifica d'identità un URL che porta a un altro membro causerebbe un **doppio conteggio** di voti e conferme (rischio di safety con N ≥ 5). Con la firma delle risposte il rischio è eliminato |
| REQ-DIS-4 | Registro alimentato da UDP | Fonti: RPC autenticata > UDP > gossip del Leader (`MemberUrls` negli `AppendEntries`) > configurazione. Un URL che porta al nodo sbagliato viene invalidato |
| REQ-CFG-1 | — | Aggiunti vincoli per `Log:*`, `StateMachine:*`, `MaxBodyBytes ≤ 64 MiB`, `RestPorts`, `Discovery:*`, `DevCluster:*`, nomi SQL validi |

## Requisiti nuovi

**REQ-SM-5 (poison entry).** Il Leader DEVE classificare gli errori di apply:
- **permanente di business** → esito `Rejected` (422) registrato, `LastApplied` avanza;
- **transitorio** → ritentato senza limite con backoff limitato da `ApplyRetryMaxDelayMs`;
- **altro** → ritentato `MaxApplyAttempts` volte, poi esito `Quarantined` (422) registrato con il payload in una tabella di quarantena, e `LastApplied` avanza.

Il cluster NON DEVE fermarsi per una singola voce.

**REQ-SM-6 (voci obsolete).** Se all'apply `now − ReceivedUtc > MaxPendingAgeMinutes`, la voce NON DEVE essere scritta: esito `Expired` (410) registrato. La decisione è presa una sola volta dall'applicatore e registrata nella transazione condizionale, quindi è deterministica. Poiché la risposta 200 arriva solo dopo l'apply, una voce scaduta non ha mai ricevuto 200. La documentazione client DEVE fissare l'intervallo massimo di retry sotto questa soglia.

**REQ-SM-7 (ripristino del database).** Se `LastApplied` del database precede l'inizio del log compattato, il Leader DEVE segnalarlo con un evento di severità alta e non applicare voci fuori sequenza.

**REQ-STO-1 (lock della DataDir).** Un nodo DEVE acquisire un lock esclusivo sulla propria DataDir; se è in uso, NON DEVE partire (codice 3).

**REQ-NET-6 (elenco di porte).** Con `RestPorts` (intervallo `X-Y`, lista `X,Y,Z` o misto `X-Y;A;B`) il nodo DEVE usare la prima porta TCP libera dell'elenco e pubblicare l'URL effettivo (segnaposto `{port}`).

**REQ-DIS-7 (discovery a sondaggio).** Finché un membro non ha un URL noto, o dopo `UnreachableAfterFailures` RPC fallite consecutive, il nodo DEVE sondare gli host noti su tutte le porte ammesse (`POST /internal/v1/hello`, destinatario jolly `*` ammesso solo su questo endpoint, che non modifica stato). Un indirizzo viene registrato solo se la risposta è firmata dal membro che la invia.

**REQ-DEV-1 (cluster di sviluppo).** Solo nell'ambiente Development il processo avviato PUÒ lanciare gli altri membri come processi figli (`DevCluster:Instances`). I figli DEVONO terminare quando il padre termina. Un segreto `dev-only-…` NON DEVE essere accettato fuori da Development.

**REQ-OPS-6 (piattaforme).** Il servizio DEVE essere eseguibile come processo console, Windows Service e unità systemd (`Type=notify`). I percorsi relativi sono risolti rispetto a `BaseDirectory`, alla directory dell'eseguibile (servizio) o alla directory corrente.

**REQ-OPS-7 (thread pool).** All'avvio il nodo DEVE impostare un minimo di thread del pool adeguato, perché fsync e lock sincroni non introducano pause tali da far scadere il lease.

## Matrice dei test: aggiunte

| ID | Scenario | Esito atteso |
| --- | --- | --- |
| T45 | Voce non applicabile per un errore non classificato | Quarantena dopo `MaxApplyAttempts`; scritture successive 200 |
| T46 | Database irraggiungibile per N tentativi | Nessuna quarantena; scrittura completata al ripristino |
| T47 | Voce in sospeso committata dopo 14 giorni | `Expired`, nessun dato scritto; retry → 410 |
| T48 | Due applicatori concorrenti (ex Leader e nuovo Leader) | Ogni voce scritta esattamente una volta |
| T49 | Follower spento durante la compattazione | Riallineamento tramite `InstallSnapshot` |
| T50 | URL di un membro che porta a un altro membro | Risposta scartata, URL invalidato, nessun doppio conteggio |
| T51 | Follower che conoscono solo il Leader; il Leader cade | Nuovo Leader eletto grazie agli URL propagati |
| T52 | Seconda istanza sulla stessa DataDir | Non parte (codice 3) |
| T53 | Porta occupata con `RestPorts` | Il nodo usa la porta libera successiva |
| T54 | Tre nodi su un elenco di porte, senza Peers né UDP | Si trovano tramite sondaggio; Leader eletto |
| T55 | Nodo riavviato su un'altra porta (la sua è occupata) | Gli altri lo ritrovano; il nodo si riallinea e inoltra al Leader |
| T56 | Un solo avvio in Development con `DevCluster:Instances=3` | Tre nodi attivi; uccidendo il padre i figli terminano |

## Fuori ambito (invariato salvo dove indicato)

- Membership dinamica.
- Discovery oltre L2: mitigata da `Peers` e dal gossip del Leader.
- Autenticazione dei client.
- Letture linearizzabili dai Follower.
- Multi-datacenter.
- Effetti esterni exactly-once.
- Alta affidabilità del database.
- Cifratura delle RPC (raccomandato TLS di rete).
