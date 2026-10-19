# Operazioni

## Endpoint di stato

| Endpoint | Uso |
| --- | --- |
| `GET /health/live` | Il processo è vivo (anche senza quorum) |
| `GET /health/ready` | 200 se il nodo svolge il suo ruolo: Follower con Leader noto; Leader con lease valido, storage sano e database raggiungibile |
| `GET /api/v1/cluster/status` | Ruolo, term, Leader, commitIndex, lastApplied, snapshot, peer con le fonti dei loro URL |
| `GET /api/v1/metrics` | Contatori e indicatori |

## Metriche da sorvegliare

| Metrica | Allarme se | Significato |
| --- | --- | --- |
| `applyLag` (sul Leader) | cresce per minuti | Il database non riesce a stare al passo o non è raggiungibile |
| `applyQuarantined` | > 0 | Voci in quarantena da analizzare (`raft.Quarantine`) |
| `applyTransientErrors` | cresce | Problemi di connessione al database |
| `stepDownCount`, `electionCount` | crescono senza guasti | Rete o dischi lenti, orologi, risorse insufficienti |
| `walFsyncLatencyMaxUs` | > 100000 | Disco lento: rischio di lease scaduti |
| `peerUrlUnknown` | > 0 | Un membro non è raggiungibile: verificare discovery e `Peers` |
| `rpcWrongDestination` | > 0 | Un URL configurato porta al nodo sbagliato |
| `membersHashMismatch`, `nonMemberRejected` | > 0 | Configurazione dei membri diversa tra i nodi |
| `rpcClockSkewRejected` | > 0 | Orologio fuori tolleranza: verificare NTP |
| `storageAvailableBytes` | sotto il 30% | Spazio disco (allarmi nei log al 70% e all'85% di occupazione) |

## Capacità del disco

Il WAL è compattato. Lo spazio occupato è circa `(RetainEntries + voci non ancora applicate) × dimensione media della richiesta`, più un segmento in scrittura (`SegmentBytes`). Esempio: 10.000 voci da 2 KB ≈ 20 MB, più un segmento da 64 MB. I dati applicativi crescono nel database, non sui nodi.

## Riavvio e aggiornamento

- **Riavvio di un nodo**: nessuna azione. I dati locali vengono verificati e il nodo si riallinea.
- **Aggiornamento di versione**: un nodo alla volta, aspettando che `/health/ready` torni 200 prima di passare al successivo.

## Sostituzione di un nodo con disco perso

Un nodo che ha perso i dati **non** può rientrare con lo stesso NodeId: potrebbe aver dimenticato un voto, e l'avvio viene bloccato. La procedura:

1. Assicurarsi che il vecchio nodo sia spento definitivamente.
2. Generare un nuovo NodeId e aggiornare `Members` su **tutti** i nodi.
3. Riavviare i nodi uno alla volta; il nuovo nodo al primo avvio con `AllowEmptyState=true`.

Durante il punto 3 i nodi con insiemi di membri diversi si rifiutano a vicenda: è prevista una breve indisponibilità.

## Sicurezza

- `CLUSTER_SECRET` lungo e casuale, solo in variabili d'ambiente o secret store. Ruotarlo richiede il riavvio coordinato di tutti i nodi.
- Le RPC interne e gli inoltri sono firmati, ma non cifrati: in reti non fidate mettere i nodi dietro TLS (proxy o mTLS di rete).
- Le API pubbliche non hanno autenticazione dei client (fuori ambito): esporle solo dietro un gateway.
