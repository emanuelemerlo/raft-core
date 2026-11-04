# Operazioni

## Endpoint di stato

| Endpoint | Uso |
| --- | --- |
| `GET /health/live` | Il processo è vivo (anche senza quorum) |
| `GET /health/ready` | 200 se il nodo svolge il suo ruolo: Follower con Leader noto; Leader con lease valido, storage sano e database raggiungibile |
| `GET /api/v1/cluster/status` | Ruolo, term, Leader, commitIndex, lastApplied, snapshot, endpoint configurati, peer con le fonti dei loro URL |
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
| `rpcProtocolMismatch` | > 0 | Un nodo di un'altra versione di protocollo tenta di comunicare: completare l'aggiornamento |
| `endpointsHashMismatch` | > 0 per più di un aggiornamento | Configurazione `Endpoints` diversa tra i nodi |
| `applyUnknownType` | > 0 | Voci di un endpoint che il Leader non conosce, registrate come rifiutate |
| `storageAvailableBytes` | sotto il 30% | Spazio disco (allarmi nei log al 70% e all'85% di occupazione) |

## Capacità del disco

Il WAL è compattato. Lo spazio occupato è circa `(RetainEntries + voci non ancora applicate) × dimensione media della richiesta`, più un segmento in scrittura (`SegmentBytes`). Esempio: 10.000 voci da 2 KB ≈ 20 MB, più un segmento da 64 MB. I dati applicativi crescono nel database, non sui nodi.

## Riavvio e aggiornamento

- **Riavvio di un nodo**: nessuna azione. I dati locali vengono verificati e il nodo si riallinea.
- **Aggiornamento di versione con la stessa versione di protocollo**: un nodo alla volta, aspettando che `/health/ready` torni 200 prima di passare al successivo.
- **Aggiornamento che cambia la versione di protocollo** (indicato nel [CHANGELOG](https://github.com/emanuelemerlo/raft-core/blob/main/CHANGELOG.md)): i nodi di versioni diverse si rifiutano a vicenda, quindi si fermano tutti i nodi, si aggiornano e si riavviano. Il cluster resta indisponibile per la durata dell'operazione.
- **Dalla 0.2 alla 0.3**: cambiano anche il formato del WAL e lo schema del database, senza migrazione. Si attende che tutte le richieste siano applicate (`commitIndex = lastApplied` sul Leader), si fermano i nodi, si svuotano le `DataDir` e si riparte con `AllowEmptyState=true` su un database o uno schema nuovo.

## Aggiungere o rimuovere un endpoint

La configurazione `Endpoints` deve essere uguale su tutti i nodi. Durante la modifica, fatta un nodo alla volta, il cluster resta disponibile e segnala la differenza (`endpointsHashMismatch`).

**Aggiungere:**
1. creare la tabella (o lasciare `AutoCreateSchema=true`), ed eventualmente registrare il writer personalizzato;
2. aggiornare la configurazione e riavviare i nodi uno alla volta;
3. solo quando tutti i nodi sono aggiornati, iniziare a inviare dati al nuovo endpoint. Prima, una richiesta può ricevere 404 (Leader non ancora aggiornato) o 422 (voce applicata da un nodo che non conosce l'endpoint).

**Rimuovere:**
1. fermare i client di quell'endpoint e attendere che le loro richieste siano applicate (`commitIndex = lastApplied` sul Leader);
2. aggiornare la configurazione e riavviare i nodi uno alla volta.

**Non rinominare** il `Name` di un endpoint con voci in sospeso: per il servizio un nome diverso è un altro tipo di dato. Il `Path` invece si può cambiare liberamente, perché non viene registrato nel log.

## Sostituzione di un nodo con disco perso

Un nodo che ha perso i dati **non** può rientrare con lo stesso NodeId: potrebbe aver dimenticato un voto, e l'avvio viene bloccato. La procedura:

1. Assicurarsi che il vecchio nodo sia spento definitivamente.
2. Generare un nuovo NodeId e aggiornare `Members` su **tutti** i nodi.
3. Riavviare i nodi uno alla volta; il nuovo nodo al primo avvio con `AllowEmptyState=true`.

Durante il punto 3 i nodi con insiemi di membri diversi si rifiutano a vicenda: è prevista una breve indisponibilità.

## Sicurezza

- `CLUSTER_SECRET` lungo e casuale, solo in variabili d'ambiente o secret store. Ruotarlo richiede il riavvio coordinato di tutti i nodi.
- Le RPC interne e gli inoltri sono firmati, ma non cifrati: in reti non fidate mettere i nodi dietro TLS (proxy o mTLS di rete).
- Gli endpoint di ingestione non hanno autenticazione dei client (fuori ambito): esporli solo dietro un gateway.
