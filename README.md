# RaftCore

[![CI](https://github.com/emanuelemerlo/raft-core/actions/workflows/ci.yml/badge.svg)](https://github.com/emanuelemerlo/raft-core/actions/workflows/ci.yml)

Servizio .NET 8 ad alta affidabilità che riceve dati tramite API REST e li scrive **una sola volta** su un database **SQL Server condiviso**, anche quando un'istanza si guasta. Le istanze (3 o 5 in produzione) si coordinano con un sottoinsieme rigoroso di **Raft**: elezione del Leader, replica durevole del log, compattazione.

- **Nessun componente esterno** di coordinamento: niente Consul, Redis o ZooKeeper.
- **Il client può chiamare qualunque istanza**: i Follower inoltrano al Leader in modo trasparente.
- **Effectively-once**: lo stesso `X-Request-ID` non viene mai scritto due volte, anche dopo un failover o un retry.
- **Il failover non perde nulla**: una risposta `200` implica la scrittura durevole su un quorum e sul database.
- Funziona su **Windows** (anche come servizio), **Linux** (systemd), **macOS** e in **Docker**.

## Come funziona, in breve

```text
client ──POST /api/v1/data──▶ qualunque nodo ──inoltro firmato──▶ Leader
                                                                   │ 1. append al WAL (fsync, group commit)
                                                                   │ 2. replica su un quorum
                                                                   │ 3. apply su SQL Server in UNA transazione:
                                                                   │    dati + esito + LastApplied = LogIndex
                                                                   ▼
client ◀────────────── 200 (o esito già registrato) ─────────── SQL Server condiviso
```

Solo il Leader scrive sul database, e ogni scrittura è condizionata a `LastApplied = LogIndex - 1`. Né un ex Leader né un nodo rientrato dopo settimane possono quindi riscrivere dati già scritti. Una voce rimasta in sospeso oltre `MaxPendingAgeMinutes` viene registrata come scaduta e non viene scritta. Una voce che fallisce sempre (*poison entry*) viene messa in quarantena, e il cluster continua.

## Avvio rapido: 3 istanze sulla stessa macchina

**Visual Studio**: aprire `RaftCore.sln`, profilo *Cluster 3 nodi (debug nodo 1)*, **F5**. Il nodo 1 va in debug e avvia da solo i nodi 2 e 3; la configurazione è già pronta in `appsettings.Development.json`. Shift+F5 chiude tutto. Dettagli in [Avvio rapido](docs/wiki/Avvio-rapido.md).

Con il profilo multiplo **Cluster + Simulatore** parte anche il [simulatore client](docs/wiki/Simulatore.md) (http://localhost:5100): invio ciclico di dati, scenari di idempotenza ed errore, kill e pausa dei nodi, verifica delle scritture.

**Riga di comando**:

Prerequisito: [.NET 8 SDK](https://dotnet.microsoft.com/download).

```bash
dotnet build -c Release
export CLUSTER_SECRET=un-segreto-di-almeno-16-caratteri      # PowerShell: $env:CLUSTER_SECRET="..."
cd src/RaftCore/bin/Release/net8.0

# primo avvio di ogni nodo: AllowEmptyState=true (da rimuovere ai riavvii successivi)
for i in 1 2 3; do
  dotnet RaftCore.dll --RaftCluster:NodeId=$i$i$i$i$i$i$i$i-$i$i$i$i-$i$i$i$i-$i$i$i$i-$i$i$i$i$i$i$i$i$i$i$i$i \
    --RaftCluster:RestPort=500$i --RaftCluster:UdpMode=Multicast --RaftCluster:Storage:AllowEmptyState=true &
done

curl -X POST http://localhost:5002/api/v1/data -H "X-Request-ID: $(uuidgen)" -H "Content-Type: application/json" -d '{"temperatura": 21.5}'
curl http://localhost:5001/api/v1/cluster/status
```

Senza SQL Server il servizio usa la state machine **SharedFile** (`./shared-state/applied.log`), pensata solo per sviluppo e test sulla stessa macchina. Con Docker si avvia un cluster completo con SQL Server:

```bash
docker compose -f deploy/docker/docker-compose.yml up --build
```

## Documentazione

| Argomento | Pagina |
| --- | --- |
| Primi passi e prove di failover | [Avvio rapido](docs/wiki/Avvio-rapido.md) |
| Invio dati, scenari e guasti dall'interfaccia web | [Simulatore client](docs/wiki/Simulatore.md) |
| Tutti i parametri | [Configurazione](docs/wiki/Configurazione.md) |
| Come funziona dentro | [Architettura](docs/wiki/Architettura.md) |
| Database, writer personalizzato, quarantena | [SQL Server](docs/wiki/SQL-Server.md) |
| Retry, codici di risposta, idempotenza | [Contratto client](docs/wiki/Contratto-client.md) |
| Installazione | [Windows Service](docs/wiki/Deploy-Windows-Service.md) · [Linux systemd](docs/wiki/Deploy-Linux-systemd.md) · [Docker](docs/wiki/Docker.md) |
| Monitoraggio, sostituzione di un nodo, capacità | [Operazioni](docs/wiki/Operazioni.md) |
| Problemi comuni | [Risoluzione dei problemi](docs/wiki/Risoluzione-problemi.md) |
| Rilasci | [Creare una release](docs/wiki/Rilasci.md) · [CHANGELOG](CHANGELOG.md) |
| Specifica | [Modifiche proposte per la rev. 6](docs/specifica-rev6-delta.md) |

## Struttura del repository

```text
src/RaftCore/            servizio (Raft, WAL, proxy, state machine, hosting)
tests/RaftCore.Tests/    unit, cluster in-process con fault injection, E2E con processi reali, SQL Server
sql/001_init.sql         schema del database condiviso (applicato anche automaticamente)
deploy/                  systemd, Windows Service, launchd, Docker, configurazione d'esempio
docs/wiki/               guide how-to (pubblicate nella Wiki del repository)
.github/workflows/       CI, release, wiki
```

## Test

```bash
dotnet test                                        # unit + cluster + E2E (circa 1 minuto)
RAFT_TEST_SQL="Server=...;User Id=sa;Password=...;TrustServerCertificate=True" dotnet test --filter Category=SqlServer
```
