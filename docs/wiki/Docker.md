# Docker

## Cluster di prova con SQL Server

```bash
docker compose -f deploy/docker/docker-compose.yml up --build -d
curl -X POST localhost:5001/api/v1/data -H "X-Request-ID: $(uuidgen)" -H "Content-Type: application/json" -d '{"a":1}'
docker compose -f deploy/docker/docker-compose.yml kill node1      # crash del nodo 1
curl -X POST localhost:5002/api/v1/data -H "X-Request-ID: $(uuidgen)" -H "Content-Type: application/json" -d '{"a":2}'
docker compose -f deploy/docker/docker-compose.yml start node1     # rientra e si riallinea
```

Il compose avvia SQL Server e tre nodi (porte 5001-5003) con `Peers` statici e discovery UDP disattivata: nelle reti bridge di Docker il broadcast tra container non è garantito. Per i riavvii successivi al primo si usa `RAFT_FIRST_START=false docker compose ... up -d`.

## Immagine

Le release pubblicano `ghcr.io/emanuelemerlo/raft-core:<versione>` per `linux/amd64` e `linux/arm64`.

| Variabile | Uso |
| --- | --- |
| `CLUSTER_SECRET` | Segreto HMAC (obbligatorio) |
| `RAFT_SQL_CONNECTION` | Stringa di connessione SQL Server |
| `RaftCluster__NodeId` | NodeId del container |
| `RaftCluster__AdvertiseUrl` | URL con cui gli altri nodi raggiungono il container (es. `http://node1:5000`) |
| `RaftCluster__Storage__DataDir` | Default `/data`: montare un **volume persistente per nodo** |

In Kubernetes: StatefulSet con un PersistentVolumeClaim per pod, `AdvertiseUrl` sul nome DNS stabile del pod, sonde `GET /health/live` (liveness) e `GET /health/ready` (readiness).
