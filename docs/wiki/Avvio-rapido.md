# Avvio rapido

Obiettivo: 3 istanze sulla stessa macchina, una scrittura, la caduta del Leader senza perdere dati.

## Da Visual Studio: F5 avvia tutto il cluster

1. Aprire `RaftCore.sln` e scegliere il profilo **Cluster 3 nodi (debug nodo 1)** (è il primo e il predefinito).
2. Premere **F5**. Il processo in debug è il **nodo 1**; avvia da solo i nodi 2 e 3 come processi **non** in debug, ciascuno nella propria finestra console.
3. **Shift+F5** (stop del debugger) chiude anche i nodi 2 e 3, entro circa un secondo.

La configurazione è già pronta in `appsettings.Development.json`:
- 3 membri;
- porte `5001-5010`: ogni nodo prende la prima libera;
- i nodi si trovano con la discovery a sondaggio su `127.0.0.1`;
- dati in `src/RaftCore/devcluster/`;
- `AllowEmptyState` attivo, solo in sviluppo;
- segreto di sviluppo in `launchSettings.json`, rifiutato fuori da Development.

Altri profili:

| Profilo | Uso |
| --- | --- |
| Nodo singolo (ClusterSize 1) | Un solo nodo che è subito Leader: utile per provare l'API |
| Solo nodo 1 / 2 / 3 | Un solo nodo, senza figli: per mettere in debug un nodo diverso dal primo. Si avviano gli altri a mano |

Per seguire in debug anche un nodo figlio: **Debug → Connetti a processo** e scegliere `RaftCore.exe` con il titolo della finestra corrispondente.

Prova rapida (PowerShell):

```powershell
Invoke-RestMethod -Method Post http://localhost:5002/api/v1/data -Headers @{"X-Request-ID"=[guid]::NewGuid()} -ContentType application/json -Body '{"a":1}'
Invoke-RestMethod http://localhost:5001/api/v1/cluster/status
```

## Da riga di comando

## 1. Compilare

```bash
git clone https://github.com/emanuelemerlo/raft-core && cd raft-core
dotnet build -c Release
cd src/RaftCore/bin/Release/net8.0
```

## 2. Avviare 3 istanze

Ogni istanza ha un `NodeId` diverso (deve comparire in `Members`) e una porta diversa. `appsettings.json` contiene già i tre NodeId d'esempio `1111…`, `2222…`, `3333…`. Al primo avvio serve `AllowEmptyState=true`, perché un nodo senza dati parte solo se lo si autorizza esplicitamente.

**Linux / macOS**

```bash
export CLUSTER_SECRET=dev-secret-0123456789
for i in 1 2 3; do
  id=$(printf '%s' "$i$i$i$i$i$i$i$i-$i$i$i$i-$i$i$i$i-$i$i$i$i-$i$i$i$i$i$i$i$i$i$i$i$i")
  dotnet RaftCore.dll --RaftCluster:NodeId=$id --RaftCluster:RestPort=500$i \
     --RaftCluster:UdpMode=Multicast --RaftCluster:Storage:AllowEmptyState=true > node$i.log 2>&1 &
done
```

**Windows (PowerShell)**: tre finestre, una per nodo:

```powershell
$env:CLUSTER_SECRET = "dev-secret-0123456789"
dotnet RaftCore.dll --RaftCluster:NodeId=11111111-1111-1111-1111-111111111111 --RaftCluster:RestPort=5001 `
   --RaftCluster:UdpMode=Multicast --RaftCluster:Storage:AllowEmptyState=true
# finestra 2: NodeId 2222..., porta 5002   finestra 3: NodeId 3333..., porta 5003
```

> Sulla stessa macchina Windows il broadcast UDP può arrivare a una sola istanza: usare `UdpMode=Multicast`, oppure indicare gli indirizzi in `Peers`. Basta che ogni nodo ne conosca uno: il Leader propaga poi gli indirizzi di tutti.

Dopo il primo avvio riuscito, i riavvii successivi vanno fatti **senza** `AllowEmptyState`.

## 3. Scrivere un dato

```bash
curl -i -X POST http://localhost:5002/api/v1/data \
  -H "X-Request-ID: 6f1c2b2e-4b7a-4d55-9d0a-1e2f3a4b5c6d" -H "Content-Type: application/json" -d '{"temperatura":21.5}'
```

La risposta è `200` anche se il nodo 2 non è il Leader. Ripetendo la stessa richiesta si ottiene `200` con `X-Idempotent-Replay: true`, e il dato non viene scritto una seconda volta.

## 4. Osservare il cluster

```bash
curl http://localhost:5001/api/v1/cluster/status   # ruolo, term, Leader, commitIndex, peer
curl http://localhost:5001/api/v1/metrics
curl http://localhost:5001/health/ready
```

## 5. Provare il failover

Si termina brutalmente il processo del Leader (Linux: `kill -9 <pid>`; Windows: `Stop-Process -Force`), poi si ripete la scrittura su un altro nodo: entro circa 2-3 secondi un nuovo Leader risponde. Riavviando il nodo terminato (senza `AllowEmptyState`), questo si riallinea da solo.

I dati scritti si trovano in `shared-state/applied.log` (sviluppo) oppure nelle tabelle SQL Server (vedi [SQL Server](SQL-Server)).
