# Risoluzione dei problemi

### Dopo la caduta del Leader nessun altro nodo diventa Leader ("resta in PreVote")
Quasi sempre i nodi superstiti non conoscono i rispettivi indirizzi. Nei log compare `Nessun URL noto per il membro ...` e cresce la metrica `peerUrlUnknown`.
- Con più istanze sulla stessa macchina **Windows** il broadcast UDP può arrivare a una sola istanza: usare `UdpMode=Multicast` oppure `Peers`.
- Dalla 0.2 il Leader propaga gli URL di tutti i membri negli heartbeat: basta che ogni nodo ne conosca uno all'avvio.
- Controllare `GET /api/v1/cluster/status` → `peers` (campi `rpcUrl`, `udpUrl`, `gossipUrl`, `staticUrl`).
- Con porte assegnate dinamicamente (`RestPorts`) i nodi si trovano con la discovery a sondaggio: `Discovery:SeedHosts` deve contenere gli host dei nodi (default solo `127.0.0.1`). La metrica `probeFound` conta i membri trovati così.
- Ricordare che con 3 nodi ne servono **2** attivi: con un solo superstite nessuno può diventare Leader, per progetto.

### Il nodo non parte: "stato persistente assente ... AllowEmptyState=false"
È il primo avvio, oppure la DataDir è cambiata (attenzione al segnaposto `{port}` con `RestPorts`: usare `{nodeId}`). Al primo avvio impostare `Storage:AllowEmptyState=true`, poi rimuoverlo. Se il disco è stato perso, vedere [Operazioni](Operazioni) → sostituzione di un nodo.

### Il nodo non parte: "La directory dati ... è già in uso" (codice 3)
Un'altra istanza usa la stessa DataDir. Sulla stessa macchina ogni istanza deve avere una DataDir propria: il default `./data/{nodeId}` lo garantisce.

### Il nodo non parte: "Nessuna porta REST disponibile" (codice 4)
Tutte le porte di `RestPorts` sono occupate: aggiungere porte all'elenco (es. `5001-5010;5020-5030`).

### Le scritture restano in attesa e poi danno 504
Il Leader non riesce a scrivere sul database. Controllare `GET /api/v1/cluster/status` sul Leader (`stateMachineHealthy`, `stateMachineError`) e le metriche `applyLag` e `applyTransientErrors`. Le richieste non vanno perse: ritentate con lo stesso ID, ricevono l'esito quando il database torna disponibile.

### Una richiesta riceve 422 "quarantined" o "rejected"
Vedere [SQL Server](SQL-Server) → errori e poison entry.

### Una richiesta riceve 410 "expired"
La richiesta era rimasta in sospeso oltre `MaxPendingAgeMinutes` (es. cluster rimasto a lungo senza quorum) e non è stata scritta di proposito. Se il dato è ancora valido, inviarlo con un nuovo `X-Request-ID`.

### Dopo la caduta del Leader il nuovo Leader arriva in diversi secondi
Nei log compaiono più `Ruolo Follower -> Candidate` con term crescenti prima di `eletto Leader`: sono voti contesi. Dalla versione successiva alla 0.2.0 il candidato non aspetta più il nodo spento in ogni round; su Windows questo era la causa principale. Se succede ancora:
- verificare il carico della macchina e la metrica `electionCount`;
- allargare la finestra casuale `ElectionTimeoutMaxMs - ElectionTimeoutMinMs`, che rende meno probabili due candidature simultanee.

### Elezioni frequenti senza guasti
Verificare la latenza dei dischi (`walFsyncLatencyMaxUs`), il carico della CPU e la rete. Su macchine lente aumentare insieme i tempi, rispettando i vincoli: ad esempio heartbeat 500, elezione 2000-3600, lease 1500.

### Log troppo verbosi
Livelli in `appsettings.json` → `Logging:LogLevel`. Le RPC interne non producono log a livello `Information`. Per diagnosticare la replica: `"RaftCore": "Debug"`.
