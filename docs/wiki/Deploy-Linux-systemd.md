# Installazione con systemd (Linux)

1. Scaricare `raftcore-<versione>-linux-x64.tar.gz` (o `linux-arm64`) dalla [pagina delle release](https://github.com/emanuelemerlo/raft-core/releases) ed estrarlo.
2. Installare:

```bash
tar xzf raftcore-0.2.0-linux-x64.tar.gz
sudo raftcore/deploy/systemd/install.sh raftcore
```

Lo script:
- crea l'utente di sistema `raftcore`;
- copia i file in `/opt/raftcore`;
- crea `/var/lib/raftcore/data` (DataDir) e `/etc/raftcore/raftcore.env` (permessi 0600);
- abilita l'unità `raftcore.service`.

3. Configurare `/etc/raftcore/raftcore.env` (segreto, stringa SQL, `RaftCluster__NodeId`) e `/opt/raftcore/appsettings.json`. Al **primo** avvio aggiungere `RaftCluster__Storage__AllowEmptyState=true`:

```bash
sudo systemctl start raftcore
journalctl -u raftcore -f
curl -s localhost:5000/api/v1/cluster/status
# dopo il primo avvio riuscito: rimuovere AllowEmptyState da raftcore.env e
sudo systemctl restart raftcore
```

L'unità usa `Type=notify`: systemd considera il servizio avviato solo a recovery completato. Hardening applicato: `NoNewPrivileges`, `ProtectSystem`, `ProtectHome`, `PrivateTmp`.

**Firewall**: aprire TCP 5000 tra i nodi (e verso i client) e UDP 8888 tra i nodi, se si usa la discovery. Ad esempio con firewalld:

```bash
sudo firewall-cmd --permanent --add-port=5000/tcp --add-port=8888/udp && sudo firewall-cmd --reload
```

**Orologi**: abilitare la sincronizzazione NTP (`timedatectl set-ntp true`). Le RPC con uno scarto oltre `RpcClockToleranceMs` vengono rifiutate.
