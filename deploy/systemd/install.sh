#!/usr/bin/env bash
# Installa RaftCore come servizio systemd. Uso: sudo ./install.sh <cartella con i file pubblicati>
set -euo pipefail
SRC="${1:-$(dirname "$0")/../..}"
id raftcore &>/dev/null || useradd --system --home /var/lib/raftcore --shell /usr/sbin/nologin raftcore
install -d -o raftcore -g raftcore -m 0750 /var/lib/raftcore /var/lib/raftcore/data
install -d -m 0755 /opt/raftcore /etc/raftcore
cp -r "$SRC"/* /opt/raftcore/
chmod +x /opt/raftcore/RaftCore
if [ ! -f /etc/raftcore/raftcore.env ]; then
  install -m 0600 "$(dirname "$0")/raftcore.env.example" /etc/raftcore/raftcore.env
  echo "Configurare /etc/raftcore/raftcore.env e /opt/raftcore/appsettings.json, poi: systemctl start raftcore"
fi
install -m 0644 "$(dirname "$0")/raftcore.service" /etc/systemd/system/raftcore.service
systemctl daemon-reload
systemctl enable raftcore
echo "Servizio installato. Avvio: sudo systemctl start raftcore   Log: journalctl -u raftcore -f"
