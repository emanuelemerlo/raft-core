#!/usr/bin/env bash
# Per ogni test fallito (dai file .trx) pubblica come annotazione le righe salienti dei log dei nodi di quel test.
# Le annotazioni sono leggibili nella PR e via API anche quando gli artifact non sono scaricabili.
set -uo pipefail
logs="${RAFTCORE_TEST_LOGS:-test-logs}"
shopt -s globstar nullglob
for trx in **/TestResults/*.trx; do
  grep -oE 'testName="[^"]*"[^>]*outcome="Failed"' "$trx" | sed -E 's/testName="([^"]*)".*/\1/' | while read -r full; do
    method="${full##*.}"; method="${method%%(*}"
    for f in "$logs/$method"-*.log; do
      body=$(grep -E "Ruolo|Leader|step down|PreVote|Nessun|ALLARME|URL|Error|Critical|Warning|fail|crit|warn|Exception|EVENTO" "$f" | tail -n 60)
      [ -z "$body" ] && body=$(tail -n 40 "$f")
      body="${body//'%'/'%25'}"; body="${body//$'\r'/}"; body="${body//$'\n'/'%0A'}"
      echo "::error title=Log ${method} $(basename "$f")::${body}"
    done
  done
done
