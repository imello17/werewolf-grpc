#!/usr/bin/env bash
# run_demo.sh — automated 7-player game over mutual TLS, each player a
# separate process running as its own Linux user (player0..player6).
# Run as root from /opt/werewolf after setup (see README). Logs go to /tmp/ww-demo/.
set -euo pipefail
cd /opt/werewolf
NAMES=(ali bob car dav eve fei gus)
OUT=/tmp/ww-demo; mkdir -p "$OUT"; rm -f "$OUT"/*.log
python3 -u server.py --players 7 --fast --seed 19 --doctor-min 0 --jester-min 0 \
  --tls certs > "$OUT/server.log" 2>&1 &
SRV=$!; sleep 1.5
for i in "${!NAMES[@]}"; do
  runuser -u "player$i" -- python3 -u test/bot_player.py --me "${NAMES[$i]}" \
    > "$OUT/${NAMES[$i]}.log" 2>&1 &
done
wait $SRV || true
sleep 1
grep -v Handshake "$OUT/server.log"
echo; echo "per-player transcripts: $OUT/<name>.log"
