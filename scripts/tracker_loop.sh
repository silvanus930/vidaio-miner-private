#!/bin/bash
# Runs the dashboard refresher on a fixed cadence, forever. Also makes sure
# watchdog.sh itself is alive each cycle, since nothing else supervises the
# supervisor -- if watchdog.sh dies, bore/miner/compression lose their
# auto-restart safety net silently.
cd /root/vidaio-subnet || exit 1
source venv/bin/activate
while true; do
  if ! pgrep -f "scripts/watchdog.sh" > /dev/null; then
    printf '{"ts":"%s","event":"tracker_loop: watchdog.sh was not running, restarted it"}\n' \
      "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> /tmp/vidaio-watchdog-events.jsonl
    nohup /root/vidaio-subnet/scripts/watchdog.sh >> /tmp/vidaio-watchdog-stdout.log 2>&1 &
    disown
  fi
  python3 scripts/dashboard_refresh.py > /tmp/vidaio-tracker.log 2>&1
  sleep 120
done
