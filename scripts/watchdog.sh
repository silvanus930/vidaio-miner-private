#!/bin/bash
# Keeps the bore tunnel + bittensor miner axon alive, and re-serves the axon
# whenever the tunnel's remote port changes. Docker already restarts the
# compression container on its own (restart: unless-stopped), so this only
# needs to cover the two plain background processes.

LOCAL_PORT=8091
REMOTE_PORT=39518
TUNNEL_HOST=159.223.110.159
BORE_BIN=/tmp/bore
BORE_LOG=/tmp/bore.log
MINER_LOG=/tmp/vidaio-miner-process.log
REPO=/root/vidaio-subnet
EVENTS_LOG=/tmp/vidaio-watchdog-events.jsonl

log() {
  local ts
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "$ts $1"
  printf '{"ts":"%s","event":"%s"}\n' "$ts" "$1" >> "$EVENTS_LOG"
}

start_bore() {
  pkill -f "bore local $LOCAL_PORT" 2>/dev/null
  sleep 1
  nohup "$BORE_BIN" local "$LOCAL_PORT" --to bore.pub --port "$REMOTE_PORT" > "$BORE_LOG" 2>&1 &
  disown
  log "bore (re)started, requesting port $REMOTE_PORT"
}

start_miner() {
  pkill -f "neurons/miner.py" 2>/dev/null
  sleep 2
  (
    cd "$REPO" || exit 1
    source venv/bin/activate
    nohup python3 neurons/miner.py \
      --wallet.name silvanus-hs1 --wallet.hotkey default \
      --subtensor.network finney --netuid 85 \
      --axon.ip 0.0.0.0 --axon.port "$LOCAL_PORT" \
      --axon.external_ip "$TUNNEL_HOST" --axon.external_port "$REMOTE_PORT" \
      --logging.debug >> "$MINER_LOG" 2>&1 &
    disown
  )
  log "miner (re)started, advertising $TUNNEL_HOST:$REMOTE_PORT"
}

log "watchdog started (local=$LOCAL_PORT remote=$REMOTE_PORT)"

while true; do
  bore_up=0
  pgrep -f "bore local $LOCAL_PORT" > /dev/null && bore_up=1

  if [ "$bore_up" -eq 0 ]; then
    log "bore tunnel is down, restarting"
    start_bore
    sleep 6
    # remote port may have been reassigned; re-serve axon to be safe
    start_miner
  fi

  if ! pgrep -f "neurons/miner.py" > /dev/null; then
    log "miner process is down, restarting"
    start_miner
  fi

  if ! docker inspect -f '{{.State.Running}}' miner-compression-1 2>/dev/null | grep -q true; then
    log "compression container is down, restarting via compose"
    (cd "$REPO/miner" && docker compose up -d compression) >> "$BORE_LOG" 2>&1
  fi

  sleep 20
done
