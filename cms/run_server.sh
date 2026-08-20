#!/bin/bash
# Keeps the CMS API/UI server alive, restarting on crash.
cd "$(dirname "$0")" || exit 1
source ../venv/bin/activate
while true; do
  python3 -m uvicorn server:app --host 0.0.0.0 --port 8005 >> /tmp/cms-server.log 2>&1
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) server exited, restarting in 5s" >> /tmp/cms-server.log
  sleep 5
done
