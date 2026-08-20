#!/bin/bash
# Keeps the CMS ingest loop alive, restarting on crash. ingest.py has its
# own internal polling loop; this wrapper only guards against the process
# dying outright (uncaught exception, OOM, etc).
cd "$(dirname "$0")" || exit 1
source ../venv/bin/activate
while true; do
  python3 ingest.py >> /tmp/cms-ingest.log 2>&1
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) ingest.py exited, restarting in 5s" >> /tmp/cms-ingest.log
  sleep 5
done
