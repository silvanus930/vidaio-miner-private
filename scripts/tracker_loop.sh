#!/bin/bash
# Runs the dashboard refresher on a fixed cadence, forever.
cd /root/vidaio-subnet || exit 1
source venv/bin/activate
while true; do
  python3 scripts/dashboard_refresh.py > /tmp/vidaio-tracker.log 2>&1
  sleep 120
done
