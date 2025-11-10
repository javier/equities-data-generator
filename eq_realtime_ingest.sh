#!/usr/bin/env bash
# eq_realtime_ingest.sh
# Real-time (wall-clock) ingest for the equities generator.
# Real-time requires --processes 1 and ignores --start_ts.

python equities_data_generator.py \
  --host 172.31.42.41 \
  --pg_port 8812 \
  --user admin \
  --password quest \
  --token "REPLACE_ME_token" \
  --token_x "REPLACE_ME_token_x" \
  --token_y "REPLACE_ME_token_y" \
  --ilp_user ilp_ingest \
  --protocol tcp \
  --mode real-time \
  --processes 1 \
  --total_market_data_events 800_000_000 \
  --market_data_min_eps 1200 \
  --market_data_max_eps 2500 \
  --trades_min_eps 300 \
  --trades_max_eps 800 \
  --min_levels 40 \
  --max_levels 40 \
  --create_views false \
  --short_ttl false \
  --incremental false \
  --suffix "_eq" \
  --yahoo_refresh_secs 300 \
  --session_pacing true \
  --offsession_trades trickle \
  --session_tz "America/New_York"
