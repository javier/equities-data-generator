#!/usr/bin/env bash
# eq_backfill_ingest.sh
# Batch (faster-than-life) backfill for the equities generator.

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
  --mode faster-than-life \
  --processes 6 \
  --total_market_data_events 600_000_000 \
  --start_ts "2025-11-11T00:00:00.000000Z" \
  --end_ts   "2025-11-11T14:00:00.000000Z" \
  --market_data_min_eps 8200 \
  --market_data_max_eps 11000 \
  --trades_min_eps 1000 \
  --trades_max_eps 1500 \
  --min_levels 3 \
  --max_levels 3 \
  --create_views false \
  --short_ttl false \
  --incremental false \
  --prefix "eq_" \
  --yahoo_refresh_secs 300 \
  --session_pacing true \
  --offsession_trades none \
  --session_tz "America/New_York"
