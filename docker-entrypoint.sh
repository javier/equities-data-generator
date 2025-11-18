#!/bin/bash
set -e

# Build the python command from env vars for the equities generator
exec python equities_data_generator.py \
  --mode "${MODE}" \
  --short_ttl "${SHORT_TTL}" \
  --market_data_min_eps "${MARKET_DATA_MIN_EPS}" \
  --market_data_max_eps "${MARKET_DATA_MAX_EPS}" \
  --trades_min_eps "${TRADES_MIN_EPS}" \
  --trades_max_eps "${TRADES_MAX_EPS}" \
  --total_market_data_events "${TOTAL_MARKET_DATA_EVENTS}" \
  --protocol "${PROTOCOL}" \
  --host "${HOST}" \
  --user "${PG_USER}" \
  --password "${PG_PASSWORD}" \
  --pg_port "${PG_PORT}" \
  --token "${TOKEN}" \
  --token_x "${TOKEN_X}" \
  --token_y "${TOKEN_Y}" \
  --ilp_user "${ILP_USER}" \
  --yahoo_refresh_secs "${YAHOO_REFRESH_SECS}" \
  --suffix "${SUFFIX}" \
  --session_pacing "${SESSION_PACING}" \
  --offsession_trades "${OFFSESSION_TRADES}" \
  --session_tz "${SESSION_TZ}" \
  --processes "${PROCESSES}" \
  --min_levels "${MIN_LEVELS}" \
  --max_levels "${MAX_LEVELS}" \
  --create_views "${CREATE_VIEWS}" \
  --incremental "${INCREMENTAL}" \
  "$@"
