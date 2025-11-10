# Equities Synthetic Data Generator for QuestDB (TIMESTAMP_NS)

This script  generates highly realistic U.S. equities with
multi-venue L2 books and trades. Arrays appear **only** in the L2 source table;
all views publish scalars (top of book, NBBO, OHLCV). Everything uses
**TIMESTAMP_NS**, and the writer emits **nanosecond** timestamps.

It supports:
- **Modes:** `real-time` (wall-clock) and `faster-than-life` (max throughput)
- **Session pacing in both modes** with a demo switch to allow off-session trading:
  `--offsession_trades=none|trickle|full`
- **Backpressure control** using WAL lag (pause/resume)
- **Yahoo seeding** for symbol price brackets (with safe fallbacks)
- **Suffixing/TTL** consistent with your FX setup

---

## Schema (arrays only in the L2 table)

```sql
-- L2 snapshots per venue
CREATE TABLE IF NOT EXISTS equities_market_data${SUFFIX} (
  timestamp_ns TIMESTAMP_NS,
  symbol       SYMBOL CAPACITY 256,
  venue        SYMBOL CAPACITY 32,
  bids         DOUBLE[][],   -- [2 x N]: row 1 prices, row 2 sizes
  asks         DOUBLE[][]    -- [2 x N]: row 1 prices, row 2 sizes
)
timestamp(timestamp_ns)
PARTITION BY HOUR;

-- Executed trades
CREATE TABLE IF NOT EXISTS equities_trades${SUFFIX} (
  timestamp_ns TIMESTAMP_NS,
  symbol       SYMBOL CAPACITY 256,
  venue        SYMBOL CAPACITY 32,
  price        DOUBLE,
  size         LONG,
  side         SYMBOL,       -- "B" buyer initiated, "S" seller initiated
  cond         SYMBOL        -- "T" trade, "O" open, "C" close, "H" halt resume
)
timestamp(timestamp_ns)
PARTITION BY DAY;
```

**Continuous materialized views (live refresh, no timer):**

```sql
-- Top of book per venue (derived from arrays)
CREATE MATERIALIZED VIEW IF NOT EXISTS top_of_book_1s${SUFFIX} AS (
  SELECT
    timestamp_ns,
    symbol,
    venue,
    last(bids[1][1]) AS bid_price,
    last(bids[2][1]) AS bid_size,
    last(asks[1][1]) AS ask_price,
    last(asks[2][1]) AS ask_size
  FROM equities_market_data${SUFFIX}
  SAMPLE BY 1s
)
PARTITION BY HOUR;

-- NBBO per symbol
CREATE MATERIALIZED VIEW IF NOT EXISTS nbbo_1s${SUFFIX} AS (
  SELECT
    timestamp_ns,
    symbol,
    max(bid_price) AS best_bid,
    min(ask_price) AS best_ask,
    min(ask_price) - max(bid_price) AS spread
  FROM top_of_book_1s${SUFFIX}
  SAMPLE BY 1s
)
PARTITION BY HOUR;

-- Trades OHLCV + VWAP per symbol
CREATE MATERIALIZED VIEW IF NOT EXISTS trades_ohlcv_1s${SUFFIX} AS (
  SELECT
    timestamp_ns,
    symbol,
    first(price) AS open,
    max(price)   AS high,
    min(price)   AS low,
    last(price)  AS close,
    sum(size)    AS volume,
    sum(price * size) / nullif(sum(size), 0) AS vwap
  FROM equities_trades${SUFFIX}
  SAMPLE BY 1s
)
PARTITION BY HOUR;
```

**Timed rollups (cheap long-range queries):**

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS nbbo_1m${SUFFIX}
REFRESH EVERY 1m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
  SELECT
    timestamp_ns,
    symbol,
    max(best_bid) AS max_bid,
    min(best_ask) AS min_ask,
    min(best_ask) - max(best_bid) AS min_spread
  FROM nbbo_1s${SUFFIX}
  SAMPLE BY 1m
)
PARTITION BY DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS trades_ohlcv_1m${SUFFIX}
REFRESH EVERY 1m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
  SELECT
    timestamp_ns,
    symbol,
    first(open)  AS open,
    max(high)    AS high,
    min(low)     AS low,
    last(close)  AS close,
    sum(volume)  AS volume,
    sum(vwap * volume) / nullif(sum(volume), 0) AS vwap
  FROM trades_ohlcv_1s${SUFFIX}
  SAMPLE BY 1m
)
PARTITION BY DAY;
```

> For demo TTLs, add `TTL 3 DAYS` to HOUR partitions and `TTL 1 MONTH`
> to DAY/MONTH partitions, mirroring your FX setup.

---

## Symbols (30) and venues

**Tech / Mega-cap (14):**
AAPL, MSFT, NVDA, AMZN, GOOGL, META, TSLA, AVGO, ADBE, ORCL, CRM, NFLX, AMD, INTC

**Finance / Market Infra (9):**
JPM, BAC, GS, MS, V, MA, CME, ICE, NDAQ

**Healthcare (3):**
LLY, JNJ, UNH

**Energy (2):**
XOM, CVX

**Consumer (2):**
COST, WMT

**Venues:**
NASDAQ, NYSE, ARCA, BATS, IEX

---

## Spreads and volumes (defaults)

- **Tick size:** 0.01 USD
- **Typical spread:** 0.02–0.05 (tight for mega-caps, wider on bursts)
- **Book depth (shares per level):**
  - L1: 100–400
  - L2: 150–600
  - L3: 200–800
  - L4..N: gently increasing to ~3,000–5,000 by deep levels
- **Venue size multipliers:** NASDAQ 1.0, NYSE 1.0, ARCA 0.9, BATS 0.9, IEX 0.8
- **Trade sizes:** odd lots 1–99 (40–55%), round lots 100–1000 (35–45%),
  blocks 1,000–10,000 (5–10%); average ~150–250 shares for mega-caps

---

## Session pacing (applies to both modes)

- **Timezone:** `America/New_York`
- **Phases:** pre-open (quotes only), continuous (normal), close auction (bursty),
  post-close (taper)
- **Flag:** `--session_pacing=true|false` (default true)
- **Off-session control:** `--offsession_trades=none|trickle|full` (default **none**)
  - `none`: no trades outside regular hours (quotes may continue)
  - `trickle`: low-rate trades for demos when markets are closed
  - `full`: ignore session; always allow trades

For early-UTC live demos, use `--offsession_trades=trickle` or `full`.

---

## Usage

**Faster-than-life (time-boxed ingestion):**
```bash
python equities_data_generator.py \
  --host 127.0.0.1 \
  --protocol tcp \
  --mode faster-than-life \
  --processes 6 \
  --market_data_min_eps 6000 \
  --market_data_max_eps 10000 \
  --trades_min_eps 2000 \
  --trades_max_eps 4000 \
  --total_market_data_events 100_000_000 \
  --start_ts "2025-07-01T13:00:00Z" \
  --end_ts   "2025-07-01T16:00:00Z" \
  --session_pacing true \
  --offsession_trades none
```

**Real-time (wall-clock; allow trickle after hours):**
```bash
python equities_data_generator.py \
  --host 127.0.0.1 \
  --protocol http \
  --mode real-time \
  --processes 1 \
  --market_data_min_eps 800 \
  --market_data_max_eps 2000 \
  --trades_min_eps 300 \
  --trades_max_eps 800 \
  --session_pacing true \
  --offsession_trades trickle
```

---

## Flags

**Common:**
`--host`, `--pg_port`, `--user`, `--password`, `--protocol [http|tcp]`,
`--token`, `--token_x`, `--token_y`, `--ilp_user`, `--mode`, `--processes`,
`--suffix`, `--create_views`, `--short_ttl`, `--yahoo_refresh_secs`

**Rates:**
`--market_data_min_eps`, `--market_data_max_eps`,
`--trades_min_eps`, `--trades_max_eps`

**Timing:**
`--start_ts`, `--end_ts` (faster-than-life only)

**Behavior:**
`--session_pacing`, `--offsession_trades [none|trickle|full]`,
`--incremental` (faster-than-life only)

**Notes:**
The generator advances `start_ns` past the latest row in either base table to
avoid overlap. All ingestion uses `TimestampNanos(ns)` with nanosecond offsets.
