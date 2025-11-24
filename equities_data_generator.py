#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# equities_data_generator.py
#
# Equities synthetic data generator for QuestDB using TIMESTAMP_NS everywhere.
# Mirrors the FX architecture:
# - Arrays only in equities_market_data (DOUBLE[][] for bids/asks)
# - top_of_book_1s, nbbo_1s, trades_ohlcv_1s are continuous views
# - 1m rollups refresh on a timer
# - Session pacing in both modes, with --offsession_trades for demos
# - WAL lag monitor pauses and resumes ingestion
# - Yahoo seeding for price brackets (safe fallbacks)


import argparse
import datetime
import math
import random
import time
import sys
import multiprocessing as mp
from multiprocessing import Event
from zoneinfo import ZoneInfo
from typing import Optional

import numpy as np
import psycopg as pg
import yfinance as yf
from questdb.ingress import Sender, TimestampNanos


# ----------------------------
# Symbols and venues
# ----------------------------

SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO",
    "ADBE", "ORCL", "CRM", "NFLX", "AMD", "INTC",
    "JPM", "BAC", "GS", "MS", "V", "MA", "CME", "ICE", "NDAQ",
    "LLY", "JNJ", "UNH",
    "XOM", "CVX",
    "COST", "WMT",
]

VENUES = ["NASDAQ", "NYSE", "ARCA", "BATS", "IEX"]
VENUE_SIZE_MULT = {"NASDAQ": 1.0, "NYSE": 1.0, "ARCA": 0.9, "BATS": 0.9, "IEX": 0.8}
TICK = 0.01


# ----------------------------
# Helpers
# ----------------------------

def now_ns() -> int:
    return time.time_ns()

def parse_ts_arg(ts: str) -> int:
    # Accepts 2025-07-11T14:00:00, 2025-07-11T14:00:00Z, or with offset
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    else:
        dt = dt.astimezone(datetime.timezone.utc)
    return int(dt.timestamp() * 1e9)

def ns_to_iso(ns: int) -> str:
    dt = datetime.datetime.utcfromtimestamp(ns / 1e9)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def quantize(price: float, tick: float = TICK) -> float:
    # Round to the nearest tick and to 2 decimals
    return round(round(price / tick) * tick, 2)

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def make_ladder(levels: int, v_min: int = 100, v_max: int = 5000) -> list[int]:
    step = (math.log10(v_max) - math.log10(v_min)) / max(1, levels - 1)
    return [int(round(10 ** (math.log10(v_min) + i * step))) for i in range(levels)]

def split_event_counts(total: int, num_workers: int) -> list[int]:
    base = total // num_workers
    remainder = total % num_workers
    return [base + (1 if i < remainder else 0) for i in range(num_workers)]

def table_name(name: str, prefix: str) -> str:
    return f"{prefix}{name}" if prefix else name


# ----------------------------
# DB setup
# ----------------------------

def ensure_tables_and_views(args, prefix: str):
    conn_str = (
        f"user={args.user} password={args.password} host={args.host} "
        f"port={args.pg_port} dbname=qdb"
    )
    ttl_md = " TTL 3 DAYS" if args.short_ttl else ""
    ttl_tr = " TTL 1 MONTH" if args.short_ttl else ""
    with pg.connect(conn_str, autocommit=True) as conn:
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name("equities_market_data", prefix)} (
          timestamp TIMESTAMP_NS,
          symbol SYMBOL CAPACITY 256,
          venue  SYMBOL CAPACITY 32,
          bids   DOUBLE[][],
          asks   DOUBLE[][]
        ) timestamp(timestamp) PARTITION BY HOUR{ttl_md};
        """)
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name("equities_trades", prefix)} (
          timestamp TIMESTAMP_NS,
          symbol SYMBOL CAPACITY 256,
          venue  SYMBOL CAPACITY 32,
          price  DOUBLE,
          size   LONG,
          side   SYMBOL,
          cond   SYMBOL
        ) timestamp(timestamp) PARTITION BY HOUR{ttl_tr};
        """)
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name("top_of_book_1s", prefix)} AS (
          SELECT
            timestamp,
            symbol,
            venue,
            last(bids[1][1]) AS bid_price,
            last(bids[2][1]) AS bid_size,
            last(asks[1][1]) AS ask_price,
            last(asks[2][1]) AS ask_size
          FROM {table_name("equities_market_data", prefix)}
          SAMPLE BY 1s
        ) PARTITION BY HOUR{ttl_md};
        """)
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name("nbbo_1s", prefix)} AS (
          SELECT
            timestamp,
            symbol,
            max(bid_price) AS best_bid,
            min(ask_price) AS best_ask,
            min(ask_price) - max(bid_price) AS spread
          FROM {table_name("top_of_book_1s", prefix)}
          SAMPLE BY 1s
        ) PARTITION BY HOUR{ttl_md};
        """)
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name("trades_ohlcv_1s", prefix)} AS (
          SELECT
            timestamp,
            symbol,
            first(price) AS open,
            max(price)   AS high,
            min(price)   AS low,
            last(price)  AS close,
            sum(size)    AS volume
          FROM {table_name("equities_trades", prefix)}
          SAMPLE BY 1s
        ) PARTITION BY HOUR{ttl_md};
        """)
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name("nbbo_1m", prefix)}
        REFRESH EVERY 1m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
          SELECT
            timestamp,
            symbol,
            max(best_bid) AS max_bid,
            min(best_ask) AS min_ask,
            min(best_ask) - max(best_bid) AS min_spread
          FROM {table_name("nbbo_1s", prefix)}
          SAMPLE BY 1m
        ) PARTITION BY HOUR{ttl_tr};
        """)
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name("nbbo_1h", prefix)}
        REFRESH EVERY 10m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
          SELECT
            timestamp,
            symbol,
            max(max_bid) AS max_bid,
            min(min_ask) AS min_ask,
            min(min_ask) - max(max_bid) AS min_spread
          FROM {table_name("nbbo_1h", prefix)}
          SAMPLE BY 1h
        ) PARTITION BY DAY{ttl_tr};
        """)
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name("trades_ohlcv_1m", prefix)}
        REFRESH EVERY 1m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
          SELECT
            timestamp,
            symbol,
            first(open)  AS open,
            max(high)    AS high,
            min(low)     AS low,
            last(close)  AS close,
            sum(volume)  AS volume
          FROM {table_name("trades_ohlcv_1s", prefix)}
          SAMPLE BY 1m
        ) PARTITION BY DAY{ttl_tr};
        """)
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name("trades_ohlcv_15m", prefix)}
        REFRESH EVERY 1m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
          SELECT
            timestamp,
            symbol,
            first(open)  AS open,
            max(high)    AS high,
            min(low)     AS low,
            last(close)  AS close,
            sum(volume)  AS volume
          FROM {table_name("trades_ohlcv_1m", prefix)}
          SAMPLE BY 1m
        ) PARTITION BY DAY{ttl_tr};
        """)

def get_latest_timestamp_ns(conn, table: str):
    cur = conn.execute(
        f"SELECT timestamp FROM {table} ORDER BY timestamp DESC LIMIT 1"
    )
    row = cur.fetchone()
    if row and row[0]:
        dt = row[0]
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            dt = dt.astimezone(datetime.timezone.utc)
        return int(dt.timestamp() * 1e9)
    return None


# ----------------------------
# Yahoo reference brackets
# ----------------------------

def fetch_symbol_brackets(symbols, pct: float = 1.0):
    out = {}
    frac = pct / 100.0
    print("[INFO] Refreshing equity brackets from Yahoo.", flush=True)

    for sym in symbols:
        try:
            # Use a larger period and daily bars for better chance of data
            bars = yf.Ticker(sym).history(period="5d", interval="1d")

            if not bars.empty:
                closes = bars["Close"].dropna()
                if closes.empty:
                    raise ValueError("Yahoo returned no non-null closes")
                mid = float(closes.iloc[-1])  # latest known close in that window
                if math.isnan(mid) or mid == 0.0:
                    raise ValueError("Yahoo price NaN or zero")
                low = mid * (1 - frac)
                high = mid * (1 + frac)
            else:
                raise ValueError("Yahoo returned empty dataframe")
        except Exception:
            # Deterministic synthetic fallback
            base = 100.0 + (abs(hash(sym)) % 200)  # 100–300 fallback anchor
            low = base * (1 - frac)
            high = base * (1 + frac)
            print(
                f"[YF] {sym}: Yahoo returned no usable data, fallback "
                f"bracket [{low:.2f}, {high:.2f}]",
                flush=True,
            )
        out[sym] = (low, high)
    return out


# ----------------------------
# Session pacing
# ----------------------------

def session_phase(ts_ns: int, tz: str = "America/New_York") -> str:
    tzinfo = ZoneInfo(tz)
    dt_local = datetime.datetime.fromtimestamp(ts_ns / 1e9, tzinfo)
    if dt_local.weekday() > 4:
        return "off"
    t = dt_local.time()
    if t < datetime.time(9, 30):
        return "pre"
    if t < datetime.time(15, 50):
        return "continuous"
    if t <= datetime.time(16, 0):
        return "close_auction"
    return "post"

# -------
# Emit in order per batch
# -------

class SortedEmitter:
    """
    Buffers events per table, keeps them in memory until buffer_limit,
    sorts by timestamp, sends them through the ILP Sender, flushes,
    and clears the buffers.

    This ensures that per-table events are sent in timestamp order,
    even though generation happens in arbitrary order inside the second.
    """

    def __init__(self, sender, buffer_limit, prefix):
        self.sender = sender
        self.buffer_limit = buffer_limit
        self.prefix = prefix

        # Per-table buffers
        self._md_buffer = []   # equities_market_data
        self._tr_buffer = []   # equities_trades

    # ---------- internal generic send ----------

    def _send_buffer(self, buf, table):
        if not buf:
            return

        # Sort by timestamp
        buf.sort(key=lambda r: r["ts"])

        for row in buf:
            self.sender.row(
                table_name(table, self.prefix),
                symbols=row["symbols"],
                columns=row["columns"],
                at=TimestampNanos(row["ts"]),
            )

        self.sender.flush()
        buf.clear()

    # ---------- public APIs used by generator ----------

    def emit_market(self, ts_ns, sym, ven, bids, asks):
        # bids/asks are reused numpy arrays, copy to freeze the snapshot
        self._md_buffer.append(
            {
                "ts": ts_ns,
                "symbols": {"symbol": sym, "venue": ven},
                "columns": {
                    "bids": bids.copy(),
                    "asks": asks.copy(),
                },
            }
        )
        if len(self._md_buffer) >= self.buffer_limit:
            self._send_buffer(self._md_buffer, "equities_market_data")

    def emit_trade(self, ts_ns, sym, venue, side, price, size):
        self._tr_buffer.append(
            {
                "ts": ts_ns,
                "symbols": {
                    "symbol": sym,
                    "venue": venue,
                    "side": side,
                    "cond": "T",
                },
                "columns": {
                    "price": float(price),
                    "size": int(size),
                },
            }
        )
        if len(self._tr_buffer) >= self.buffer_limit:
            self._send_buffer(self._tr_buffer, "equities_trades")

    def flush_all(self):
        self._send_buffer(self._md_buffer, "equities_market_data")
        self._send_buffer(self._tr_buffer, "equities_trades")


# ----------------------------
# State evolution
# ----------------------------

def evolve_mid(
    prev_mid: float,
    low: float,
    high: float,
    drift_ticks: float = 2.0,   # 0.2 for calmer, 3 or more for more volatility
) -> float:
    # Normal drift: ± drift_ticks * TICK
    change = random.uniform(-drift_ticks * TICK, drift_ticks * TICK)

    # Rare fat-tail jumps
    if random.random() < 0.002:
        change += random.uniform(-10 * TICK, 10 * TICK)

    mid = clamp(prev_mid + change, low, high)
    return quantize(mid)


def generate_l2_for_symbol(
    symbol: str,
    venue: str,
    best_bid: float,
    best_ask: float,
    levels: int,
    ladder: list[int],
    bids_buf: np.ndarray,
    asks_buf: np.ndarray,
):
    for i in range(levels):
        bids_buf[0][i] = quantize(best_bid - i * TICK)
        asks_buf[0][i] = quantize(best_ask + i * TICK)
        size_mult = VENUE_SIZE_MULT.get(venue, 1.0)
        base = ladder[min(i, len(ladder) - 1)]
        bids_buf[1][i] = int(random.randint(base // 2, base) * size_mult)
        asks_buf[1][i] = int(random.randint(base // 2, base) * size_mult)
    return bids_buf, asks_buf


# ----------------------------
# Per-second generation
# ----------------------------
def generate_second(
    ts_ns_base: int,
    symbols: list[str],
    venues: list[str],
    open_state: dict[str, dict],
    close_state: dict[str, dict],
    emitter: SortedEmitter,
    ladder: list[int],
    min_levels: int,
    max_levels: int,
    md_events: int,
    tr_events: int,
    allow_trades: bool,
):
    # Prebuilt ladders for each depth
    prebuilt_bids = [
        np.zeros((2, lvl), dtype=np.float64) for lvl in range(1, max_levels + 1)
    ]
    prebuilt_asks = [
        np.zeros((2, lvl), dtype=np.float64) for lvl in range(1, max_levels + 1)
    ]

    # Small, fixed venue bias so NBBO is realistic but not crazy
    venue_bias = {
        "NASDAQ": 0.00,       # reference
        "NYSE":   +0.01,      # 1 cent worse
        "ARCA":   +0.02,
        "BATS":   +0.02,
        "IEX":    -0.01,      # sometimes price improvement
    }

    # ----------------------------------------------------
    # 1) Choose which (symbol, venue) gets events this sec
    #    Guarantee: if md_events >= len(symbols)*len(venues),
    #    then every (symbol, venue) has at least 1 event.
    # ----------------------------------------------------
    all_pairs = [(sym, ven) for sym in symbols for ven in venues]
    num_pairs = len(all_pairs)

    if md_events >= num_pairs:
        # One guaranteed event per pair, rest distributed randomly.
        md_pairs = list(all_pairs)
        extra = md_events - num_pairs
        for _ in range(extra):
            md_pairs.append(random.choice(all_pairs))
    else:
        # Not enough EPS to touch all pairs: fall back to random.
        # (You are never in this regime with your current settings.)
        md_pairs = [
            (random.choice(symbols), random.choice(venues))
            for _ in range(md_events)
        ]

    md_events = len(md_pairs)

    # Random offsets inside the second (one per MD event)
    md_offsets = sorted(
        random.randint(0, 999_999_999) for _ in range(md_events)
    )

    # Group global indices by symbol so we can interpolate within the second
    per_symbol_indices: dict[str, list[int]] = {sym: [] for sym in symbols}
    for idx, (sym, _ven) in enumerate(md_pairs):
        per_symbol_indices[sym].append(idx)

    # ---------------------------------
    # 2) L2 market data, smooth per symbol
    # ---------------------------------
    for sym, idx_list in per_symbol_indices.items():
        if not idx_list:
            continue

        ob = open_state[sym]
        cb = close_state[sym]
        n = len(idx_list)

        for j, global_idx in enumerate(idx_list):
            ven = md_pairs[global_idx][1]
            ofs = md_offsets[global_idx]

            # Linear interpolation from open to close inside the second
            frac = 0.0 if n == 1 else j / (n - 1)

            mid_bid = ob["bid"] + frac * (cb["bid"] - ob["bid"])
            mid_ask = ob["ask"] + frac * (cb["ask"] - ob["ask"])

            biased_bid = mid_bid + venue_bias.get(ven, 0.0)
            biased_ask = mid_ask + venue_bias.get(ven, 0.0)

            best_bid = quantize(biased_bid)
            best_ask = quantize(biased_ask)

            levels = random.randint(min_levels, max_levels)
            bids = prebuilt_bids[levels - 1]
            asks = prebuilt_asks[levels - 1]

            generate_l2_for_symbol(
                sym, ven, best_bid, best_ask, levels, ladder, bids, asks
            )
            emitter.emit_market(
                ts_ns_base + ofs,
                sym,
                ven,
                bids,
                asks,
            )

    # ---------------------------------
    # 3) Trades, priced inside best bid/ask
    # ---------------------------------
    if not allow_trades or tr_events <= 0:
        return

    # Random offsets and symbols for trades
    tr_offsets = sorted(
        random.randint(0, 999_999_999) for _ in range(tr_events)
    )
    tr_symbols = [random.choice(symbols) for _ in range(tr_events)]

    for ofs, sym in zip(tr_offsets, tr_symbols):
        ob = open_state[sym]
        cb = close_state[sym]

        # Random point between open and close during the second
        frac = random.random()
        best_bid = ob["bid"] + frac * (cb["bid"] - ob["bid"])
        best_ask = ob["ask"] + frac * (cb["ask"] - ob["ask"])

        mid = (best_bid + best_ask) / 2.0
        slip = random.uniform(-0.15 * TICK, 0.15 * TICK)
        price = quantize(clamp(mid + slip, best_bid, best_ask))

        side = "B" if random.random() < 0.5 else "S"
        venue = random.choice(venues)

        r = random.random()
        if r < 0.50:
            size = random.randint(1, 99)
        elif r < 0.93:
            size = random.choice(
                [100, 200, 300, 400, 500, 600, 800, 1000]
            )
        else:
            size = random.randint(1000, 10000)

        emitter.emit_trade(
            ts_ns_base + ofs,
            sym,
            venue,
            side,
            price,
            size,
        )


def evolve_open_close_for_second(symbols, brackets, prev_state):
    open_state = {}
    close_state = {}
    for sym in symbols:
        low, high = brackets[sym]
        prev_mid = (prev_state[sym]["bid"] + prev_state[sym]["ask"]) / 2.0
        new_mid = evolve_mid(prev_mid, low, high)
        spread = clamp(
            prev_state[sym]["spread"] +
            random.uniform(-0.3 * TICK, 0.3 * TICK),  # or -0.1*TICK..0.1*TICK for calmer spreads
            TICK,           # minimum spread = 1 tick (generic)
            5 * TICK       # maximum spread = 5 ticks (0.05 with TICK=0.01)
        )

        bid = quantize(new_mid - spread / 2.0)
        ask = quantize(new_mid + spread / 2.0)
        open_state[sym] = {
            "bid": prev_state[sym]["bid"],
            "ask": prev_state[sym]["ask"],
            "spread": prev_state[sym]["spread"],
        }
        close_state[sym] = {"bid": bid, "ask": ask, "spread": spread}
        prev_state[sym] = {"bid": bid, "ask": ask, "spread": spread}
    return open_state, close_state, prev_state


# ----------------------------
# Backpressure (WAL)
# ----------------------------

def wal_monitor(args, pause_event, processes, interval=5, prefix=""):
    conn_str = (
        f"user={args.user} password={args.password} host={args.host} "
        f"port={args.pg_port} dbname=qdb"
    )
    threshold = 3 * processes
    last_logged_paused = False
    tbl = table_name("equities_market_data", prefix)
    with pg.connect(conn_str, autocommit=True) as conn:
        while True:
            cur = conn.execute(
                f"SELECT sequencerTxn, writerTxn FROM wal_tables() WHERE name = '{tbl}'"
            )
            row = cur.fetchone()
            if row:
                seq, wrt = row
                lag = seq - wrt
                if lag > threshold:
                    pause_event.set()
                    if not last_logged_paused:
                        print(f"[WAL] Pause: sequencerTxn={seq}, writerTxn={wrt}, lag={lag}")
                        last_logged_paused = True
                elif last_logged_paused:
                    if seq == wrt:
                        time.sleep(interval)
                        print(f"[WAL] Resume: sequencerTxn={seq}, writerTxn={wrt}, lag={lag}")
                        pause_event.clear()
                        last_logged_paused = False
            time.sleep(interval)

def wait_if_paused(pause_event: Event, pid: int):
    while pause_event.is_set():
        print(f"[WORKER {pid}] Paused due to WAL lag...")
        time.sleep(5)


# ----------------------------
# Worker loop
# ----------------------------

def ingest_worker(
    args,
    per_second_plan,                 # list[(md_events, tr_events)] or None
    start_ns: int,
    end_ns: Optional[int],
    symbols: list[str],
    venues: list[str],
    brackets: dict[str, tuple[float, float]],
    process_idx: int,
    processes: int,
    pause_event: Event,
    global_sec_offset,
):
    ladder = make_ladder(args.max_levels)

    # Local mutable copy of brackets so we can refresh them in real-time mode
    local_brackets = dict(brackets)

    # initialize per-symbol state from brackets
    init_state = {}
    for s in symbols:
        low, high = brackets[s]
        mid = (low + high) / 2.0
        spread = 0.02
        init_state[s] = {
            "bid": quantize(mid - spread / 2.0),
            "ask": quantize(mid + spread / 2.0),
            "spread": spread,
        }

    if args.mode == "real-time":
        base_flush = 1000
    else:
        base_flush = 10000

    buffer_limit = base_flush           # how many rows we buffer before manual flush
    auto_flush_interval = base_flush * 2  # safety net in the ILP client

    if args.protocol == "http":
        conf = (
            f"http::addr={args.host}:9000;auto_flush_interval={auto_flush_interval};"
            if not args.token else
            f"https::addr={args.host}:9000;token={args.token};tls_verify=unsafe_off;"
            f"auto_flush_interval={auto_flush_interval};"
        )
    else:
        conf = (
            f"tcp::addr={args.host}:9009;protocol_version=2;auto_flush_interval={auto_flush_interval};"
            if not args.token else
            f"tcps::addr={args.host}:9009;username={args.ilp_user};token={args.token};"
            f"token_x={args.token_x};token_y={args.token_y};tls_verify=unsafe_off;"
            f"protocol_version=2;auto_flush_interval={auto_flush_interval};"
        )

    with Sender.from_conf(conf) as sender:
        emitter = SortedEmitter(sender, buffer_limit, args.prefix)
        ts = start_ns
        sec_idx = 0
        wall_start = time.time() if args.mode == "real-time" else None

        # For real-time Yahoo refresh
        last_yf_refresh = time.time()

        while True:
            if end_ns is not None and ts >= end_ns:
                # Flush any remaining buffered events in sorted order
                emitter.flush_all()
                break

            # ---------------------------
            # Real-time: refresh brackets
            # ---------------------------
            if args.mode == "real-time" and args.yahoo_refresh_secs > 0:
                now = time.time()
                if now - last_yf_refresh >= args.yahoo_refresh_secs:
                    try:
                        local_brackets = fetch_symbol_brackets(symbols, pct=1.0)
                        last_yf_refresh = now
                        print(
                            "[INFO] Refreshed equity brackets from Yahoo "
                            f"(worker {process_idx}).",
                            flush=True,
                        )
                    except Exception as e:
                        # Do not kill the worker on Yahoo hiccups
                        print(
                            f"[WARN] Failed to refresh equity brackets from Yahoo: {e}",
                            flush=True,
                        )

            if args.mode == "faster-than-life" and per_second_plan is not None:
                if sec_idx >= len(per_second_plan):
                    # Flush any remaining buffered events in sorted order
                    emitter.flush_all()
                    break
                md_events, tr_events = per_second_plan[sec_idx]
            else:
                # real-time mode: one worker only, random EPS per second
                md_events = random.randint(args.market_data_min_eps, args.market_data_max_eps)
                tr_events = random.randint(args.trades_min_eps, args.trades_max_eps)

            # --------------------------------------------
            # DISABLE ALL pacing if session_pacing = false
            # --------------------------------------------
            if not args.session_pacing:
                allow_trades = True
                md_scale = 1.0
                tr_scale = 1.0

            else:
                phase = session_phase(ts, args.session_tz)

                # Make pacing symmetric across BOTH tables
                allow_trades = True
                scale = 1.0
                if phase == "pre":
                    allow_trades = False
                    scale = 0.4
                elif phase == "continuous":
                    allow_trades = True
                    scale = 1.0
                elif phase == "close_auction":
                    allow_trades = True
                    scale = 1.3
                else:  # post-session
                    allow_trades = False
                    scale = 0.2

                md_scale = scale
                tr_scale = scale

                # Off-session trade override for demos
                if not allow_trades:
                    if args.offsession_trades == "full":
                        allow_trades = True
                        tr_scale = 1.0
                    elif args.offsession_trades == "trickle":
                        allow_trades = True
                        tr_scale = 0.1

            md_events = int(max(0, md_events * md_scale))
            tr_events = int(max(0, tr_events * tr_scale))

            wait_if_paused(pause_event, process_idx)

            # Use *local_brackets* so Yahoo refresh affects evolution in real-time mode
            open_state, close_state, init_state = evolve_open_close_for_second(
                symbols, local_brackets, init_state
            )


            generate_second(
                ts_ns_base=ts,
                symbols=symbols,
                venues=venues,
                open_state=open_state,
                close_state=close_state,
                emitter=emitter,
                ladder=ladder,
                min_levels=args.min_levels,
                max_levels=args.max_levels,
                md_events=md_events,
                tr_events=tr_events,
                allow_trades=allow_trades,
            )

            ts += int(1e9)
            sec_idx += 1
            if args.mode == "real-time":
                target = (wall_start + sec_idx)
                sleep_for = target - time.time()
                if sleep_for > 0:
                    time.sleep(sleep_for)


# ----------------------------
# Main
# ----------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--pg_port", default="8812")
    p.add_argument("--user", default="admin")
    p.add_argument("--password", default="quest")
    p.add_argument("--token", default=None)
    p.add_argument("--token_x", default=None)
    p.add_argument("--token_y", default=None)
    p.add_argument("--ilp_user", default="admin")
    p.add_argument("--protocol", choices=["http", "tcp"], default="http")

    p.add_argument("--mode", choices=["real-time", "faster-than-life"], required=True)

    p.add_argument("--market_data_min_eps", type=int, default=1000)
    p.add_argument("--market_data_max_eps", type=int, default=15000)
    p.add_argument("--trades_min_eps", type=int, default=800)
    p.add_argument("--trades_max_eps", type=int, default=1100)

    p.add_argument("--total_market_data_events", type=int, default=1_000_000)
    p.add_argument("--start_ts", type=str)
    p.add_argument("--end_ts", type=str)
    p.add_argument("--processes", type=int, default=1)

    p.add_argument("--min_levels", type=int, default=40)
    p.add_argument("--max_levels", type=int, default=40)

    p.add_argument("--incremental", type=lambda x: str(x).lower() != "false", default=False)
    p.add_argument("--create_views", type=lambda x: str(x).lower() != "false", default=True)
    p.add_argument("--short_ttl", type=lambda x: str(x).lower() == "true", default=False)
    p.add_argument("--prefix", type=str, default="")
    p.add_argument("--yahoo_refresh_secs", type=int, default=300)

    p.add_argument("--session_pacing", type=lambda x: str(x).lower() != "false", default=True)
    p.add_argument("--offsession_trades", choices=["none", "trickle", "full"], default="none")
    p.add_argument("--session_tz", type=str, default="America/New_York")

    args = p.parse_args()
    prefix = args.prefix

    if args.min_levels > args.max_levels:
        print("ERROR: min_levels cannot be greater than max_levels.")
        sys.exit(1)

    # Ensure base objects
    ensure_tables_and_views(args, prefix)

    # Determine window
    if args.mode == "real-time":
        if args.start_ts:
            print("ERROR: --start_ts is not allowed in real-time mode.")
            sys.exit(1)
        start_ns = now_ns()
        end_ns = parse_ts_arg(args.end_ts) if args.end_ts else None
    else:
        start_ns = parse_ts_arg(args.start_ts) if args.start_ts else now_ns()
        end_ns = parse_ts_arg(args.end_ts) if args.end_ts else None

    # Advance start_ns past latest in either table to avoid overlap
    conn_str = (
        f"user={args.user} password={args.password} host={args.host} "
        f"port={args.pg_port} dbname=qdb"
    )
    with pg.connect(conn_str) as conn:
        latest_md = get_latest_timestamp_ns(conn, table_name("equities_market_data", prefix))
        latest_tr = get_latest_timestamp_ns(conn, table_name("equities_trades", prefix))
    max_latest = max([x for x in [latest_md, latest_tr] if x is not None], default=None)
    if max_latest is not None:
        next_ns = max_latest + 1_000  # 1 microsecond after last row
        if next_ns > start_ns:
            print(f"[INFO] Advancing start_ns from {ns_to_iso(start_ns)} to {ns_to_iso(next_ns)} to avoid overlap.")
            start_ns = next_ns

    if end_ns is not None and start_ns >= end_ns:
        print("[INFO] No work to do. start_ts >= end_ts.")
        sys.exit(0)

    # Seed brackets once
    symbols = SYMBOLS
    venues = VENUES
    brackets = fetch_symbol_brackets(symbols, pct=1.0)

    # WAL monitor
    pause_event = Event()
    wal_proc = mp.Process(
        target=wal_monitor, args=(args, pause_event, args.processes),
        kwargs={"prefix": prefix}
    )
    wal_proc.start()

    if args.mode == "faster-than-life":
        # Build per-second plan for requested MD and TR events
        per_second_plan = []
        md_total = 0

        while md_total < args.total_market_data_events:
            md_this = random.randint(args.market_data_min_eps, args.market_data_max_eps)
            tr_this = random.randint(args.trades_min_eps, args.trades_max_eps)
            per_second_plan.append((md_this, tr_this))
            md_total += md_this

        # Trim excess from final second
        over = md_total - args.total_market_data_events
        if over > 0:
            last_md, last_tr = per_second_plan[-1]
            per_second_plan[-1] = (max(0, last_md - over), last_tr)

        # Split each second evenly across workers
        worker_plans = [[] for _ in range(args.processes)]
        for md_sec, tr_sec in per_second_plan:
            md_splits = split_event_counts(md_sec, args.processes)
            tr_splits = split_event_counts(tr_sec, args.processes)
            for i in range(args.processes):
                worker_plans[i].append((md_splits[i], tr_splits[i]))

        global_sec_offsets = []
        acc = 0
        for plan in worker_plans:
            global_sec_offsets.append(acc)
            acc += len(plan)

        # Launch workers
        procs = []
        for i in range(args.processes):
            w = mp.Process(
                target=ingest_worker,
                args=(
                    args,
                    worker_plans[i],
                    start_ns,
                    end_ns,
                    symbols,
                    venues,
                    brackets,
                    i,
                    args.processes,
                    pause_event,
                    global_sec_offsets[i],
                ),
            )
            w.start()
            procs.append(w)
        for w in procs:
            w.join()

    else:
        # Real-time single process
        w = mp.Process(
            target=ingest_worker,
            args=(
                args,
                None,  # no explicit plan
                start_ns,
                end_ns,
                symbols,
                venues,
                brackets,
                0,
                1,
                pause_event,
                0,   # global offset for real-time mode
            ),
        )
        w.start()
        w.join()

    wal_proc.terminate()
    wal_proc.join()
    print("[INFO] Completed.")

if __name__ == "__main__":
    main()
