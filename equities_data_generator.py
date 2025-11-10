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

def table_name(name: str, suffix: str) -> str:
    return name + suffix if suffix else name


# ----------------------------
# DB setup
# ----------------------------

def ensure_tables_and_views(args, suffix: str):
    conn_str = (
        f"user={args.user} password={args.password} host={args.host} "
        f"port={args.pg_port} dbname=qdb"
    )
    ttl_md = " TTL 3 DAYS" if args.short_ttl else ""
    ttl_tr = " TTL 1 MONTH" if args.short_ttl else ""
    with pg.connect(conn_str, autocommit=True) as conn:
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name("equities_market_data", suffix)} (
          timestamp_ns TIMESTAMP_NS,
          symbol SYMBOL CAPACITY 256,
          venue  SYMBOL CAPACITY 32,
          bids   DOUBLE[][],
          asks   DOUBLE[][]
        ) timestamp(timestamp_ns) PARTITION BY HOUR{ttl_md};
        """)
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name("equities_trades", suffix)} (
          timestamp_ns TIMESTAMP_NS,
          symbol SYMBOL CAPACITY 256,
          venue  SYMBOL CAPACITY 32,
          price  DOUBLE,
          size   LONG,
          side   SYMBOL,
          cond   SYMBOL
        ) timestamp(timestamp_ns) PARTITION BY DAY{ttl_tr};
        """)
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name("top_of_book_1s", suffix)} AS (
          SELECT
            timestamp_ns,
            symbol,
            venue,
            last(bids[1][1]) AS bid_price,
            last(bids[2][1]) AS bid_size,
            last(asks[1][1]) AS ask_price,
            last(asks[2][1]) AS ask_size
          FROM {table_name("equities_market_data", suffix)}
          SAMPLE BY 1s
        ) PARTITION BY HOUR{ttl_md};
        """)
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name("nbbo_1s", suffix)} AS (
          SELECT
            timestamp_ns,
            symbol,
            max(bid_price) AS best_bid,
            min(ask_price) AS best_ask,
            min(ask_price) - max(bid_price) AS spread
          FROM {table_name("top_of_book_1s", suffix)}
          SAMPLE BY 1s
        ) PARTITION BY HOUR{ttl_md};
        """)
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name("trades_ohlcv_1s", suffix)} AS (
          SELECT
            timestamp_ns,
            symbol,
            first(price) AS open,
            max(price)   AS high,
            min(price)   AS low,
            last(price)  AS close,
            sum(size)    AS volume,
            sum(price * size) / nullif(sum(size), 0) AS vwap
          FROM {table_name("equities_trades", suffix)}
          SAMPLE BY 1s
        ) PARTITION BY HOUR{ttl_md};
        """)
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name("nbbo_1m", suffix)}
        REFRESH EVERY 1m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (
          SELECT
            timestamp_ns,
            symbol,
            max(best_bid) AS max_bid,
            min(best_ask) AS min_ask,
            min(best_ask) - max(best_bid) AS min_spread
          FROM {table_name("nbbo_1s", suffix)}
          SAMPLE BY 1m
        ) PARTITION BY DAY{ttl_tr};
        """)
        conn.execute(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name("trades_ohlcv_1m", suffix)}
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
          FROM {table_name("trades_ohlcv_1s", suffix)}
          SAMPLE BY 1m
        ) PARTITION BY DAY{ttl_tr};
        """)

def get_latest_timestamp_ns(conn, table: str):
    cur = conn.execute(
        f"SELECT timestamp_ns FROM {table} ORDER BY timestamp_ns DESC LIMIT 1"
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
            bars = yf.Ticker(sym).history(period="1d", interval="1m")
            if not bars.empty:
                mid = float(bars["Close"].iloc[-1])
                if math.isnan(mid) or mid == 0.0:
                    raise ValueError("Yahoo price NaN or zero")
                low = mid * (1 - frac)
                high = mid * (1 + frac)
            else:
                raise ValueError("Yahoo empty")
        except Exception:
            base = 100.0 + (abs(hash(sym)) % 200)  # 100–300 fallback anchor
            low = base * (1 - frac)
            high = base * (1 + frac)
            print(f"[YF] {sym}: fallback bracket [{low:.2f}, {high:.2f}]", flush=True)
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


# ----------------------------
# State evolution
# ----------------------------

def evolve_mid(prev_mid: float, low: float, high: float, drift_ticks: float = 0.8):
    change = random.uniform(-drift_ticks * TICK, drift_ticks * TICK)
    if random.random() < 0.008:
        change += random.uniform(-12 * TICK, 12 * TICK)
    return quantize(clamp(prev_mid + change, low, high))

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
    sender: Sender,
    ladder: list[int],
    min_levels: int,
    max_levels: int,
    md_events: int,
    tr_events: int,
    suffix: str,
    allow_trades: bool,
):
    prebuilt_bids = [np.zeros((2, lvl), dtype=np.float64)
                     for lvl in range(1, max_levels + 1)]
    prebuilt_asks = [np.zeros((2, lvl), dtype=np.float64)
                     for lvl in range(1, max_levels + 1)]

    # L2 snapshots
    md_targets = random.choices([(s, v) for s in symbols for v in venues], k=md_events)
    md_offsets = sorted(random.randint(0, 999_999_999) for _ in range(md_events))
    for idx, (sym, ven) in enumerate(md_targets):
        ofs = md_offsets[idx]
        levels = random.randint(min_levels, max_levels)
        bids = prebuilt_bids[levels - 1]
        asks = prebuilt_asks[levels - 1]
        ob = open_state[sym]
        cb = close_state[sym]
        best_bid = quantize(ob["bid"] + (cb["bid"] - ob["bid"]) * 0.5)
        best_ask = quantize(ob["ask"] + (cb["ask"] - ob["ask"]) * 0.5)
        generate_l2_for_symbol(sym, ven, best_bid, best_ask, levels, ladder, bids, asks)
        sender.row(
            table_name("equities_market_data", suffix),
            symbols={"symbol": sym, "venue": ven},
            columns={"bids": bids, "asks": asks},
            at=TimestampNanos(ts_ns_base + ofs),
        )

    # Trades
    if not allow_trades or tr_events <= 0:
        return

    tr_targets = random.choices(symbols, k=tr_events)
    tr_offsets = sorted(random.randint(0, 999_999_999) for _ in range(tr_events))
    for idx, sym in enumerate(tr_targets):
        ofs = tr_offsets[idx]
        ob = open_state[sym]
        cb = close_state[sym]
        best_bid = ob["bid"] + (cb["bid"] - ob["bid"]) * 0.5
        best_ask = ob["ask"] + (cb["ask"] - ob["ask"]) * 0.5
        mid = (best_bid + best_ask) / 2.0
        slip = random.uniform(-0.15 * TICK, 0.15 * TICK)
        price = quantize(clamp(mid + slip, best_bid, best_ask))
        side = "B" if random.random() < 0.5 else "S"
        venue = random.choice(venues)
        r = random.random()
        if r < 0.50:
            size = random.randint(1, 99)
        elif r < 0.93:
            size = random.choice([100, 200, 300, 400, 500, 600, 800, 1000])
        else:
            size = random.randint(1000, 10000)
        sender.row(
            table_name("equities_trades", suffix),
            symbols={"symbol": sym, "venue": venue, "side": side, "cond": "T"},
            columns={"price": float(price), "size": int(size)},
            at=TimestampNanos(ts_ns_base + ofs),
        )


def evolve_open_close_for_second(symbols, brackets, prev_state):
    open_state = {}
    close_state = {}
    for sym in symbols:
        low, high = brackets[sym]
        prev_mid = (prev_state[sym]["bid"] + prev_state[sym]["ask"]) / 2.0
        new_mid = evolve_mid(prev_mid, low, high)
        spread = clamp(prev_state[sym]["spread"] +
                       random.uniform(-0.5 * TICK, 0.5 * TICK),
                       TICK, 0.10)
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

def wal_monitor(args, pause_event, processes, interval=5, suffix=""):
    conn_str = (
        f"user={args.user} password={args.password} host={args.host} "
        f"port={args.pg_port} dbname=qdb"
    )
    threshold = 3 * processes
    last_logged_paused = False
    tbl = table_name("equities_market_data", suffix)
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
):
    ladder = make_ladder(args.max_levels)
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

    auto_flush = 1000 if args.mode == "real-time" else 10000
    if args.protocol == "http":
        conf = (
            f"http::addr={args.host}:9000;auto_flush_interval={auto_flush};"
            if not args.token else
            f"https::addr={args.host}:9000;token={args.token};tls_verify=unsafe_off;"
            f"auto_flush_interval={auto_flush};"
        )
    else:
        conf = (
            f"tcp::addr={args.host}:9009;protocol_version=2;auto_flush_interval={auto_flush};"
            if not args.token else
            f"tcps::addr={args.host}:9009;username={args.ilp_user};token={args.token};"
            f"token_x={args.token_x};token_y={args.token_y};tls_verify=unsafe_off;"
            f"protocol_version=2;auto_flush_interval={auto_flush};"
        )

    with Sender.from_conf(conf) as sender:
        ts = start_ns
        sec_idx = 0
        wall_start = time.time() if args.mode == "real-time" else None
        while True:
            if end_ns is not None and ts >= end_ns:
                break
            if args.mode == "faster-than-life" and per_second_plan is not None:
                if sec_idx >= len(per_second_plan):
                    break
                md_events, tr_events = per_second_plan[sec_idx]
            else:
                md_events = random.randint(args.market_data_min_eps, args.market_data_max_eps)
                tr_events = random.randint(args.trades_min_eps, args.trades_max_eps)

            phase = session_phase(ts, args.session_tz) if args.session_pacing else "continuous"
            allow_trades = True
            md_scale = 1.0
            tr_scale = 1.0
            if phase == "pre":
                allow_trades = False
                md_scale = 0.4
            elif phase == "continuous":
                allow_trades = True
                md_scale = 1.0
                tr_scale = 1.0
            elif phase == "close_auction":
                allow_trades = True
                md_scale = 1.3
                tr_scale = 1.8
            else:
                allow_trades = False
                md_scale = 0.2

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
            open_state, close_state, init_state = evolve_open_close_for_second(
                symbols, brackets, init_state
            )

            generate_second(
                ts_ns_base=ts,
                symbols=symbols,
                venues=venues,
                open_state=open_state,
                close_state=close_state,
                sender=sender,
                ladder=ladder,
                min_levels=args.min_levels,
                max_levels=args.max_levels,
                md_events=md_events,
                tr_events=tr_events,
                suffix=args.suffix,
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
    p.add_argument("--suffix", type=str, default="")
    p.add_argument("--yahoo_refresh_secs", type=int, default=300)

    p.add_argument("--session_pacing", type=lambda x: str(x).lower() != "false", default=True)
    p.add_argument("--offsession_trades", choices=["none", "trickle", "full"], default="none")
    p.add_argument("--session_tz", type=str, default="America/New_York")

    args = p.parse_args()
    suffix = args.suffix

    if args.min_levels > args.max_levels:
        print("ERROR: min_levels cannot be greater than max_levels.")
        sys.exit(1)

    # Ensure base objects
    ensure_tables_and_views(args, suffix)

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
        latest_md = get_latest_timestamp_ns(conn, table_name("equities_market_data", suffix))
        latest_tr = get_latest_timestamp_ns(conn, table_name("equities_trades", suffix))
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
        kwargs={"suffix": suffix}
    )
    wal_proc.start()

    if args.mode == "faster-than-life":
        # Build per-second plan for requested MD events
        per_second_plan = []
        md_so_far = 0
        while md_so_far < args.total_market_data_events:
            md_total = random.randint(args.market_data_min_eps, args.market_data_max_eps)
            tr_total = random.randint(args.trades_min_eps, args.trades_max_eps)
            per_second_plan.append((md_total, tr_total))
            md_so_far += md_total
        over = md_so_far - args.total_market_data_events
        if over > 0 and per_second_plan:
            md_last, tr_last = per_second_plan[-1]
            per_second_plan[-1] = (max(0, md_last - over), tr_last)

        # Split seconds across workers
        worker_plans = [[] for _ in range(args.processes)]
        for md_total, tr_total in per_second_plan:
            splits_md = split_event_counts(md_total, args.processes)
            splits_tr = split_event_counts(tr_total, args.processes)
            for i in range(args.processes):
                worker_plans[i].append((splits_md[i], splits_tr[i]))

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
            ),
        )
        w.start()
        w.join()

    wal_proc.terminate()
    wal_proc.join()
    print("[INFO] Completed.")

if __name__ == "__main__":
    main()
