-- Equities Demo SQL Script
-- Intro

show tables;

select * from eq_equities_market_data;

select * from eq_equities_trades;

select * from eq_equities_market_data where timestamp in today();

select * from eq_equities_market_data where timestamp in yesterday();

select * from eq_equities_trades where timestamp in today();


-- Array Basics

select timestamp, symbol, venue,
       bids[1][1] as bid_price,
       bids[2][1] as bid_size,
       asks[1][1] as ask_price,
       asks[2][1] as ask_size
from eq_equities_market_data
where symbol='AAPL' and timestamp in today()
limit -20;

select timestamp, symbol, venue,
       bids[1][1] as bid_price,
       bids[2][1] as bid_size,
       bids[1][3] as bid_priceL4,
       bids[2][3] as bid_sizeL4
from eq_equities_market_data
where symbol='AAPL' and timestamp in today()
limit -20;

-- Latest on

select * from eq_equities_market_data latest by symbol;
select * from eq_equities_market_data latest by symbol, venue;
select * from eq_equities_market_data latest by symbol, venue
where timestamp <= '2025-11-25T00:00';

select * from eq_nbbo_1s latest by symbol;

-- sample by

select timestamp, symbol, count(bids[1][1])
from eq_equities_market_data
where timestamp in today()
and symbol = 'GOOGL'
sample by 10s;

-- partitioning and parquet

table_partitions('eq_equities_market_data');
table_partitions('eq_equities_trades');

select timestamp, count(), symbol,
    avg(price) as price, avg(size) as size
from eq_equities_trades
where symbol='AAPL'
 sample by 1d;

with parts as (
    select name, last(isParquet)
    from table_partitions('eq_equities_trades')
), totals as (
    select timestamp, count(), avg(price) as avg_price
    from eq_equities_trades
    where symbol='AAPL'
    sample by 1h
)
select * from totals join parts
on to_str(timestamp, 'yyyy-MM-ddTHH') = parts.name;

read_parquet('trades.parquet');

select timestamp, count() from (select * from (read_parquet('trades.parquet') order by timestamp) timestamp(timestamp) )
sample by 1d;

-- Materialized views

SELECT      timestamp,
            symbol,
            venue,
            last(bids[1][1]) AS bid_price,
            last(bids[2][1]) AS bid_size,
            last(asks[1][1]) AS ask_price,
            last(asks[2][1]) AS ask_size
          FROM eq_equities_market_data
          where symbol='AAPL' and timestamp in today()
sample by 1s
order by timestamp desc;

CREATE MATERIALIZED VIEW 'eq_top_of_book_1s' WITH BASE 'eq_equities_market_data' REFRESH IMMEDIATE AS (

          SELECT
            timestamp,
            symbol,
            venue,
            last(bids[1][1]) AS bid_price,
            last(bids[2][1]) AS bid_size,
            last(asks[1][1]) AS ask_price,
            last(asks[2][1]) AS ask_size
          FROM eq_equities_market_data
          SAMPLE BY 1s

) PARTITION BY HOUR
OWNED BY 'admin';

SELECT * FROM eq_top_of_book_1s where symbol='AAPL' and timestamp in today();

select timestamp,
       first(bids[1][1]) as open,
       max(bids[1][1])   as high,
       min(bids[1][1])   as low,
       last(bids[1][1])  as close,
       sum(bids[2][1])   as volume
from eq_equities_market_data
where symbol='AAPL' and timestamp in today()
sample by 15m
order by timestamp desc;

CREATE MATERIALIZED VIEW 'eq_top_of_book_1s' WITH BASE 'eq_equities_market_data' REFRESH IMMEDIATE AS (

          SELECT
            timestamp,
            symbol,
            venue,
            last(bids[1][1]) AS bid_price,
            last(bids[2][1]) AS bid_size,
            last(asks[1][1]) AS ask_price,
            last(asks[2][1]) AS ask_size
          FROM eq_equities_market_data
          SAMPLE BY 1s

) PARTITION BY HOUR
OWNED BY 'admin';

select timestamp,
       first(price) as open,
       max(price)   as high,
       min(price)   as low,
       last(price)  as close,
       sum(size)   as volume
from eq_equities_trades
where symbol='AAPL' and timestamp in today()
sample by 15m
order by timestamp desc;


CREATE MATERIALIZED VIEW 'eq_trades_ohlcv_1s' WITH BASE 'eq_equities_trades' REFRESH IMMEDIATE AS (

          SELECT
            timestamp,
            symbol,
            first(price) AS open,
            max(price)   AS high,
            min(price)   AS low,
            last(price)  AS close,
            sum(size)    AS volume
          FROM eq_equities_trades
          SAMPLE BY 1s

) PARTITION BY HOUR
OWNED BY 'admin';

select * from eq_trades_ohlcv_1s where symbol='AAPL' and timestamp in today();

select * from eq_trades_ohlcv_1s where timestamp in today() and timestamp < now()
order by timestamp desc, symbol asc
limit 50;

CREATE MATERIALIZED VIEW 'eq_trades_ohlcv_1s' WITH BASE 'eq_equities_trades' REFRESH IMMEDIATE AS (

          SELECT
            timestamp,
            symbol,
            first(price) AS open,
            max(price)   AS high,
            min(price)   AS low,
            last(price)  AS close,
            sum(size)    AS volume
          FROM eq_equities_trades
          SAMPLE BY 1s

) PARTITION BY HOUR
OWNED BY 'admin';

CREATE MATERIALIZED VIEW 'eq_trades_ohlcv_1m' WITH BASE 'eq_trades_ohlcv_1s' REFRESH EVERY 1m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (

          SELECT
            timestamp,
            symbol,
            first(open)  AS open,
            max(high)    AS high,
            min(low)     AS low,
            last(close)  AS close,
            sum(volume)  AS volume
          FROM eq_trades_ohlcv_1s
          SAMPLE BY 1m

) PARTITION BY HOUR
OWNED BY 'admin';

CREATE MATERIALIZED VIEW 'eq_trades_ohlcv_15m' WITH BASE 'eq_trades_ohlcv_1m' REFRESH EVERY 1m DEFERRED START '2025-06-01T00:00:00.000000Z' AS (

          SELECT
            timestamp,
            symbol,
            first(open)  AS open,
            max(high)    AS high,
            min(low)     AS low,
            last(close)  AS close,
            sum(volume)  AS volume
          FROM eq_trades_ohlcv_1m
          SAMPLE BY 1m

) PARTITION BY DAY
OWNED BY 'admin';

-- Arrays

-- spread
select timestamp, symbol,
       asks[1][1] - bids[1][1] as spread
from eq_equities_market_data
where symbol in ('AAPL','MSFT','NVDA') and timestamp in today();


-- How much volume I can capture at a cheap price because of a relatively flat orderbook
DECLARE
    @prices := asks[1],
    @volumes := asks[2],
    @best_price := @prices[1],
    @multiplier := 1.01,
    @target_price := @multiplier *  @best_price,
    @relevant_volume_levels := @volumes[1:insertion_point(@prices, @target_price)]
SELECT timestamp, asks,
     @relevant_volume_levels as volume_levels,
     array_sum(@relevant_volume_levels) as total_volume
    FROM eq_equities_market_data where timestamp in today() and symbol = 'AAPL' limit -200 ;

-- Equivalent query without declare. Volume is available within 1% of the best price?
SELECT asks,
     asks[2, 1:insertion_point(asks[1], 1.01 * asks[1, 1])] volume_levels,
     array_sum(asks[2, 1:insertion_point(asks[1], 1.01 * asks[1, 1])]) total_volume
    FROM eq_equities_market_data where timestamp in today() and symbol = 'AAPL' limit -100 ;

-- What price level will a buy order for the given volume reach?
WITH
    q1 AS (
    SELECT timestamp, symbol, asks,
        array_cum_sum(asks[2]) cum_volumes
    FROM eq_equities_market_data
    where symbol = 'AAPL' and timestamp in today()),
    q2 AS (
    SELECT timestamp, symbol,
        asks, cum_volumes,
        insertion_point(cum_volumes, 1_000, true) target_level
        FROM q1 )
SELECT timestamp, symbol,
    cum_volumes, target_level, asks[1, target_level] price
FROM q2;

-- ASOF JOIN

select * from eq_equities_trades asof join eq_equities_market_data on (symbol, venue)
where eq_equities_trades.symbol = 'AAPL'  and eq_equities_trades.timestamp in today();

select *
from eq_trades_ohlcv_1s as t
asof join eq_nbbo_1s as n on t.symbol = n.symbol;

with p as (
        select * from eq_equities_trades
        where symbol = 'AAPL'
        and timestamp in today()
        )
select
        insertion_point(bids[2], size) as available_level,
        bids[1][insertion_point(bids[2], size)] as price_at_level,
        bids[2][insertion_point(bids[2], size)] as volume_at_level,
        *
from p asof join eq_equities_market_data on (symbol, venue) TOLERANCE 1s
--where eq_equities_market_data.symbol is not null
;


--- end




select timestamp, symbol,
       count() as venues_reporting,
       avg(asks[1][1] - bids[1][1]) as avg_spread,
       min(asks[1][1] - bids[1][1]) as best_spread,
       max(asks[1][1] - bids[1][1]) as worst_spread
from eq_equities_market_data
where timestamp in today()
sample by 1s;



-- basic anomaly detection. Ask further from average than a given multiple of stddev
DECLARE
    @l1_ask := asks[1,1],
    @low_threshold := 2,
    @medium_threshold := 3.3,
    @high_threshold := 4
WITH s AS (
    SELECT avg(@l1_ask) as avg_ask, stddev(@l1_ask ) as stddev_ask
    FROM eq_equities_market_data
    where symbol IN ('AAPL')
    and timestamp >= dateadd('h', -1, now())
), combined AS (
SELECT timestamp, @l1_ask as l1_ask, avg_ask, stddev_ask, abs(l1_ask - avg_ask) as delta
FROM eq_equities_market_data m CROSS JOIN s
where m.symbol IN ('AAPL')
and m.timestamp >= dateadd('h', -1, now())
)
SELECT timestamp, l1_ask, avg_ask, delta,
    CASE
        WHEN delta >= stddev_ask * @high_threshold THEN 'High'
        WHEN delta >= stddev_ask * @medium_threshold THEN 'Medium'
        WHEN delta >= stddev_ask * @low_threshold THEN 'Low'
        ELSE '-'
    END AS anomaly
 from combined where delta >= stddev_ask * @low_threshold;
