WITH date_series AS (
    SELECT generate_series('2020-06-01'::date, '2020-09-30'::date, interval '1 day')::date AS dt_report
),
trade_data AS (
    SELECT 
        t.login_hash,
        t.server_hash,
        t.symbol,
        t.volume,
        t.close_time,
        u.currency,
        date_trunc('day', t.close_time) as dt_report_trades
    FROM trades t
    JOIN users u ON t.login_hash = u.login_hash AND t.server_hash = u.server_hash
),
date_combinations AS (
    SELECT
        ds.dt_report,
        td.login_hash,
        td.server_hash,
        td.symbol,
        td.currency
    FROM date_series ds
    CROSS JOIN (
        SELECT DISTINCT login_hash, server_hash, symbol, currency FROM trade_data
    ) td
),
volume_aggregates AS (
    SELECT
        dc.dt_report,
        dc.login_hash,
        dc.server_hash,
        dc.symbol,
        dc.currency,
        COALESCE(SUM(td.volume) FILTER (WHERE td.dt_report_trades >= dc.dt_report - interval '6 days'), 0) AS sum_volume_prev_7d,
        COALESCE(SUM(td.volume)) AS sum_volume_prev_all,
        DENSE_RANK() OVER (PARTITION BY dc.login_hash, dc.symbol ORDER BY COALESCE(SUM(td.volume) FILTER (WHERE td.dt_report_trades >= dc.dt_report - interval '6 days' AND td.dt_report_trades <= dc.dt_report), 0) DESC) AS rank_volume_symbol_prev_7d,
        DENSE_RANK() OVER (PARTITION BY dc.login_hash ORDER BY COUNT(*) FILTER (WHERE td.dt_report_trades >= dc.dt_report - interval '6 days' AND td.dt_report_trades <= dc.dt_report) DESC) AS rank_count_prev_7d,
        COALESCE(SUM(td.volume) FILTER (WHERE td.dt_report_trades BETWEEN '2020-08-01' AND '2020-08-31'), 0) AS sum_volume_2020_08,
        MIN(td.close_time) AS date_first_trade
    FROM date_combinations dc
    LEFT JOIN trade_data td ON dc.login_hash = td.login_hash AND dc.server_hash = td.server_hash AND dc.symbol = td.symbol
    GROUP BY dc.dt_report, dc.login_hash, dc.server_hash, dc.symbol, dc.currency
)
SELECT
    ROW_NUMBER() OVER () AS id,
    va.dt_report,
    va.login_hash,
    va.server_hash,
    va.symbol,
    va.currency,
    va.sum_volume_prev_7d,
    va.sum_volume_prev_all,
    va.rank_volume_symbol_prev_7d,
    va.rank_count_prev_7d,
    va.sum_volume_2020_08,
    va.date_first_trade
FROM volume_aggregates va
ORDER BY va.dt_report, va.login_hash, va.server_hash, va.symbol;
