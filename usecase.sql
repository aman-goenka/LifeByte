with all_dates as 
(
  select generate_series(date'2020-06-01', '2030-09-30', '1 day')::date as dt_report
),
cx_date_hash as 
(
 select * from all_dates cross join (select distinct login_hash, server_hash from users where enable = 1) temp_table
),
cx_symbol as 
(
  with temp1 as 
  (
    select * from cx_date_hash cross join (select distinct symbol from trades) temp_trades
   ) 
   select temp1.*, users.currency from temp1 left join users on temp1.login_hash = users.login_hash and temp1.server_hash = users.server_hash
),
main_table as (
 select
    row_number() over (order by dt_report, abc.login_hash, abc.server_hash, abc.symbol) as id,
    date(dt_report) as dt_report,
    cast(abc.login_hash as varchar) as login_hash,
    cast(abc.server_hash as varchar) as server_hash,
    cast(abc.currency as varchar) as currency,
    cast(trades.symbol as varchar) as symbol,
    cast(sum(trades.volume) over (order by date(trades.open_time) rows between 6 preceding and current row) as double precision) as sum_volume_prev_7d,
    cast(sum(trades.volume) over (partition by abc.login_hash, abc.server_hash, abc.symbol order by date(trades.open_time) range between unbounded preceding and current row) as double precision) as sum_volume_prev_all,
    count(*) over (partition by abc.login_hash order by date(trades.open_time) rows between 6 preceding and current row) trade_count,
    sum(case when trades.open_time >= '2020-08-01 00:00:00' and trades.close_time <= '2020-08-31 00:00:00' then trades.volume else 0 end) 
    over (partition by abc.login_hash, abc.server_hash, abc.symbol) as sum_volume_2020_08,
    min((trades.open_time)) over (partition by abc.login_hash, abc.server_hash, abc.symbol) as date_first_trade,
    row_number() over (order by date(trades.open_time), abc.login_hash, abc.server_hash, trades.symbol) as row_number
from cx_symbol abc left join trades on abc.dt_report = date(trades.open_time) and abc.login_hash = trades.login_hash and abc.server_hash = trades.server_hash and abc.symbol = trades.symbol
order by date(trades.open_time) desc
) select 
  id, dt_report, login_hash, server_hash, symbol, currency, sum_volume_prev_7d, sum_volume_prev_all,
  cast(dense_rank() over (partition by login_hash order by sum_volume_prev_7d asc) as int) as rank_volume_symbol_prev_7d,
  cast(dense_rank() over (partition by login_hash order by trade_count asc) as int) as rank_volume_symbol_prev_7d,
  sum_volume_2020_08, date_first_trade, row_number
  from main_table order by id, dt_report;