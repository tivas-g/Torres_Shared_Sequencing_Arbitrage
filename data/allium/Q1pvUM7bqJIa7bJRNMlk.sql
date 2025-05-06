select
  timestamp, price, median_price, median_safe_price
from
  {{chain}}.dex.token_prices_hourly
where
  address = '{{address}}' and 
  timestamp >= '{{block_timestamp_start}}' and
  timestamp <= '{{block_timestamp_end}}'
order by
  timestamp;