select
  block_number, transaction_index, log_index, block_timestamp as block_date, liquidity_pool_address, event, liquidity, sqrt_price_x96 as sqrt_price, tick, tick_lower, tick_upper
from
  {{chain}}.dex.uniswap_v3_events
where
  event in ('swap', 'mint', 'burn')  and
  liquidity_pool_address in {{pools}} and
  block_timestamp >= '{{block_timestamp_start}}' and
  block_timestamp <= '{{block_timestamp_end}}' 
order by block_number, transaction_index, log_index;
