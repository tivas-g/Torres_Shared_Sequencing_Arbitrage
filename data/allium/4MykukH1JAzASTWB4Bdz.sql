select
  block_number, transaction_index, log_index, block_timestamp as block_date, liquidity_pool_address, token0_amount_raw_str as reserve0, token1_amount_raw_str as reserve1
from
  {{chain}}.dex.uniswap_v2_events
where
  event = 'sync' and
  liquidity_pool_address in {{pools}} and
  block_timestamp >= '{{block_timestamp_start}}' and
  block_timestamp <= '{{block_timestamp_end}}'
order by block_number, transaction_index, log_index;