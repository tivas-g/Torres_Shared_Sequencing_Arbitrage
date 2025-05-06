select
  block_number, transaction_index, log_index, event, liquidity, tick_lower, tick_upper
from
  {{chain}}.dex.uniswap_v3_events
where
  event in ('mint', 'burn')  and
  liquidity_pool_address = '{{pool}}' and
  block_timestamp <= '{{block_timestamp}}'
order by block_number, transaction_index, log_index;