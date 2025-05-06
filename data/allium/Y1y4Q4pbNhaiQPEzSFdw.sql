select
  event, liquidity, sqrt_price_x96 as sqrt_price, tick, tick_lower, tick_upper
from
  {{chain}}.dex.uniswap_v3_events
where
  event in {{events}} and
  liquidity_pool_address = '{{pool}}' and
  block_timestamp < '{{block_timestamp}}'
order by 
  block_timestamp desc, block_number desc, transaction_index desc, log_index desc
limit 
  {{limit}};