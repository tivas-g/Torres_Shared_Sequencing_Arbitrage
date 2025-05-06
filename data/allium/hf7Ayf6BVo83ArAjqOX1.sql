select
  token0_amount_raw_str as reserve0, token1_amount_raw_str as reserve1
from
  {{chain}}.dex.uniswap_v2_events
where
  event = 'sync' and
  liquidity_pool_address = '{{pool}}' and
  block_timestamp < '{{block_timestamp}}'
order by 
  block_timestamp desc, block_number desc, transaction_index desc, log_index desc
limit 
  1;