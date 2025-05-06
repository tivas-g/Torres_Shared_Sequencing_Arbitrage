select
  distinct transaction_hash, protocol, transaction_fees, transaction_fees_usd, block_timestamp
from
  {{chain}}.dex.trades
where
  project = 'uniswap' and
  (protocol = 'uniswap_v2' or protocol = 'uniswap_v3') and
  swap_count = {{swap_count}} and
  block_timestamp >= '{{block_timestamp_start}}' and
  block_timestamp <= '{{block_timestamp_end}}'
order by
  block_timestamp;