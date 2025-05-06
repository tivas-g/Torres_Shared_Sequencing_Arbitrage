with raw as (
    select blockchain, project, "version", 
            project_contract_address, token_pair, 
            case when token_bought_address < token_sold_address then token_bought_symbol else token_sold_symbol end as token_a_symbol,
            case when token_bought_address < token_sold_address then token_sold_symbol else token_bought_symbol end as token_b_symbol,
            
            case when token_bought_address < token_sold_address then token_bought_address else token_sold_address end as token_a_address,
            case when token_bought_address < token_sold_address then token_sold_address else token_bought_address end as token_b_address,
                        
            CAST(REPLACE(SPLIT_PART(format('%e', max(case when token_bought_address < token_sold_address 
                                                        then (token_bought_amount_raw/token_bought_amount)
                                                        else (token_sold_amount_raw/token_sold_amount) end
                                                    )
                                            ), 'e', 2), '+', '') AS INTEGER) as token_a_decimal,
                        
            CAST(REPLACE(SPLIT_PART(format('%e', max(case when token_bought_address < token_sold_address 
                                                        then (token_sold_amount_raw/token_sold_amount)
                                                        else (token_bought_amount_raw/token_bought_amount) end
                                                    )
                                            ), 'e', 2), '+', '') AS INTEGER) as token_b_decimal,
                        
            sum(amount_usd) as volume, count(*) as tx_count, max(tx_hash) as example_hash
                -- ,
                -- 1.0000* sum(amount_usd) / sum(sum(amount_usd)) over (partition by blockchain order by sum(amount_usd) desc) as vol_pct
    
    from dex.trades
    where block_month between timestamp '2024-01-01' and timestamp '2025-01-01'
            and blockchain in ('base','arbitrum','optimism')
    group by 1, 2, 3, 4, 5, 6,7,8,9  order by sum(amount_usd) desc
), per_chain as (
    select blockchain, sum(volume) as total_vol
    from raw
    group by 1
) select raw.*, 
            volume/total_vol as vol_pct_per_chain, 
            sum(volume/total_vol) over (partition by raw.blockchain order by volume desc) as cum_vol_pct_per_chain
from raw
join per_chain on raw.blockchain = per_chain.blockchain
where volume >=100000000
order by volume desc

