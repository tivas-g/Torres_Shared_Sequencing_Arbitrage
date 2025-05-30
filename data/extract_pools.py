#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import csv
import json

# INPUT
TOP_POOLS               = "top_pools_over_100m.csv"
RESERVE_TOKEN_SYMBOLS   = ["WETH", "USDC", "USDT", "WBTC", "DAI"]

# OUTPUT
POOLS                   = "pools.json"
RESERVE_TOKEN_ADDRESSES = "reserve_token_addresses.json"

class colors:
    INFO = '\033[94m'
    OK = '\033[92m'
    FAIL = '\033[91m'
    END = '\033[0m'


def main():
    if not os.path.exists(POOLS):
        print(colors.FAIL+"Error: Please run 'data/download_top_pools.py' first to create the '"+TOP_POOLS+"' file first!"+colors.END)
        sys.exit(-1)

    duplicates = dict()
    with open(TOP_POOLS, "r") as file:
        csvFile = csv.reader(file)
        next(csvFile)
        for row in csvFile:
            if not row[0]+row[3] in duplicates:
                duplicates[row[0]+row[3]] = 0
            duplicates[row[0]+row[3]] += 1

    pools = list()
    reserve_token_addresses = dict()
    with open(TOP_POOLS, "r") as file:
        csvFile = csv.reader(file)
        next(csvFile)
        for row in csvFile:
            if row[1] == "uniswap" and row[5]:
                if row[13] in RESERVE_TOKEN_SYMBOLS:
                    if not row[13] in reserve_token_addresses:
                        reserve_token_addresses[row[13]] = dict()
                    if not row[0] in reserve_token_addresses[row[13]]:
                        reserve_token_addresses[row[13]][row[0]] = row[11]
                if row[14] in RESERVE_TOKEN_SYMBOLS:
                    if not row[14] in reserve_token_addresses:
                        reserve_token_addresses[row[14]] = dict()
                    if not row[0] in reserve_token_addresses[row[14]]:
                        reserve_token_addresses[row[14]][row[0]] = row[12]
                print(row[0].ljust(10), row[1], "\t", row[4], "\t", row[5], "\t", row[3], "\t", row[13].ljust(12), row[14])
                if duplicates[row[0]+row[3]] == 1:
                    pools.append({
                        "protocol": row[1],
                        "version": row[4],
                        "chain": row[0],
                        "address": row[3],
                        "token0": {
                            "address": row[11],
                            "symbol": row[13],
                            "decimals": row[15]
                        },
                        "token1": {
                            "address": row[12],
                            "symbol": row[14],
                            "decimals": row[16]
                        },
                        "feeTier": row[5]
                    })
    print("Number of pools:", len(pools))
    print()
    with open(POOLS, "w") as f:
        json.dump(pools, f, indent=4)
    
    irrelevant_tokens = list()
    for token in reserve_token_addresses:
        if len(reserve_token_addresses[token]) == 1:
            irrelevant_tokens.append(token)
    for token in irrelevant_tokens:
        del reserve_token_addresses[token]
    for token in reserve_token_addresses:
        for chain in reserve_token_addresses[token]:
            print(token, "\t", chain.ljust(10), "\t", reserve_token_addresses[token][chain])
    with open(RESERVE_TOKEN_ADDRESSES, "w") as f:
        json.dump(reserve_token_addresses, f, indent=4)


if __name__ == "__main__":
    main()
