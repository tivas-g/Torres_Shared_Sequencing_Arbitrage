#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import requests
import pandas as pd

# INPUT
STARTING_DATE  = "2024-11-01"
ENDING_DATE    = "2024-11-01"
RESERVE_TOKENS = "reserve_token_addresses.json"

# OUTPUT
OUTPUT_FOLDER  = "prices"

class colors:
    INFO = '\033[94m'
    OK = '\033[92m'
    FAIL = '\033[91m'
    END = '\033[0m'


def main():
    if len(sys.argv) < 2:
        print(colors.FAIL+"Error: Please provide an Allium API key to download token prices: 'python3 "+sys.argv[0]+" <ALLIUM_API_KEY>'"+colors.END)
        sys.exit(-1)

    allium_api_key = sys.argv[1]

    if not os.path.exists(RESERVE_TOKENS):
        print(colors.FAIL+"Error: Please run 'extract_pools.py' first to create the '"+RESERVE_TOKENS+"' file first!"+colors.END)
        sys.exit(-2)

    reserve_token_addresses = dict()
    with open(RESERVE_TOKENS, "r") as f:
        reserve_token_addresses = json.load(f)

    for symbol in reserve_token_addresses:
        for chain in reserve_token_addresses[symbol]:
            address = reserve_token_addresses[symbol][chain]

            if not os.path.exists(OUTPUT_FOLDER+"/"+chain+"/"):
                os.makedirs(OUTPUT_FOLDER+"/"+chain+"/")
        
            period_range = pd.period_range(start=STARTING_DATE, end=ENDING_DATE, freq="D")
            dates = tuple([(period.year, period.month, period.day) for period in period_range])
            
            for date in dates:
                start_date = str(date[0])+"-"+"{:02d}".format(date[1])+"-"+"{:02d}".format(date[2])+"T00:00:00"
                end_date = str(date[0])+"-"+"{:02d}".format(date[1])+"-"+"{:02d}".format(date[2])+"T23:59:59"

                if not os.path.exists(OUTPUT_FOLDER+"/"+chain+"/"+chain+"_"+address.lower()+"_prices_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json"):

                    print("Downloading "+symbol+" prices on "+chain.capitalize()+" for token address "+address+" from", start_date, "to", end_date)
                    download_start = time.time()

                    # Create a query run
                    response = requests.post(
                        "https://api.allium.so/api/v1/explorer/queries/Q1pvUM7bqJIa7bJRNMlk/run-async",
                        json={"parameters": {"chain": chain, "address": address.lower(), "block_timestamp_start": start_date, "block_timestamp_end": end_date}, "run_config": {"limit": 250000}},
                        headers={"X-API-Key": allium_api_key}
                    )
                    query_run_id = response.json()["run_id"]
                    
                    # Poll for query run status
                    while True:
                        response = requests.get(
                            "https://api.allium.so/api/v1/explorer/query-runs/"+query_run_id,
                            headers={"X-API-Key": allium_api_key}
                        )
                        if not response.json()["status"] in ["queued", "running"]:
                            break
                        time.sleep(2)

                    # Fetch results
                    response = requests.post(
                        "https://api.allium.so/api/v1/explorer/query-runs/"+query_run_id+"/results",
                        headers={
                            "X-API-Key": allium_api_key,
                            "Content-Type": "application/json"
                        },
                        json={"config": {}}
                    )

                    download_stop = time.time()                
                    print("Download took:", download_stop - download_start, "second(s).")

                    prices = list()
                    for price in response.json()["data"]:
                        prices.append(price)

                    with open(OUTPUT_FOLDER+"/"+chain+"/"+chain+"_"+address.lower()+"_prices_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json", "w") as f:
                        json.dump(prices, f, indent=4)
                    print("Saved", len(prices), "price updates.")


if __name__ == "__main__":
    main()
