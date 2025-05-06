#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import requests
import pandas as pd

# INPUT
STARTING_DATE = "2024-11-01"
ENDING_DATE   = "2024-11-01"
CHAINS        = ["arbitrum", "optimism", "base"] 
PATHS         = ["cross_chain_paths.json", "single_chain_paths.json"]

# OUTPUT
OUTPUT_FOLDER = "uniswap_v3"

class colors:
    INFO = '\033[94m'
    OK = '\033[92m'
    FAIL = '\033[91m'
    END = '\033[0m'


def main():
    if len(sys.argv) < 2:
        print(colors.FAIL+"Error: Please provide an Allium API key to download Uniswap V3 events: 'python3 "+sys.argv[0]+" <ALLIUM_API_KEY>'"+colors.END)
        sys.exit(-1)

    allium_api_key = sys.argv[1]

    if not any([os.path.exists(path) for path in PATHS]):
        print(colors.FAIL+"Error: Please run 'simulation/path_builder.py' first to create any of the '"+", ".join(PATHS)+"' files first!"+colors.END)
        sys.exit(-2)

    pools = dict()

    for chain in CHAINS:
        pools[chain] = set()
    
    for path in PATHS:
        with open(path, "r") as f:
            paths = json.load(f)
            for path in paths:
                for route in path:
                    if route["chain"] in CHAINS and route["protocol"] == "uniswap" and route["version"] == "v3":
                        pools[route["chain"]].add(route["address"].lower())

    for chain in CHAINS:
        if not os.path.exists(OUTPUT_FOLDER+"/"+chain+"/"):
            os.makedirs(OUTPUT_FOLDER+"/"+chain+"/")
        
        period_range = pd.period_range(start=STARTING_DATE, end=ENDING_DATE, freq="D")
        dates = tuple([(period.year, period.month, period.day) for period in period_range])
        
        for date in dates:
            start_date = str(date[0])+"-"+"{:02d}".format(date[1])+"-"+"{:02d}".format(date[2])+"T00:00:00"
            end_date = str(date[0])+"-"+"{:02d}".format(date[1])+"-"+"{:02d}".format(date[2])+"T23:59:59"
        
            if len(pools[chain]) == 0:
                events = list()
                with open(OUTPUT_FOLDER+"/"+chain+"/"+chain+"_uniswap_v3_events_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json", "w") as f:
                    json.dump(events, f, indent=4)
                continue

            if not os.path.exists(OUTPUT_FOLDER+"/"+chain+"/"+chain+"_uniswap_v3_events_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json"):

                print("Downloading "+chain.capitalize()+" Uniswap V3 events from", start_date, "to", end_date)
                download_start = time.time()

                # Create a query run
                response = requests.post(
                    "https://api.allium.so/api/v1/explorer/queries/uQXoHlF8G2TfJRT2Byk6/run-async",
                    json={"parameters": {"chain": chain, "pools": str(list(pools[chain])), "block_timestamp_start": start_date, "block_timestamp_end": end_date}, "run_config": {"limit": 250000}},
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

                events = list()
                for event in response.json()["data"]:
                    events.append(event)

                with open(OUTPUT_FOLDER+"/"+chain+"/"+chain+"_uniswap_v3_events_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json", "w") as f:
                    json.dump(events, f, indent=4)
                print("Saved", len(events), "events.")


if __name__ == "__main__":
    main()
