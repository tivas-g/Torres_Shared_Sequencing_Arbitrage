#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import requests
import pandas as pd

STARTING_DATE = "2024-11-01"
ENDING_DATE   = "2024-11-01"
CHAINS        = ["arbitrum", "optimism", "base"] 
PATHS         = "../simulation/paths.json"
API_KEY       = "W8ax4JoLX4xa9pX1Qow-uMrPGnImFEHo-YjKM4ixU2A4b7WhxDuGVishO0djeMKb6Bt-5ouZBn8aTJqDGQF43w"

def main():
    pools = dict()

    for chain in CHAINS:
        pools[chain] = set()
    
    with open(PATHS, "r") as f:
        paths = json.load(f)
        for path in paths:
            for route in path:
                if route["chain"] in CHAINS and route["protocol"] == "uniswap" and route["version"] == "v3":
                    pools[route["chain"]].add(route["address"].lower())

    for chain in CHAINS:
        if not os.path.exists("uniswap_v3/"+chain+"/"):
            os.makedirs("uniswap_v3/"+chain+"/")
        
        period_range = pd.period_range(start=STARTING_DATE, end=ENDING_DATE, freq="D")
        dates = tuple([(period.year, period.month, period.day) for period in period_range])
        
        for date in dates:
            start_date = str(date[0])+"-"+"{:02d}".format(date[1])+"-"+"{:02d}".format(date[2])+"T00:00:00"
            end_date = str(date[0])+"-"+"{:02d}".format(date[1])+"-"+"{:02d}".format(date[2])+"T23:59:59"
        
            if len(pools[chain]) == 0:
                events = list()
                with open("uniswap_v3/"+chain+"/"+chain+"_uniswap_v3_events_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json", "w") as f:
                    json.dump(events, f, indent=4)
                continue

            if not os.path.exists("uniswap_v3/"+chain+"/"+chain+"_uniswap_v3_events_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json"):

                print("Downloading "+chain.capitalize()+" Uniswap V3 events from", start_date, "to", end_date)
                download_start = time.time()

                # Create a query run
                response = requests.post(
                    "https://api.allium.so/api/v1/explorer/queries/uQXoHlF8G2TfJRT2Byk6/run-async",
                    json={"parameters": {"chain": chain, "pools": str(list(pools[chain])), "block_timestamp_start": start_date, "block_timestamp_end": end_date}, "run_config": {"limit": 250000}},
                    headers={"X-API-Key": API_KEY}
                )
                query_run_id = response.json()["run_id"]
                
                # Poll for query run status
                while True:
                    response = requests.get(
                        "https://api.allium.so/api/v1/explorer/query-runs/"+query_run_id,
                        headers={"X-API-Key": API_KEY}
                    )
                    if not response.json()["status"] in ["queued", "running"]:
                        break
                    time.sleep(2)

                # Fetch results
                response = requests.post(
                    "https://api.allium.so/api/v1/explorer/query-runs/"+query_run_id+"/results",
                    headers={
                        "X-API-Key": API_KEY,
                        "Content-Type": "application/json"
                    },
                    json={"config": {}}
                )

                download_stop = time.time()                
                print("Download took:", download_stop - download_start, "second(s).")

                events = list()
                for event in response.json()["data"]:
                    events.append(event)

                with open("uniswap_v3/"+chain+"/"+chain+"_uniswap_v3_events_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json", "w") as f:
                    json.dump(events, f, indent=4)
                print("Saved", len(events), "events.")


if __name__ == "__main__":
    main()
