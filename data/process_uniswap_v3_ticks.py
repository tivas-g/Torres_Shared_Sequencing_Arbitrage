#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import pymongo
import requests
import pandas as pd

from hashlib import sha256

# INPUT
STARTING_DATE = "2024-11-01"
ENDING_DATE   = "2024-11-01"
CHAINS        = ["arbitrum", "optimism", "base"] 
INPUT_FOLDER  = "uniswap_v3"

# OUTPUT
MONGO_HOST    = "localhost"
MONGO_PORT    = 27017

class colors:
    INFO = '\033[94m'
    OK = '\033[92m'
    FAIL = '\033[91m'
    END = '\033[0m'


def main():
    if len(sys.argv) < 2:
        print(colors.FAIL+"Error: Please provide an Allium API key to download Uniswap V3 ticks: 'python3 "+sys.argv[0]+" <ALLIUM_API_KEY>'"+colors.END)
        sys.exit(-1)

    allium_api_key = sys.argv[1]
    
    processing_start = time.time()
    
    mongo_connection = pymongo.MongoClient("mongodb://"+MONGO_HOST+":"+str(MONGO_PORT), maxPoolSize=None)
    collection = mongo_connection["cross_chain_arbitrage"]["dex_updates"]
                    
    for chain in CHAINS:
        period_range = pd.period_range(start=STARTING_DATE,end=ENDING_DATE, freq='D')
        dates = tuple([(period.year, period.month, period.day) for period in period_range])

        pools = list(collection.distinct("pool", {"chain": chain, "tick": {"$exists": True}}))

        for pool in pools:
            if not os.path.exists(INPUT_FOLDER+"/"+chain+"/"+chain+"_uniswap_v3_ticks_"+pool+".json"):
                print("Downloading "+chain.capitalize()+" Uniswap V3 ticks for pool", pool, "until", STARTING_DATE)
                download_start = time.time()

                # Create a query run
                response = requests.post(
                    "https://api.allium.so/api/v1/explorer/queries/Gpm54k46AGjtegifodfr/run-async",
                    json={"parameters": {"chain": chain, "pool": pool.lower(), "block_timestamp": STARTING_DATE+"T00:00:00"}, "run_config": {"limit": 250000}},
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

                ticks = dict()
                for event in response.json()["data"]:
                    tick_lower = int(float(event["tick_lower"]))
                    tick_upper = int(float(event["tick_upper"]))
                    if not tick_lower in ticks:
                        ticks[tick_lower] = 0
                    if not tick_upper in ticks:
                        ticks[tick_upper] = 0
                    if   event["event"] == "mint":
                        ticks[tick_lower] += int(event["liquidity"])
                        ticks[tick_upper] -= int(event["liquidity"])
                    elif event["event"] == "burn":
                        ticks[tick_lower] -= int(event["liquidity"])
                        ticks[tick_upper] += int(event["liquidity"])
                
                with open(INPUT_FOLDER+"/"+chain+"/"+chain+"_uniswap_v3_ticks_"+pool+".json", "w") as f:
                    json.dump({k: ticks[k] for k in sorted(ticks)}, f, indent=4)
                print("Saved", len(ticks), "ticks.")

        ticks = dict()
        
        for date in dates:

            if not os.path.exists(INPUT_FOLDER+"/"+chain+"/"+chain+"_uniswap_v3_events_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json"):
                print(colors.FAIL+"Error file '"+INPUT_FOLDER+"/"+chain+"/"+chain+"_uniswap_v3_events_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json' is missing!"+colors.END)
                sys.exit(-2)
            
            print("Searching for tick updates within:", INPUT_FOLDER+"/"+chain+"/"+chain+"_uniswap_v3_events_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json")
            with open(INPUT_FOLDER+"/"+chain+"/"+chain+"_uniswap_v3_events_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json", 'r') as f:
                events = json.load(f)
                
                ordered_events = dict()
                for event in events:
                    if event["event"] == "swap" or event["event"] == "mint" or event["event"] == "burn":
                        if not event["block_number"] in ordered_events:
                            ordered_events[event["block_number"]] = dict()
                        if not event["transaction_index"] in ordered_events[event["block_number"]]:
                            ordered_events[event["block_number"]][event["transaction_index"]] = list()
                        ordered_events[event["block_number"]][event["transaction_index"]].append(event)        
        
                for block_number in ordered_events:
                    for transaction_index in ordered_events[block_number]:
                        ordered_events[block_number][transaction_index] = sorted(ordered_events[block_number][transaction_index], key=lambda event: event["log_index"])
                
                ordered_block_numbers = sorted(list(ordered_events.keys()))
                for block_number in ordered_block_numbers:
                    ordered_transaction_indexes = sorted(list(ordered_events[block_number].keys()))
                    for transaction_index in ordered_transaction_indexes:
                        for event in ordered_events[block_number][transaction_index]:

                            if not event["liquidity_pool_address"] in ticks:
                                with open(INPUT_FOLDER+"/"+chain+"/"+chain+"_uniswap_v3_ticks_"+event["liquidity_pool_address"]+".json", "r") as f:
                                    ticks[event["liquidity_pool_address"]] = json.load(f)
                                    for tick in ticks[event["liquidity_pool_address"]]:
                                        ticks[event["liquidity_pool_address"]][tick] = str(ticks[event["liquidity_pool_address"]][tick])

                            if   event["event"] == "mint":
                                tick_lower = str(int(float(event["tick_lower"])))
                                if not tick_lower in ticks[event["liquidity_pool_address"]]:
                                    ticks[event["liquidity_pool_address"]][tick_lower] = "0"
                                ticks[event["liquidity_pool_address"]][tick_lower] = str(int(ticks[event["liquidity_pool_address"]][tick_lower]) + int(event["liquidity"]))

                                tick_upper = str(int(float(event["tick_upper"])))
                                if not tick_upper in ticks[event["liquidity_pool_address"]]:
                                    ticks[event["liquidity_pool_address"]][tick_upper] = "0"
                                ticks[event["liquidity_pool_address"]][tick_upper] = str(int(ticks[event["liquidity_pool_address"]][tick_upper]) - int(event["liquidity"]))

                            elif event["event"] == "burn":
                                tick_lower = str(int(float(event["tick_lower"])))
                                if not tick_lower in ticks[event["liquidity_pool_address"]]:
                                    ticks[event["liquidity_pool_address"]][tick_lower] = "0"
                                ticks[event["liquidity_pool_address"]][tick_lower] = str(int(ticks[event["liquidity_pool_address"]][tick_lower]) - int(event["liquidity"]))

                                tick_upper = str(int(float(event["tick_upper"])))
                                if not tick_upper in ticks[event["liquidity_pool_address"]]:
                                    ticks[event["liquidity_pool_address"]][tick_upper] = "0"
                                ticks[event["liquidity_pool_address"]][tick_upper] = str(int(ticks[event["liquidity_pool_address"]][tick_upper]) + int(event["liquidity"]))

                            id = sha256(str(event["liquidity_pool_address"]+":"+chain+":"+str(block_number)).encode('utf-8')).hexdigest()
                            query = {"id": id}
                            values = {"$set": {"ticks": ticks[event["liquidity_pool_address"]]}}
                            print("Updating ticks of DEX update ID", id)
                            update_start = time.time()
                            collection.update_one(query, values)
                            update_stop = time.time()
                            print("Update took:", update_stop - update_start, "second(s).")

    processing_stop = time.time()
    print("Total processing took:", processing_stop - processing_start, "second(s).")
                   
                   
if __name__ == "__main__":
    main()
