#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import pymongo
import pandas as pd

from hashlib import sha256
from datetime import datetime

# INPUT
STARTING_DATE = "2024-11-01"
ENDING_DATE   = "2024-11-01"
CHAINS        = ["arbitrum", "optimism", "base"]
INPUT_FOLDER  = "uniswap_v2"

# OUTPUT
MONGO_HOST    = "localhost"
MONGO_PORT    = 27017

class colors:
    INFO = '\033[94m'
    OK = '\033[92m'
    FAIL = '\033[91m'
    END = '\033[0m'


def main():
    processing_start = time.time()
    
    mongo_connection = pymongo.MongoClient("mongodb://"+MONGO_HOST+":"+str(MONGO_PORT), maxPoolSize=None)
    collection = mongo_connection["cross_chain_arbitrage"]["dex_updates"]
                    
    for chain in CHAINS:
        period_range = pd.period_range(start=STARTING_DATE,end=ENDING_DATE, freq='D')
        dates = tuple([(period.year, period.month, period.day) for period in period_range])
        
        for date in dates:

            if not os.path.exists(INPUT_FOLDER+"/"+chain+"/"+chain+"_uniswap_v2_events_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json"):
                print(colors.FAIL+"Error file '"+INPUT_FOLDER+"/"+chain+"/"+chain+"_uniswap_v2_events_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json' is missing!"+colors.END)
                sys.exit(-1)
            
            print("Searching for reserve updates within:", INPUT_FOLDER+"/"+chain+"/"+chain+"_uniswap_v2_events_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json")
            with open(INPUT_FOLDER+"/"+chain+"/"+chain+"_uniswap_v2_events_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json", 'r') as f:
                events = json.load(f)

                ordered_events = dict()
                for event in events:
                    if not event["block_number"] in ordered_events:
                        ordered_events[event["block_number"]] = dict()
                    if not event["transaction_index"] in ordered_events[event["block_number"]]:
                        ordered_events[event["block_number"]][event["transaction_index"]] = list()
                    ordered_events[event["block_number"]][event["transaction_index"]].append(event)        
        
                for block_number in ordered_events:
                    for transaction_index in ordered_events[block_number]:
                        ordered_events[block_number][transaction_index] = sorted(ordered_events[block_number][transaction_index], key=lambda event: event["log_index"])
                
                dex_updates = dict()
                ordered_block_numbers = sorted(list(ordered_events.keys()))
                for block_number in ordered_block_numbers:
                    ordered_transaction_indexes = sorted(list(ordered_events[block_number].keys()))
                    for transaction_index in ordered_transaction_indexes:
                        for event in ordered_events[block_number][transaction_index]:

                            if not event["block_number"] in dex_updates:
                                dex_updates[event["block_number"]] = dict()
                            
                            date = datetime.strptime(event["block_date"], '%Y-%m-%dT%H:%M:%S')

                            dex_updates[event["block_number"]][event["liquidity_pool_address"]] = {
                                "reserve0": event["reserve0"],
                                "reserve1": event["reserve1"],
                                "block_date": event["block_date"],
                                "block_timestamp": int(date.timestamp()) 
                            }

                updates = list()
                for block_number in dex_updates:
                    for pool in dex_updates[block_number]:
                        update = dict()
                        update["id"] = sha256(str(pool+":"+chain+":"+str(block_number)).encode('utf-8')).hexdigest()
                        update["pool"] = pool
                        update["chain"] = chain
                        update["block_number"] = block_number
                        update.update(dex_updates[block_number][pool])
                        updates.append(update)

                print("Found", len(updates), "reserve update(s).")
                if len(updates) > 0:
                    try:
                        collection.insert_many(updates)
                    except pymongo.errors.BulkWriteError:
                        for update in updates:
                            try:
                                collection.insert_one(update)
                            except pymongo.errors.DuplicateKeyError:
                                pass
                    print("Stored", len(updates), "reserve update(s) to MongoDB.")

    print("Indexing MongoDB...")          
    # Indexing...
    if "id" not in collection.index_information():
        collection.create_index("id", unique=True)
        collection.create_index("pool")
        collection.create_index("chain")
        collection.create_index([("block_number", pymongo.DESCENDING)])
        collection.create_index([("block_timestamp", pymongo.DESCENDING)])

    processing_stop = time.time()
    print("Total processing took:", processing_stop - processing_start, "second(s).")
                   

if __name__ == "__main__":
    main()
