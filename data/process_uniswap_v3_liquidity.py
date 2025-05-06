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

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

# INPUT
STARTING_DATE     = "2024-11-01"
ENDING_DATE       = "2024-11-01"
CHAINS            = ["arbitrum", "optimism", "base"] 
INPUT_FOLDER      = "uniswap_v3"
OPTIMISM_PROVIDER = Web3.HTTPProvider("https://rpc.ankr.com/optimism", request_kwargs={'timeout': 60})
BASE_PROVIDER     = Web3.HTTPProvider("https://rpc.ankr.com/base",     request_kwargs={'timeout': 60})
ARBITRUM_PROVIDER = Web3.HTTPProvider("https://rpc.ankr.com/arbitrum", request_kwargs={'timeout': 60})

# OUTPUT
MONGO_HOST        = "localhost"
MONGO_PORT        = 27017

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

            if not os.path.exists(INPUT_FOLDER+"/"+chain+"/"+chain+"_uniswap_v3_events_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json"):
                print(colors.FAIL+"Error file '"+INPUT_FOLDER+"/"+chain+"/"+chain+"_uniswap_v3_events_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json' is missing!"+colors.END)
                sys.exit(-1)
            
            print("Searching for liquidity updates within:", INPUT_FOLDER+"/"+chain+"/"+chain+"_uniswap_v3_events_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json")
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

                dex_state = dict()
                dex_updates = dict()
                
                ordered_block_numbers = sorted(list(ordered_events.keys()))
                for block_number in ordered_block_numbers:
                    ordered_transaction_indexes = sorted(list(ordered_events[block_number].keys()))
                    for transaction_index in ordered_transaction_indexes:
                        for event in ordered_events[block_number][transaction_index]:

                            if not event["block_number"] in dex_updates:
                                dex_updates[event["block_number"]] = dict()
                            
                            date = datetime.strptime(event["block_date"], '%Y-%m-%dT%H:%M:%S')
                            
                            if   event["event"] == "swap":
                                dex_state[event["liquidity_pool_address"]] = {
                                    "tick": str(int(float(event["tick"]))),
                                    "liquidity": event["liquidity"],
                                    "sqrt_price": event["sqrt_price"],
                                }

                                dex_updates[event["block_number"]][event["liquidity_pool_address"]] = {
                                    "tick": str(int(float(event["tick"]))),
                                    "liquidity": event["liquidity"],
                                    "sqrt_price": event["sqrt_price"],
                                    "block_date": event["block_date"],
                                    "block_timestamp": int(date.timestamp()) 
                                }
                            
                            elif event["event"] == "mint":
                                if event["liquidity_pool_address"] in dex_state:
                                    if  int(float(event["tick_lower"])) <= int(dex_state[event["liquidity_pool_address"]]["tick"]) and \
                                        int(float(event["tick_upper"])) > int(dex_state[event["liquidity_pool_address"]]["tick"]):
                                    
                                        dex_state[event["liquidity_pool_address"]]["liquidity"] = str(int(dex_state[event["liquidity_pool_address"]]["liquidity"]) + int(event["liquidity"]))
                                        
                                        dex_updates[event["block_number"]][event["liquidity_pool_address"]] = {
                                            "tick": dex_state[event["liquidity_pool_address"]]["tick"],
                                            "liquidity": dex_state[event["liquidity_pool_address"]]["liquidity"],
                                            "sqrt_price": dex_state[event["liquidity_pool_address"]]["sqrt_price"],
                                            "block_date": event["block_date"],
                                            "block_timestamp": int(date.timestamp()) 
                                        }
                                else:
                                    if   chain == "arbitrum":
                                        w3 = Web3(ARBITRUM_PROVIDER)
                                    elif chain == "optimism":
                                        w3 = Web3(OPTIMISM_PROVIDER)
                                    elif chain == "base":
                                        w3 = Web3(BASE_PROVIDER)
                                    if w3 and w3.is_connected():
                                        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
                                        pool_contract = w3.eth.contract(address=Web3.to_checksum_address(event["liquidity_pool_address"]), abi=[
                                            {"inputs":[],"name":"liquidity","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"},
                                            {"inputs":[],"name":"slot0","outputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"internalType":"int24","name":"tick","type":"int24"},{"internalType":"uint16","name":"observationIndex","type":"uint16"},{"internalType":"uint16","name":"observationCardinality","type":"uint16"},{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"},{"internalType":"uint8","name":"feeProtocol","type":"uint8"},{"internalType":"bool","name":"unlocked","type":"bool"}],"stateMutability":"view","type":"function"},
                                        ])
                                        liquidity = pool_contract.functions.liquidity().call(block_identifier=event["block_number"])
                                        slot0 = pool_contract.functions.slot0().call(block_identifier=event["block_number"])
                                        dex_updates[event["block_number"]][event["liquidity_pool_address"]] = {
                                            "tick": str(int(float(slot0[1]))),
                                            "liquidity": str(liquidity),
                                            "sqrt_price": str(slot0[0]),
                                            "block_date": event["block_date"],
                                            "block_timestamp": int(date.timestamp()) 
                                        }
                                    else:
                                        sys.exit(-2)

                            elif event["event"] == "burn":
                                if event["liquidity_pool_address"] in dex_state:
                                    if  int(float(event["tick_lower"])) <= int(dex_state[event["liquidity_pool_address"]]["tick"]) and \
                                        int(float(event["tick_upper"])) > int(dex_state[event["liquidity_pool_address"]]["tick"]):

                                        dex_state[event["liquidity_pool_address"]]["liquidity"] = str(int(dex_state[event["liquidity_pool_address"]]["liquidity"]) - int(event["liquidity"]))
                                        
                                        dex_updates[event["block_number"]][event["liquidity_pool_address"]] = {
                                            "tick": dex_state[event["liquidity_pool_address"]]["tick"],
                                            "liquidity": dex_state[event["liquidity_pool_address"]]["liquidity"],
                                            "sqrt_price": dex_state[event["liquidity_pool_address"]]["sqrt_price"],
                                            "block_date": event["block_date"],
                                            "block_timestamp": int(date.timestamp()) 
                                        }
                                else:
                                    if   chain == "arbitrum":
                                        w3 = Web3(ARBITRUM_PROVIDER)
                                    elif chain == "optimism":
                                        w3 = Web3(OPTIMISM_PROVIDER)
                                    elif chain == "base":
                                        w3 = Web3(BASE_PROVIDER)
                                    if w3 and w3.is_connected():
                                        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
                                        pool_contract = w3.eth.contract(address=Web3.to_checksum_address(event["liquidity_pool_address"]), abi=[
                                            {"inputs":[],"name":"liquidity","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"},
                                            {"inputs":[],"name":"slot0","outputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"internalType":"int24","name":"tick","type":"int24"},{"internalType":"uint16","name":"observationIndex","type":"uint16"},{"internalType":"uint16","name":"observationCardinality","type":"uint16"},{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"},{"internalType":"uint8","name":"feeProtocol","type":"uint8"},{"internalType":"bool","name":"unlocked","type":"bool"}],"stateMutability":"view","type":"function"},
                                        ])
                                        liquidity = pool_contract.functions.liquidity().call(block_identifier=event["block_number"])
                                        slot0 = pool_contract.functions.slot0().call(block_identifier=event["block_number"])
                                        dex_updates[event["block_number"]][event["liquidity_pool_address"]] = {
                                            "tick": str(int(float(slot0[1]))),
                                            "liquidity": str(liquidity),
                                            "sqrt_price": str(slot0[0]),
                                            "block_date": event["block_date"],
                                            "block_timestamp": int(date.timestamp()) 
                                        }
                                    else:
                                        sys.exit(-2)

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

                print("Found", len(updates), "liquidity update(s).")
                if len(updates) > 0:
                    try:
                        collection.insert_many(updates)
                    except pymongo.errors.BulkWriteError:
                        for update in updates:
                            try:
                                collection.insert_one(update)
                            except pymongo.errors.DuplicateKeyError:
                                pass
                    print("Stored", len(updates), "liquidity update(s) to MongoDB.")

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
