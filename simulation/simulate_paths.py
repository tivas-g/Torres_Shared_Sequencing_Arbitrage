#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import math
import copy
import pymongo
import datetime
import multiprocessing

from hashlib import sha256

from path_arb import *

MONGO_HOST = "localhost"
MONGO_PORT = 27017

DEBUG_MODE = False

class colors:
    INFO = '\033[94m'
    OK = '\033[92m'
    FAIL = '\033[91m'
    END = '\033[0m'

def path_string(input_path):
    paths = []
    for entry in input_path:
        _chain = entry["chain"]
        _dex = entry["address"][:8]
        _in = entry["token_in_symbol"]
        _out = entry["token_out_symbol"]
        _protocol = "("+entry["version"].capitalize()+")"
        paths.append(f"{_chain}({_dex})[{_in}->{_protocol}->{_out}]")
    return " -> ".join(paths)

def path_analysis(path_with_prices):
    start = time.time()
    path = path_with_prices[0]
    token_usd_prices = path_with_prices[1]

    if DEBUG_MODE:
        print("Analyzing path:", path_string(path))

    protocols = set([h["protocol"]+"_"+h["version"] for h in path])
    
    if len(protocols) == 1 and "uniswap_v2" in protocols:
        if len(path) == 2:
            amt_in, profit, between_lp_amts = fast_path_two_arb(
                                                float(path[0]["reserve_in"]),
                                                float(path[0]["reserve_out"]),
                                                float(path[1]["reserve_in"]),
                                                float(path[1]["reserve_out"]))
        elif len(path) == 3:
            amt_in, profit, between_lp_amts = fast_path_three_arb(
                                                float(path[0]["reserve_in"]),
                                                float(path[0]["reserve_out"]),
                                                float(path[1]["reserve_in"]),
                                                float(path[1]["reserve_out"]),
                                                float(path[2]["reserve_in"]),
                                                float(path[2]["reserve_out"]))
        elif len(path) == 4:
            amt_in, profit, between_lp_amts = fast_path_four_arb(
                                                float(path[0]["reserve_in"]),
                                                float(path[0]["reserve_out"]),
                                                float(path[1]["reserve_in"]),
                                                float(path[1]["reserve_out"]),
                                                float(path[2]["reserve_in"]),
                                                float(path[2]["reserve_out"]),
                                                float(path[3]["reserve_in"]),
                                                float(path[3]["reserve_out"]))
        elif len(path) == 5:
            amt_in, profit, between_lp_amts = fast_path_five_arb(
                                                float(path[0]["reserve_in"]),
                                                float(path[0]["reserve_out"]),
                                                float(path[1]["reserve_in"]),
                                                float(path[1]["reserve_out"]),
                                                float(path[2]["reserve_in"]),
                                                float(path[2]["reserve_out"]),
                                                float(path[3]["reserve_in"]),
                                                float(path[3]["reserve_out"]),
                                                float(path[4]["reserve_in"]),
                                                float(path[4]["reserve_out"]))
        else:
            print("Path length "+str(len(path))+" not supported.")
            return
    else:
        amt_in, profit, between_lp_amts = ternary_search(path)

    if DEBUG_MODE:
        print("Search time:", time.time()-start)

    amt_in = amt_in / (10 ** int(path[0]["token_in_decimals"]))
    profit = profit / (10 ** int(path[0]["token_in_decimals"]))
          
    token_usd_price = token_usd_prices[path[0]["chain"]][path[0]["token_in_address"]]

    profit_USD = profit * token_usd_price
    amt_in_USD = amt_in * token_usd_price

    if DEBUG_MODE:
        print("Profit USD:", profit_USD, "Input Amount:", amt_in_USD)

    between_lp_amts = [between_lp_amts[i] / (10 ** int(path[i % len(path)]["token_in_decimals"])) for i in range(len(between_lp_amts))]

    if round(profit_USD, 2) > 0 and profit <= amt_in * 0.5:
        return profit_USD, amt_in_USD, between_lp_amts, path
            
    return 0.0, 0.0, between_lp_amts, path

def calculate(paths, updated_pools, prices):
    start = time.time()
    profitable_paths = dict()
    paths_with_positive_gains = set()

    for path in paths:
        gains = list()
        decimals = list()
        liquidity = list()
        for route in path:

            if   route["protocol"] == "uniswap" and route["version"] == "v2":            
                if route["zero_for_one"]:
                    reserve_0 = route["reserve_in"]
                    reserve_1 = route["reserve_out"]
                    decimal_delta = int(route["token_in_decimals"]) - int(route["token_out_decimals"])
                    price = (reserve_1 / reserve_0) * (10 **  decimal_delta)
                    price *= 0.997
                    gains.append(-math.log((price), 2))
                    decimals.append((int(route["token_in_decimals"]), int(route["token_out_decimals"])))
                    liquidity.append(math.sqrt(reserve_0 * reserve_1))
                else:
                    reserve_0 = route["reserve_out"]
                    reserve_1 = route["reserve_in"]
                    decimal_delta = int(route["token_out_decimals"]) - int(route["token_in_decimals"])
                    price = (reserve_0 / reserve_1) * (10 ** -decimal_delta)
                    price *= 0.997
                    gains.append(-math.log((price), 2))
                    decimals.append((int(route["token_out_decimals"]), int(route["token_in_decimals"])))
                    liquidity.append(math.sqrt(reserve_0 * reserve_1))

            elif route["protocol"] == "uniswap" and route["version"] == "v3":
                sqrt_P = int(route["current_price"])
                tick = math.log(math.pow(sqrt_P, 2) / math.pow(2, 192), 1.0001)
                if route["zero_for_one"]:
                    decimal_delta = int(route["token_in_decimals"]) - int(route["token_out_decimals"])
                    price = (1.0001**tick) * (10 ** decimal_delta)
                    fee_perc = float(route["fee_tier"]) / 1_000_000
                    price *= 1 - fee_perc
                    gains.append(-math.log((price), 2))
                    decimals.append((int(route["token_in_decimals"]), int(route["token_out_decimals"])))
                    liquidity.append(int(route["liquidity"]))
                else:
                    decimal_delta = int(route["token_out_decimals"]) - int(route["token_in_decimals"])
                    price = (1.0001**tick) * (10 ** decimal_delta)
                    price = 1 / price
                    fee_perc = float(route["fee_tier"]) / 1_000_000
                    price *= 1 - fee_perc
                    gains.append(-math.log((price), 2))
                    decimals.append((int(route["token_out_decimals"]), int(route["token_in_decimals"])))
                    liquidity.append(int(route["liquidity"]))
        
        gain = sum(gains)
        if gain < 0 and gain > -1.000:        

            # Normalize liquidity and identify smallest liquidity
            max_decimal = max([max(pair) for pair in decimals])
            normalized_liquidity = [int(10 ** (max_decimal - ((decimals[i][0] + decimals[i][0]) / 2))) * liquidity[i] for i in range(len(liquidity))]
            min_liquidity = min(normalized_liquidity)
        
            for pool in updated_pools:
                if pool in [route["address"] for route in path]:
                    if not pool in profitable_paths:
                        profitable_paths[pool] = dict()
                    if not min_liquidity in profitable_paths[pool]:
                        profitable_paths[pool][min_liquidity] = list()
                    profitable_paths[pool][min_liquidity].append({"path": path, "gain": gain})
                    paths_with_positive_gains.add(path_string(path))

    profitable_paths_with_prices = list()
    for pool in profitable_paths:
        for liquidity in profitable_paths[pool]:
            profitable_paths[pool][liquidity] = sorted(profitable_paths[pool][liquidity], key=lambda d: d["gain"])
            profitable_paths_with_prices.append((profitable_paths[pool][liquidity][0]["path"], prices))
            
    simulated_paths = list()
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        simulated_paths += pool.map(path_analysis, profitable_paths_with_prices)
            
    max_profit_paths = list()
    for profit, amount, inputs, path in simulated_paths:
        if profit > 0:
            max_profit_paths.append({"profit": profit, "amount": amount, "inputs": inputs, "path": path})

    total_profit = 0
    conflicting_paths = list()
    profitable_non_conflicting_paths = list()
    sorted_max_profit_paths = list(reversed(sorted(max_profit_paths, key=lambda d: d["profit"])))
    for i in range(len(sorted_max_profit_paths)):
        path_i = sorted_max_profit_paths[i]
        if path_i in conflicting_paths:
            continue
        for j in range(i+1, len(sorted_max_profit_paths)):
            path_j = sorted_max_profit_paths[j]
            if len(set([route["address"] for route in path_i["path"]]).intersection(set([route["address"] for route in path_j["path"]]))) > 0:
                conflicting_paths.append(path_j)
        total_profit += path_i["profit"]
        if DEBUG_MODE:
            print(colors.OK+"{0:.2f}".format(round(path_i["profit"], 2))+" USD on "+"{0:.2f}".format(round(path_i["amount"], 2))+" USD using "+str(path_string(path_i["path"]))+colors.END)
        profitable_non_conflicting_paths.append(path_i)
    
    print("Number of paths with positive gains:", colors.INFO, len(paths_with_positive_gains), colors.END)
    if DEBUG_MODE:
        print("Reduction of", colors.INFO, str(100-len(paths_with_positive_gains)/len(paths)*100.0)+"%", colors.END)   
    print("Number of paths simulated:", colors.INFO, len(simulated_paths), colors.END)
    if DEBUG_MODE:
        print("Reduction of", colors.INFO, str(100-len(simulated_paths)/len(paths)*100.0)+"%", colors.END)  
    print("Number of non-conflicting profitable paths:", colors.INFO, len(profitable_non_conflicting_paths), colors.END)
    if DEBUG_MODE:
        print("Calculating profitable paths took:", colors.INFO, time.time() - start, colors.END, "second(s).")
    print("Total profit:", colors.OK, total_profit, "USD", colors.END)
    
    return len(paths_with_positive_gains), len(simulated_paths), profitable_non_conflicting_paths, total_profit


def main():
    if len(sys.argv) < 4:
        print(colors.FAIL+"Error: Please provide a time range to be analyzed and a time window: 'python3 "+sys.argv[0]+" <single-chain|cross-chain> <START_UNIX_TIMESTAMP> <STOP_UNIX_TIMESTAMP> [<TIME_WINDOW>]'"+colors.END)
        sys.exit(-1)

    if not sys.argv[1].lower() in ["single-chain", "cross-chain"]:
        print(colors.FAIL+"Error: Please provide a valid parameter: <single-chain|cross-chain>"+colors.END)
        sys.exit(-2)

    if sys.argv[1].lower() == "cross-chain" and len(sys.argv) == 4:
        print(colors.FAIL+"Error: Please provide a time window: 'python3 "+sys.argv[0]+" <single-chain|cross-chain> <START_UNIX_TIMESTAMP> <STOP_UNIX_TIMESTAMP> [<TIME_WINDOW>]'"+colors.END)
        sys.exit(-3)

    print("Initializing...")
    
    start = time.time()

    paths_file                  = "../data/single_chain_paths.json" if sys.argv[1].lower() == "single-chain" else "../data/cross_chain_paths.json"
    collection_profitable_paths = "single_chain_profitable_paths" if sys.argv[1].lower() == "single-chain" else "cross_chain_profitable_paths"
    start_unix_timestamp        = int(sys.argv[2])
    stop_unix_timestamp         = int(sys.argv[3])
    time_window                 = None if sys.argv[1].lower() == "single-chain" else int(sys.argv[4])

    print("Loading arbitrage paths...")
    p_start = time.time()
    paths = list()
    if not os.path.exists(paths_file):
        print("Error: '"+paths_file+"' file does not exist. Please run path builder first to generate a list of single-chain and cross-chain arbitrage paths.")
        sys.exit(-3)
    with open(paths_file, "r") as f:
        paths = json.load(f)
    print("Loaded", colors.INFO, len(paths), colors.END, "arbitrage paths.")
    p_stop = time.time()
    if DEBUG_MODE:
        print("Loading arbitrage paths took:", colors.INFO, p_stop - p_start, colors.END, "second(s).")

    print("Indexing and populating paths...")
    p_start = time.time()
    initial_pool_states = dict()
    if os.path.exists("../data/initial_pool_states.json"):
        with open("../data/initial_pool_states.json", "r") as f:
            initial_pool_states = json.load(f)
    for path in paths:
        for route in path:
            if route["address"] in initial_pool_states:
                if route["protocol"] == "uniswap" and route["version"] == "v2":
                    if initial_pool_states[route["address"]]["reserve0"] != None and initial_pool_states[route["address"]]["reserve0"] != None:
                        if route["zero_for_one"]:
                            route["reserve_in"]  = int(float(initial_pool_states[route["address"]]["reserve0"]) * (10 ** int(route["token_in_decimals"])))
                            route["reserve_out"] = int(float(initial_pool_states[route["address"]]["reserve1"]) * (10 ** int(route["token_out_decimals"])))
                        else:
                            route["reserve_in"]  = int(float(initial_pool_states[route["address"]]["reserve1"]) * (10 ** int(route["token_in_decimals"])))
                            route["reserve_out"] = int(float(initial_pool_states[route["address"]]["reserve0"]) * (10 ** int(route["token_out_decimals"])))
                    else:
                        route["reserve_in"]  = None
                        route["reserve_out"] = None
                elif route["protocol"] == "uniswap" and route["version"] == "v3":
                    route["liquidity"]     = initial_pool_states[route["address"]]["liquidity"]
                    route["current_price"] = initial_pool_states[route["address"]]["sqrt_price"]
                    route["current_tick"]  = initial_pool_states[route["address"]]["tick"]
                    route["ticks"]         = initial_pool_states[route["address"]]["ticks"]
    path_index = dict()
    for path in paths:
        for route in path:
            if not route["address"] in path_index:
                path_index[route["address"]] = list()
            path_index[route["address"]].append(path)
    p_stop = time.time()
    if DEBUG_MODE:
        print("Indexing and populating took:", colors.INFO, p_stop - p_start, colors.END, "second(s).")

    print("Loading reserve token prices...")
    p_start = time.time()
    prices = dict()
    if not os.path.exists("../data/prices"):
        print("Error: 'data/prices' folder does not exist. Please fetch first prices of reserve tokens.")
        sys.exit(-2)
    for chain in os.listdir("../data/prices"):
        if os.path.isdir(os.path.join("../data/prices", chain)):
            if not chain in prices:
                prices[chain] = dict()
            for file in os.listdir(os.path.join("../data/prices", chain)):
                if os.path.isfile(os.path.join("../data/prices", chain, file)) and file.endswith(".json"):
                    address = file.split("_")[1]
                    if not address in prices[chain]:
                        prices[chain][address] = dict()
                    with open(os.path.join("../data/prices", chain, file), "r") as f:
                        fetched_prices = json.load(f)
                        for fetched_price in fetched_prices:
                            timestamp = int(datetime.datetime.strptime(fetched_price["timestamp"], "%Y-%m-%dT%H:%M:%S").timestamp())
                            prices[chain][address][timestamp] = fetched_price["price"]
    p_stop = time.time()
    if DEBUG_MODE:
        print("Loading reserve token prices took:", colors.INFO, p_stop - p_start, colors.END, "second(s).")
    
    mongo_connection = pymongo.MongoClient("mongodb://"+MONGO_HOST+":"+str(MONGO_PORT), maxPoolSize=None)

    if sys.argv[1].lower() == "single-chain":
        previous_timestamp = None
        for current_timestamp in range(start_unix_timestamp, stop_unix_timestamp+1):
            if previous_timestamp != None:
                unique_pool_updates = dict()
                
                print()
                print("Analyzing arbitrage opportunities between timestamp", colors.INFO, previous_timestamp, colors.END, "and timestamp", colors.INFO, current_timestamp, colors.END)

                db_start = time.time()
                retrieved_pool_updates = list(mongo_connection["cross_chain_arbitrage"]["dex_updates"].find({"block_timestamp": {"$gt": previous_timestamp, "$lte": current_timestamp}}))            
                
                for pool_update in retrieved_pool_updates:
                    if not pool_update["block_number"] in unique_pool_updates:
                        unique_pool_updates[pool_update["block_number"]] = dict()
                    if not pool_update["pool"] in unique_pool_updates[pool_update["block_number"]] :
                        unique_pool_updates[pool_update["block_number"]][pool_update["pool"] ] = pool_update
                db_stop = time.time()
                if DEBUG_MODE:
                    print("Number of pool updates:", colors.INFO, len(unique_pool_updates), colors.END)
                    print("MongoDB retrieval took:", colors.INFO, db_stop - db_start, colors.END, "second(s).")

                if len(unique_pool_updates) > 0:
                    for updated_block in sorted(unique_pool_updates.keys()):
                        u_start = time.time()
                        updated_paths = list()
                        unique_paths = set()
                        chain = ""
                        for updated_pool_address in unique_pool_updates[updated_block]:
                            for path in path_index[updated_pool_address]:                
                                path_id = ""
                                for route in path:
                                    chain = route["chain"]
                                    path_id += route["address"]
                                    if route["address"] == updated_pool_address:
                                        if   route["protocol"] == "uniswap" and route["version"] == "v2":
                                            if route["zero_for_one"]:
                                                route["reserve_in"]  = int(float(unique_pool_updates[updated_block][updated_pool_address]["reserve0"]) * (10 ** int(route["token_in_decimals"])))
                                                route["reserve_out"] = int(float(unique_pool_updates[updated_block][updated_pool_address]["reserve1"]) * (10 ** int(route["token_out_decimals"])))
                                            else:
                                                route["reserve_in"]  = int(float(unique_pool_updates[updated_block][updated_pool_address]["reserve1"]) * (10 ** int(route["token_in_decimals"])))
                                                route["reserve_out"] = int(float(unique_pool_updates[updated_block][updated_pool_address]["reserve0"]) * (10 ** int(route["token_out_decimals"])))
                                        elif route["protocol"] == "uniswap" and route["version"] == "v3":
                                            route["liquidity"]     = unique_pool_updates[updated_block][updated_pool_address]["liquidity"]
                                            route["current_price"] = unique_pool_updates[updated_block][updated_pool_address]["sqrt_price"]
                                            route["current_tick"]  = unique_pool_updates[updated_block][updated_pool_address]["tick"]
                                            route["ticks"]         = unique_pool_updates[updated_block][updated_pool_address]["ticks"]
                                if not path_id in unique_paths:
                                    unique_paths.add(path_id)
                                    updated_paths.append(path)

                        valid_paths = list()
                        for path in updated_paths:
                            valid_path = True
                            for route in path:
                                if route["protocol"] == "uniswap" and route["version"] == "v2":
                                    if route["reserve_in"] == None or route["reserve_out"] == None:
                                        valid_path = False
                                        break
                                elif route["protocol"] == "uniswap" and route["version"] == "v3":
                                    if not "current_price" in route:
                                        valid_path = False
                                        break
                            if valid_path:
                                valid_paths.append(path)

                        u_stop = time.time()
                        print("Number of updated paths:", colors.INFO, len(valid_paths), colors.END)
                        if DEBUG_MODE:
                            print("Calculating path updates took:", colors.INFO, u_stop - u_start, colors.END, "second(s).")

                        if len(valid_paths) > 0:
                            print("Calculating profitable paths...")

                            current_prices = dict()
                            for path in valid_paths:
                                chain = path[0]["chain"]
                                if not chain in current_prices:
                                    current_prices[chain] = dict()
                                address = path[0]["token_in_address"]
                                if not address in current_prices[chain]:
                                    for price_timestamp in sorted(prices[chain][address].keys()):
                                        if price_timestamp > current_timestamp:
                                            break
                                        current_prices[chain][address] = prices[chain][address][price_timestamp]
            
                            paths_with_positive_gains, simulated_paths, profitable_non_conflicting_paths, total_profit = calculate(valid_paths, list(unique_pool_updates[updated_block].keys()), current_prices)

                            try:
                                compressed_profitable_paths = copy.deepcopy(profitable_non_conflicting_paths)
                                
                                for path in compressed_profitable_paths:
                                    path["profit_usd"] = path["profit"]
                                    del path["profit"]
                                    
                                    path["amount_usd"] = path["amount"]
                                    del path["amount"]
                                    
                                    for i in range(len(path["path"])):
                                        route = path["path"][i]
                                        route["token_in_amount"] = path["inputs"][i]
                                        route["token_out_amount"] = path["inputs"][i + 1 % len(path["path"])]
                                        if   route["protocol"] == "uniswap" and route["version"] == "v2":
                                            del route["reserve_in"]
                                            del route["reserve_out"]
                                        elif route["protocol"] == "uniswap" and route["version"] == "v3":
                                            del route["liquidity"]
                                            del route["current_price"]
                                            del route["current_tick"]
                                            del route["ticks"]
                                    del path["inputs"]

                                cross_chain_profitable_paths = {
                                    "id": sha256(str(str(previous_timestamp)+":"+str(current_timestamp)+":"+str(updated_block)+":"+chain).encode('utf-8')).hexdigest(),
                                    "chain": chain,
                                    "block_number": updated_block,
                                    "timestamp_range_start": previous_timestamp,
                                    "timestamp_range_stop": current_timestamp,
                                    "number_of_updated_pools": len(unique_pool_updates),
                                    "number_of_updated_paths": len(valid_paths),
                                    "number_of_paths_with_positive_gains": paths_with_positive_gains,
                                    "number_of_simulated_paths": simulated_paths,
                                    "number_of_profitable_non_conflicting_paths": len(profitable_non_conflicting_paths),
                                    "profitable_non_conflicting_paths": compressed_profitable_paths,
                                    "total_profit_usd": total_profit
                                }
                                
                                mongo_connection["cross_chain_arbitrage"][collection_profitable_paths].insert_one(cross_chain_profitable_paths)
                            except pymongo.errors.DuplicateKeyError:
                                pass

            previous_timestamp = current_timestamp

    else:
        previous_timestamp = None
        for current_timestamp in range(start_unix_timestamp, stop_unix_timestamp+1, time_window):
            if previous_timestamp != None:
                profitable_non_conflicting_paths = list()
                paths_with_positive_gains = 0
                unique_pool_updates = dict()
                simulated_paths = 0
                valid_paths = list()
                total_profit = 0
                
                id = sha256(str(str(previous_timestamp)+":"+str(current_timestamp)).encode('utf-8')).hexdigest()
                found = mongo_connection["cross_chain_arbitrage"][collection_profitable_paths].find_one({"id": id})
                
                if not found:
                    print()
                    print("Analyzing arbitrage opportunities between timestamp", colors.INFO, previous_timestamp, colors.END, "and timestamp", colors.INFO, current_timestamp, colors.END)
                
                db_start = time.time()
                retrieved_pool_updates = list(mongo_connection["cross_chain_arbitrage"]["dex_updates"].find({"block_timestamp": {"$gt": previous_timestamp, "$lte": current_timestamp}}))            
                
                for pool_update in retrieved_pool_updates:
                    if not pool_update["pool"] in unique_pool_updates:
                        unique_pool_updates[pool_update["pool"]] = pool_update
                db_stop = time.time()
                if DEBUG_MODE:
                    print("Number of pool updates:", colors.INFO, len(unique_pool_updates), colors.END)
                    print("MongoDB retrieval took:", colors.INFO, db_stop - db_start, colors.END, "second(s).")

                if len(unique_pool_updates) > 0:       
                    u_start = time.time()
                    updated_paths = list()
                    unique_paths = set()
                    for updated_pool_address in unique_pool_updates:
                        for path in path_index[updated_pool_address]:                
                            path_id = ""
                            for route in path:
                                path_id += route["address"]
                                if route["address"] == updated_pool_address:
                                    if   route["protocol"] == "uniswap" and route["version"] == "v2":
                                        if route["zero_for_one"]:
                                            route["reserve_in"]  = int(float(unique_pool_updates[updated_pool_address]["reserve0"]) * (10 ** int(route["token_in_decimals"])))
                                            route["reserve_out"] = int(float(unique_pool_updates[updated_pool_address]["reserve1"]) * (10 ** int(route["token_out_decimals"])))
                                        else:
                                            route["reserve_in"]  = int(float(unique_pool_updates[updated_pool_address]["reserve1"]) * (10 ** int(route["token_in_decimals"])))
                                            route["reserve_out"] = int(float(unique_pool_updates[updated_pool_address]["reserve0"]) * (10 ** int(route["token_out_decimals"])))
                                    elif route["protocol"] == "uniswap" and route["version"] == "v3":
                                        route["liquidity"]     = unique_pool_updates[updated_pool_address]["liquidity"]
                                        route["current_price"] = unique_pool_updates[updated_pool_address]["sqrt_price"]
                                        route["current_tick"]  = unique_pool_updates[updated_pool_address]["tick"]
                                        route["ticks"]         = unique_pool_updates[updated_pool_address]["ticks"]
                            if not path_id in unique_paths:
                                unique_paths.add(path_id)
                                updated_paths.append(path)

                    for path in updated_paths:
                        valid_path = True
                        for route in path:
                            if route["protocol"] == "uniswap" and route["version"] == "v2":
                                if route["reserve_in"] == None or route["reserve_out"] == None:
                                    valid_path = False
                                    break
                            elif route["protocol"] == "uniswap" and route["version"] == "v3":
                                if not "current_price" in route:
                                    valid_path = False
                                    break
                        if valid_path:
                            valid_paths.append(path)

                    u_stop = time.time()
                    if not found:
                        print("Number of updated paths:", colors.INFO, len(valid_paths), colors.END)
                        if DEBUG_MODE:
                            print("Calculating path updates took:", colors.INFO, u_stop - u_start, colors.END, "second(s).")

                        if len(valid_paths) > 0:
                            print("Calculating profitable paths...")

                            current_prices = dict()
                            for path in valid_paths:
                                chain = path[0]["chain"]
                                if not chain in current_prices:
                                    current_prices[chain] = dict()
                                address = path[0]["token_in_address"]
                                if not address in current_prices[chain]:
                                    for price_timestamp in sorted(prices[chain][address].keys()):
                                        if price_timestamp > current_timestamp:
                                            break
                                        current_prices[chain][address] = prices[chain][address][price_timestamp]
                            
                            paths_with_positive_gains, simulated_paths, profitable_non_conflicting_paths, total_profit = calculate(valid_paths, list(unique_pool_updates.keys()), current_prices)
            
                if not found:
                    try:
                        compressed_profitable_paths = copy.deepcopy(profitable_non_conflicting_paths)
                        
                        for path in compressed_profitable_paths:
                            path["profit_usd"] = path["profit"]
                            del path["profit"]
                            
                            path["amount_usd"] = path["amount"]
                            del path["amount"]
                            
                            for i in range(len(path["path"])):
                                route = path["path"][i]
                                route["token_in_amount"] = path["inputs"][i]
                                route["token_out_amount"] = path["inputs"][i + 1 % len(path["path"])]
                                if   route["protocol"] == "uniswap" and route["version"] == "v2":
                                    del route["reserve_in"]
                                    del route["reserve_out"]
                                elif route["protocol"] == "uniswap" and route["version"] == "v3":
                                    del route["liquidity"]
                                    del route["current_price"]
                                    del route["current_tick"]
                                    del route["ticks"]
                            del path["inputs"]

                        cross_chain_profitable_paths = {
                            "id": id,
                            "timestamp_range_start": previous_timestamp,
                            "timestamp_range_stop": current_timestamp,
                            "number_of_updated_pools": len(unique_pool_updates),
                            "number_of_updated_paths": len(valid_paths),
                            "number_of_paths_with_positive_gains": paths_with_positive_gains,
                            "number_of_simulated_paths": simulated_paths,
                            "number_of_profitable_non_conflicting_paths": len(profitable_non_conflicting_paths),
                            "profitable_non_conflicting_paths": compressed_profitable_paths,
                            "total_profit_usd": total_profit
                        }
                        
                        mongo_connection["cross_chain_arbitrage"][collection_profitable_paths].insert_one(cross_chain_profitable_paths)
                    except pymongo.errors.DuplicateKeyError:
                        pass
            
            previous_timestamp = current_timestamp
        
    print()
    print("Indexing MongoDB...")          
    # Indexing...
    i_start = time.time()
    collection = mongo_connection["cross_chain_arbitrage"][collection_profitable_paths]
    if "id" not in collection.index_information():
        collection.create_index("id", unique=True)
        collection.create_index("timestamp_range_start")
        collection.create_index("timestamp_range_stop")
        collection.create_index("number_of_updated_pools")
        collection.create_index("number_of_updated_paths")
        collection.create_index("number_of_profitable_paths")
    i_stop = time.time()
    if DEBUG_MODE:
        print("Indexing MongoDB took:", i_stop - i_start, "second(s).")
    
    print()
    print("Total execution time:", time.time()-start)


if __name__ == "__main__":
    main()
