#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import requests

# INPUT
STARTING_DATE       = "2024-11-01"
POOLS               = "pools.json"

# OUTPUT
INITIAL_POOL_STATES = "initial_pool_states.json"

class colors:
    INFO = '\033[94m'
    OK = '\033[92m'
    FAIL = '\033[91m'
    END = '\033[0m'


def main():
    if len(sys.argv) < 2:
        print(colors.FAIL+"Error: Please provide an Allium API key to download initial pool states: 'python3 "+sys.argv[0]+" <ALLIUM_API_KEY>'"+colors.END)
        sys.exit(-1)

    allium_api_key = sys.argv[1]

    if not os.path.exists(POOLS):
        print(colors.FAIL+"Error: Please run 'data/extract_pools.py' first to create the '"+POOLS+"' file first!"+colors.END)
        sys.exit(-2)

    pools = list()
    with open(POOLS, "r") as f:
        pools = json.load(f)

    initial_pool_state = dict()
    if os.path.exists(INITIAL_POOL_STATES):
        with open(INITIAL_POOL_STATES, "r") as f:
            initial_pool_state = json.load(f)

    for pool in pools:
        if not pool["address"] in initial_pool_state:
            print("Downloading initial pool state for pool", pool["address"], "as of", STARTING_DATE+"T00:00:00.")
            if pool["protocol"] == "uniswap":
                if   pool["version"] == "v2":
                    response = requests.post(
                        "https://api.allium.so/api/v1/explorer/queries/hf7Ayf6BVo83ArAjqOX1/run",
                        json={"chain": pool["chain"], "block_timestamp": STARTING_DATE+"T00:00:00", "pool": pool["address"]},
                        headers={"X-API-Key": allium_api_key},
                    )

                    data = response.json()["data"]

                    if len(data) > 0:
                        initial_pool_state[pool["address"].lower()] = {
                            "reserve0": str(data[0]["reserve0"]),
                            "reserve1": str(data[0]["reserve1"])
                        }
                    else:
                        initial_pool_state[pool["address"].lower()] = {
                            "reserve0": None,
                            "reserve1": None
                        }

                    with open(INITIAL_POOL_STATES, "w") as f:
                        json.dump(initial_pool_state, f, indent=4)

                elif pool["version"] == "v3":
                    response = requests.post(
                        "https://api.allium.so/api/v1/explorer/queries/Y1y4Q4pbNhaiQPEzSFdw/run",
                        json={"chain": pool["chain"], "events": "('swap', 'mint', 'burn')", "block_timestamp": STARTING_DATE+"T00:00:00", "pool": pool["address"], "limit": "1"},
                        headers={"X-API-Key": allium_api_key},
                    )

                    data = response.json()["data"]

                    if len(data) > 0:
                        if data[0]["event"] == "swap":

                            initial_pool_state[pool["address"].lower()] = {
                                "tick": str(int(float(data[0]["tick"]))),
                                "ticks": dict(),
                                "liquidity": str(data[0]["liquidity"]),
                                "sqrt_price": str(data[0]["sqrt_price"])
                            }

                            if os.path.exists("uniswap_v3/"+pool["chain"]+"/"+pool["chain"]+"_uniswap_v3_ticks_"+pool["address"]+".json"):
                                with open("uniswap_v3/"+pool["chain"]+"/"+pool["chain"]+"_uniswap_v3_ticks_"+pool["address"]+".json", "r") as f:
                                    initial_pool_state[pool["address"].lower()]["ticks"] = json.load(f)
                            else:
                                del initial_pool_state[pool["address"].lower()]
                        else:
                            response = requests.post(
                                "https://api.allium.so/api/v1/explorer/queries/Y1y4Q4pbNhaiQPEzSFdw/run",
                                json={"chain": pool["chain"], "events": "('swap', 'mint', 'burn')", "block_timestamp": STARTING_DATE+"T00:00:00", "pool": pool["address"], "limit": "100"},
                                headers={"X-API-Key": allium_api_key},
                            )
                            
                            data = response.json()["data"]

                            events = list()
                            for event in data:
                                if event["event"] == "swap":
                                    initial_pool_state[pool["address"].lower()] = {
                                        "tick": str(int(float(event["tick"]))),
                                        "ticks": dict(),
                                        "liquidity": str(event["liquidity"]),
                                        "sqrt_price": str(event["sqrt_price"])
                                    }
                                    break
                                events.append(event)

                            for event in reversed(events):
                                if   event["event"] == "mint":
                                    if  int(float(event["tick_lower"])) <= int(initial_pool_state[pool["address"].lower()]["tick"]) and \
                                        int(float(event["tick_upper"])) >  int(initial_pool_state[pool["address"].lower()]["tick"]):
                                        initial_pool_state[pool["address"].lower()]["liquidity"] = str(int(initial_pool_state[pool["address"].lower()]["liquidity"]) + int(event["liquidity"]))
                                elif event["event"] == "burn":
                                    if  int(float(event["tick_lower"])) <= int(initial_pool_state[pool["address"].lower()]["tick"]) and \
                                        int(float(event["tick_upper"])) >  int(initial_pool_state[pool["address"].lower()]["tick"]):
                                        initial_pool_state[pool["address"].lower()]["liquidity"] = str(int(initial_pool_state[pool["address"].lower()]["liquidity"]) - int(event["liquidity"]))
                            
                            if os.path.exists("uniswap_v3/"+pool["chain"]+"/"+pool["chain"]+"_uniswap_v3_ticks_"+pool["address"]+".json"):
                                with open("uniswap_v3/"+pool["chain"]+"/"+pool["chain"]+"_uniswap_v3_ticks_"+pool["address"]+".json", "r") as f:
                                    initial_pool_state[pool["address"].lower()]["ticks"] = json.load(f)
                            else:
                                del initial_pool_state[pool["address"].lower()]
                    else:
                        initial_pool_state[pool["address"].lower()] = {                            
                            "tick": None,
                            "ticks": dict(),
                            "liquidity": None,
                            "sqrt_price": None
                        }

                    with open(INITIAL_POOL_STATES, "w") as f:
                        json.dump(initial_pool_state, f, indent=4)
                    

if __name__ == "__main__":
    main()
