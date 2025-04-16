#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import pprint
import requests

STARTING_DATE = "2024-11-01"
API_KEY       = "W8ax4JoLX4xa9pX1Qow-uMrPGnImFEHo-YjKM4ixU2A4b7WhxDuGVishO0djeMKb6Bt-5ouZBn8aTJqDGQF43w"


def main():
    pools = list()
    with open("pools.json", "r") as f:
        pools = json.load(f)

    initial_pool_state = dict()
    if os.path.exists("initial_pool_states.json"):
        with open("initial_pool_states.json", "r") as f:
            initial_pool_state = json.load(f)

    for pool in pools:
        if not pool["address"] in initial_pool_state:
            print("Downloading initial pool state for pool", pool["address"], "as of", STARTING_DATE+"T00:00:00.")
            if pool["protocol"] == "uniswap":
                if   pool["version"] == "v2":
                    response = requests.post(
                        "https://api.allium.so/api/v1/explorer/queries/hf7Ayf6BVo83ArAjqOX1/run",
                        json={"chain": pool["chain"], "block_timestamp": STARTING_DATE+"T00:00:00", "pool": pool["address"]},
                        headers={"X-API-Key": API_KEY},
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

                    with open("initial_pool_states.json", "w") as f:
                        json.dump(initial_pool_state, f, indent=4)

                elif pool["version"] == "v3":
                    response = requests.post(
                        "https://api.allium.so/api/v1/explorer/queries/Y1y4Q4pbNhaiQPEzSFdw/run",
                        json={"chain": pool["chain"], "events": "('swap', 'mint', 'burn')", "block_timestamp": STARTING_DATE+"T00:00:00", "pool": pool["address"], "limit": "1"},
                        headers={"X-API-Key": API_KEY},
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
                                headers={"X-API-Key": API_KEY},
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

                    with open("initial_pool_states.json", "w") as f:
                        json.dump(initial_pool_state, f, indent=4)
                    

if __name__ == "__main__":
    main()
