#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time

MAX_PATH_LENGTH = 5

class colors:
    INFO = '\033[94m'
    OK = '\033[92m'
    FAIL = '\033[91m'
    END = '\033[0m'

class path_builder:
    def __init__(self, _reserve_tokens):
        self.chain_to_chain_mapping = dict()
        self.reserve_tokens = dict()
        self.chains = list()

        for ticker in _reserve_tokens:
            for chain in _reserve_tokens[ticker]:
                if not chain in self.reserve_tokens:
                    self.reserve_tokens[chain] = dict()
                    self.chains.append(chain)
                self.reserve_tokens[chain][ticker] = _reserve_tokens[ticker][chain]
        
        for chain_1 in self.reserve_tokens:
            for ticker in self.reserve_tokens[chain_1]:
                for chain_2 in self.reserve_tokens:
                    if chain_1 != chain_2 and ticker in self.reserve_tokens[chain_2]:
                        if not chain_1+chain_2 in self.chain_to_chain_mapping:
                            self.chain_to_chain_mapping[chain_1+chain_2] = dict()
                        self.chain_to_chain_mapping[chain_1+chain_2][self.reserve_tokens[chain_1][ticker].lower()] = self.reserve_tokens[chain_2][ticker].lower()

    def cross_chain_paths(self, pools, max_path_len=MAX_PATH_LENGTH):
        # Start by getting all possible paths between our reserve tokens for each chain
        chain_reserve_paths = dict()
        for chain in self.chains:
            amm_list = list()
            for pool in pools:
                if pool["chain"] == chain:
                    amm_list.append(pool)
            chain_reserve_paths[chain] = self._reserve_paths(amm_list, max_path_len - 1, chain, cross_chain=True)

        candidate_paths = []
        
        for chain_1 in self.chains:
            
            for chain_1_start in self.reserve_tokens[chain_1].values():
                for chain_1_path in chain_reserve_paths[chain_1][chain_1_start.lower()]:

                    # Loop through chain 2 paths that start where the chain 1 ends
                    for chain_2 in self.chains:
                        if chain_1 != chain_2:
                            
                            if chain_1_path[-1]["token_out_address"].lower() in self.chain_to_chain_mapping[chain_1+chain_2]:

                                for chain_2_path in chain_reserve_paths[chain_2][self.chain_to_chain_mapping[chain_1+chain_2][chain_1_path[-1]["token_out_address"].lower()]]:
                                
                                    if chain_2_path[-1]["token_out_address"].lower() in self.chain_to_chain_mapping[chain_2+chain_1]:

                                        # Check that the chain 2 path end is equal to the chain 1 beginning
                                        if chain_1_start.lower() == self.chain_to_chain_mapping[chain_2+chain_1][chain_2_path[-1]["token_out_address"].lower()]:

                                            # Make sure the path is not longer than the set max
                                            if len(chain_2_path) + len(chain_1_path) <= max_path_len:
                                                candidate_paths.append(chain_2_path + chain_1_path)

        return candidate_paths
    
    def single_chain_paths(self, pools, max_path_len=MAX_PATH_LENGTH):
        # Start by getting all possible paths between our reserve tokens for each chain
        chain_reserve_paths = dict()
        for chain in self.chains:
            amm_list = list()
            for pool in pools:
                if pool["chain"] == chain:
                    amm_list.append(pool)
            chain_reserve_paths[chain] = self._reserve_paths(amm_list, max_path_len, chain, cross_chain=False)

        candidate_paths = []

        for chain in self.chains:
            for reserve_token in chain_reserve_paths[chain]:
                for path in chain_reserve_paths[chain][reserve_token]:
                    if path[0]["token_in_address"].lower() == path[-1]["token_out_address"].lower():
                        candidate_paths.append(path)

        return candidate_paths

    def _reserve_paths(self, amm_list, max_path_len, chain, cross_chain=True):
        reserve_tokens = self.reserve_tokens[chain]

        # We will store edges of each possible length - first we consider edges of length 1
        adjacency_list = [{}]

        # Construct adjacency list of AMM edges (we add references to each AMM
        # edge object in both directions to consider all possible paths)
        for amm in amm_list:
            edge = {"end": amm["token1"]["address"].lower(), "amm_list": [amm]}
            if amm["token0"]["address"].lower() in adjacency_list[0]:
                adjacency_list[0][amm["token0"]["address"].lower()].append(edge)
            else:
                adjacency_list[0][amm["token0"]["address"].lower()] = [edge]

            reverse_edge = {"end": amm["token0"]["address"].lower(), "amm_list": [amm]}
            if amm["token1"]["address"].lower() in adjacency_list[0]:
                adjacency_list[0][amm["token1"]["address"].lower()].append(reverse_edge)
            else:
                adjacency_list[0][amm["token1"]["address"].lower()] = [reverse_edge]

        # Remove all tokens that only appear once from adjacency list and aren't reserve tokens
        while True:
            del_keys = []

            for key in adjacency_list[0]:
                if len(adjacency_list[0][key]) == 1 and key not in reserve_tokens.values():
                    del_keys.append(key)

                    # Now we must delete the corresponding reference elsewhere in the adjacency_list
                    other_token = adjacency_list[0][key][0]["end"]

                    for entry in adjacency_list[0][other_token]:
                        if entry["end"] == key:
                            adjacency_list[0][other_token].remove(entry)

                if len(adjacency_list[0][key]) == 0:
                    del_keys.append(key)

            # Break loop once we find no more keys to delete
            if not del_keys:
                break

            for key in del_keys:
                del(adjacency_list[0][key])

        # Adjacency list containing the paths that go between the reserve tokens
        reserve_paths = {key: [] for key in reserve_tokens.values()}

        # Initially we must fill it with all simple edges that move between two reserve currencies
        for reserve_token in reserve_tokens.values():
            if reserve_token in adjacency_list[0]:
                for edge in adjacency_list[0][reserve_token]:
                    if edge["end"] in reserve_tokens.values():
                        reserve_paths[reserve_token].append(edge)

        # It will take two edges to move from one reserve token to another
        # reserve token. Path lengths longer than two can move through tokens
        # not included in our list of reserve tokens. Path lengths of three must
        # include edges where at least one token is a reserve token. Path lengths
        # of 4 or more meanwhile can contain edges where neither token is a reserve token

        for path_len in range(max_path_len - 1):
            adjacency_list.append({key: [] for key in adjacency_list[0]})

            for join_token in adjacency_list[path_len]:
                # Check if we can join each of our current longest edges
                for edge in adjacency_list[path_len][join_token]:

                    # A path must start and end with a reserve token, if this path
                    # is one less than the max path length then we will enforce
                    # that at least one token is a reserve token
                    if path_len + 2 >= max_path_len - 1 and edge["end"] not in reserve_tokens.values():
                        continue

                    # Check for joining with each of our smallest edges
                    for simple_edge in adjacency_list[0][join_token]:

                        # Similarly to above we are enforcing that the second token
                        # must also be a reserve token if the current path length is the max path length
                        if path_len + 2 >= max_path_len and simple_edge["end"] not in reserve_tokens.values():
                            continue

                        # Make sure that there is no overlap in included AMMS
                        # and that the new edge would not have the same endpoint
                        # This is only valid for cross-chain paths
                        if cross_chain:
                            if simple_edge["amm_list"][0] not in edge["amm_list"] and \
                                simple_edge["end"] != edge["end"]:

                                new_edge = {
                                    "end": edge["end"],
                                    "amm_list": simple_edge["amm_list"] + edge["amm_list"]
                                }

                                adjacency_list[path_len + 1][simple_edge["end"]].append(new_edge)

                                if edge["end"] in reserve_tokens.values() and simple_edge["end"] in reserve_tokens.values():
                                        reserve_paths[simple_edge["end"]].append(new_edge)
                        else:
                            if simple_edge["amm_list"][0] not in edge["amm_list"]:
                                new_edge = {
                                    "end": edge["end"],
                                    "amm_list": simple_edge["amm_list"] + edge["amm_list"]
                                }

                                adjacency_list[path_len + 1][simple_edge["end"]].append(new_edge)

                                if edge["end"] in reserve_tokens.values() and simple_edge["end"] in reserve_tokens.values():
                                        reserve_paths[simple_edge["end"]].append(new_edge)


        reserve_path_formatted = {key: [] for key in reserve_paths}

        for key in reserve_paths:
            for edge in reserve_paths[key]:
                curr_token = key

                reserve_path_formatted[key].append([])
                for amm in edge["amm_list"]:

                    reserve_path_formatted[key][-1].append({})
                    reserve_path_formatted[key][-1][-1]["chain"] = chain
                    reserve_path_formatted[key][-1][-1]["address"] = amm["address"]
                    reserve_path_formatted[key][-1][-1]["protocol"] = amm["protocol"]
                    reserve_path_formatted[key][-1][-1]["version"] = amm["version"]

                    if amm["token0"]["address"] == curr_token:
                        reserve_path_formatted[key][-1][-1]["token_in_symbol"] = amm["token0"]["symbol"]
                        reserve_path_formatted[key][-1][-1]["token_out_symbol"] = amm["token1"]["symbol"]
                        reserve_path_formatted[key][-1][-1]["token_in_address"] = amm["token0"]["address"]
                        reserve_path_formatted[key][-1][-1]["token_out_address"] = amm["token1"]["address"]
                        reserve_path_formatted[key][-1][-1]["token_in_decimals"] = amm["token0"]["decimals"]
                        reserve_path_formatted[key][-1][-1]["token_out_decimals"] = amm["token1"]["decimals"]
                        if "feeTier" in amm:
                            reserve_path_formatted[key][-1][-1]["fee_tier"] = amm["feeTier"]
                        reserve_path_formatted[key][-1][-1]["zero_for_one"] = True

                        curr_token = amm["token1"]["address"]
                        
                    elif amm["token1"]["address"] == curr_token:
                        reserve_path_formatted[key][-1][-1]["token_in_symbol"] = amm["token1"]["symbol"]
                        reserve_path_formatted[key][-1][-1]["token_out_symbol"] = amm["token0"]["symbol"]
                        reserve_path_formatted[key][-1][-1]["token_in_address"] = amm["token1"]["address"]
                        reserve_path_formatted[key][-1][-1]["token_out_address"] = amm["token0"]["address"]
                        reserve_path_formatted[key][-1][-1]["token_in_decimals"] = amm["token1"]["decimals"]
                        reserve_path_formatted[key][-1][-1]["token_out_decimals"] = amm["token0"]["decimals"]
                        if "feeTier" in amm:
                            reserve_path_formatted[key][-1][-1]["fee_tier"] = amm["feeTier"]
                        reserve_path_formatted[key][-1][-1]["zero_for_one"] = False

                        curr_token = amm["token0"]["address"]
                        
                    else:
                        raise Exception("Can not format invalid path!")

        return reserve_path_formatted


if __name__ == "__main__":
    pools = dict()
    with open("/Users/christof/TLDR/Project/data/pools.json", "r") as f:
        pools = json.load(f)
    print("Number of Uniswap V2 pools:", colors.INFO, len([pool for pool in pools if pool["protocol"] == "uniswap" and pool["version"] == "v2"]), colors.END)
    print("Number of Uniswap V3 pools:", colors.INFO, len([pool for pool in pools if pool["protocol"] == "uniswap" and pool["version"] == "v3"]), colors.END)
    print()
    
    reserve_token_addresses = dict()
    with open("/Users/christof/TLDR/Project/data/reserve_token_addresses.json", "r") as f:
        reserve_token_addresses = json.load(f)

    print("Constructing paths...")
    builder = path_builder(reserve_token_addresses)
    
    print()
    start = time.time()
    cross_chain_paths = builder.cross_chain_paths(pools, MAX_PATH_LENGTH)
    print("Cross-Chain path construction took:", colors.INFO, time.time() - start, colors.END, "second(s).")
    print("Number of cross-chain paths:", colors.INFO, len(cross_chain_paths), colors.END)

    with open("/Users/christof/TLDR/Project/data/cross_chain_paths.json", "w") as f:
        json.dump(cross_chain_paths, f, indent=2)

    print()
    start = time.time()
    single_chain_paths = builder.single_chain_paths(pools, MAX_PATH_LENGTH)
    print("Single-Chain path construction took:", colors.INFO, time.time() - start, colors.END, "second(s).")
    print("Number of single-chain paths:", colors.INFO, len(single_chain_paths), colors.END)

    with open("/Users/christof/TLDR/Project/data/single_chain_paths.json", "w") as f:
        json.dump(single_chain_paths, f, indent=2)
   
    """print("Analyzing Arbitrum arbitrages up to a swap length of", MAX_PATH_LENGTH)
    with open("../data/arbitrum/sorted_arbitrage_arbitrum_swaps.json", "r") as f:
        sorted_arbitrage_arbitrum_swaps = json.load(f)
        total_arbitrages = 0
        max_length_arbitrages = 0
        for swap_length in sorted_arbitrage_arbitrum_swaps:
            total_arbitrages += swap_length[1]
            if swap_length[0] <= MAX_PATH_LENGTH:
                max_length_arbitrages += swap_length[1]
        print("{0:.2f}%".format(max_length_arbitrages / total_arbitrages * 100), "of arbitrages on Arbitrum are up to a swap length of", MAX_PATH_LENGTH)
    print()

    print("Analyzing Optimism arbitrages up to a swap length of", MAX_PATH_LENGTH)
    with open("../data/optimism/sorted_arbitrage_optimism_swaps.json", "r") as f:
        sorted_arbitrage_optimism_swaps = json.load(f)
        total_arbitrages = 0
        max_length_arbitrages = 0
        for swap_length in sorted_arbitrage_optimism_swaps:
            total_arbitrages += swap_length[1]
            if swap_length[0] <= MAX_PATH_LENGTH:
                max_length_arbitrages += swap_length[1]
        print("{0:.2f}%".format(max_length_arbitrages / total_arbitrages * 100), "of arbitrages on Optimism are up to a swap length of", MAX_PATH_LENGTH)
    print()
        
    print("Loading reserve tokens for Arbitrum...")
    arbitrum_reserve_tokens = dict()
    with open("../data/arbitrum/arbitrum_reserve_tokens.json", "r") as f:
        arbitrum_reserve_tokens = json.load(f)
    pprint.pprint(arbitrum_reserve_tokens)
    print()

    print("Analyzing number of Arbitrum arbitrages for chosen reserve tokens...")
    with open("../data/arbitrum/sorted_arbitrage_arbitrum_inital_tokens.json", "r") as f:
        sorted_arbitrage_arbitrum_inital_tokens = json.load(f)
        total_arbitrages = 0
        reserve_token_arbitrages = 0
        for initial_token in sorted_arbitrage_arbitrum_inital_tokens:
            total_arbitrages += initial_token[1]
            if initial_token[0].split(" ")[0].lower() in [value.lower() for value in list(arbitrum_reserve_tokens.values())]:
                reserve_token_arbitrages += initial_token[1]
        print("{0:.2f}%".format(reserve_token_arbitrages / total_arbitrages * 100), "of arbitrages on Arbitrum using chosen reserve tokens.")
    print()

    print("Loading reserve tokens for Optimism...")
    optimism_reserve_tokens = dict()
    with open("../data/optimism/optimism_reserve_tokens.json", "r") as f:
        optimism_reserve_tokens = json.load(f)
    pprint.pprint(optimism_reserve_tokens)
    print()

    print("Analyzing number of Optimism arbitrages for chosen reserve tokens...")
    with open("../data/optimism/sorted_arbitrage_optimism_inital_tokens.json", "r") as f:
        sorted_arbitrage_optimism_inital_tokens = json.load(f)
        total_arbitrages = 0
        reserve_token_arbitrages = 0
        for initial_token in sorted_arbitrage_optimism_inital_tokens:
            total_arbitrages += initial_token[1]
            if initial_token[0].split(" ")[0].lower() in [value.lower() for value in list(optimism_reserve_tokens.values())]:
                reserve_token_arbitrages += initial_token[1]
        print("{0:.2f}%".format(reserve_token_arbitrages / total_arbitrages * 100), "of arbitrages on Optimism using chosen reserve tokens.")
    print()

    print("Loading Uniswap V3 pools for Arbitrum...")
    arbitrum_reserve_pools = list()
    with open("../data/arbitrum/uniswap_v3_pools_arbitrum.json", "r") as f:
        pools = json.load(f)
        for pool_address in pools:
            if pools[pool_address]["token0"] in list(arbitrum_reserve_tokens.values()) or pools[pool_address]["token1"] in list(arbitrum_reserve_tokens.values()):
                pools[pool_address]["id"] = pool_address.lower()
                pools[pool_address]["variation"] = "uniswap_v3"
                pools[pool_address]["token0"] = pools[pool_address]["token0"].lower()
                pools[pool_address]["token1"] = pools[pool_address]["token1"].lower()
                del pools[pool_address]["block_number"]
                del pools[pool_address]["transaction_hash"]
                arbitrum_reserve_pools.append(pools[pool_address])
    print("Loaded", len(arbitrum_reserve_pools), "pools.")
    print()

    print("Reducing down arbitrage pools for Arbitrum based on popular past arbitrage pools...")
    chain_1_amm_list = list()
    with open("../data/arbitrum/sorted_arbitrage_arbitrum_pools.json", "r") as f:
        sorted_arbitrage_arbitrum_pools = json.load(f)
        popular_arbitrage_pools = list()
        for arbitrage_pool in sorted_arbitrage_arbitrum_pools:
            popular_arbitrage_pools.append(arbitrage_pool[0].split(" ")[0].lower())
        for pool in arbitrum_reserve_pools:
            if pool["id"].lower() in popular_arbitrage_pools:
                chain_1_amm_list.append(pool)
    print("Reduced down to", len(chain_1_amm_list), "pools.")
    print()

    print("Loading Uniswap V3 pools for Optimism...")
    optimism_reserve_pools = list()
    with open("../data/optimism/uniswap_v3_pools_optimism.json", "r") as f:
        pools = json.load(f)
        for pool_address in pools:
            if pools[pool_address]["token0"] in list(optimism_reserve_tokens.values()) or pools[pool_address]["token1"] in list(optimism_reserve_tokens.values()):
                pools[pool_address]["id"] = pool_address.lower()
                pools[pool_address]["variation"] = "uniswap_v3"
                pools[pool_address]["token0"] = pools[pool_address]["token0"].lower()
                pools[pool_address]["token1"] = pools[pool_address]["token1"].lower()
                del pools[pool_address]["block_number"]
                del pools[pool_address]["transaction_hash"]
                optimism_reserve_pools.append(pools[pool_address])
    print("Loaded", len(optimism_reserve_pools), "pools.")
    print()

    print("Reducing down arbitrage pools for Optimism based on popular past arbitrage pools...")
    chain_2_amm_list = list()
    with open("../data/optimism/sorted_arbitrage_optimism_pools.json", "r") as f:
        sorted_arbitrage_optimism_pools = json.load(f)
        popular_arbitrage_pools = list()
        for arbitrage_pool in sorted_arbitrage_optimism_pools:
            popular_arbitrage_pools.append(arbitrage_pool[0].split(" ")[0].lower())
        for pool in optimism_reserve_pools:
            if pool["id"].lower() in popular_arbitrage_pools:
                chain_2_amm_list.append(pool)
    print("Reduced down to", len(chain_2_amm_list), "pools.")
    print()

    print("Constructing paths..")
    builder = path_builder(arbitrum_reserve_tokens, optimism_reserve_tokens)
    start = time.time()
    paths = builder.cross_chain_paths(chain_1_amm_list[:100], chain_2_amm_list[:100], MAX_PATH_LENGTH)
    print("Path construction took:", time.time()-start)
    print("Number of paths:", len(paths))

    #pprint.pprint(paths)"""
    
