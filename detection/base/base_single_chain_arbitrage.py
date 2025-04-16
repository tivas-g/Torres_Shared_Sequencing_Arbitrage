#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import numpy
import decimal
import hashlib
import pymongo
import traceback
import multiprocessing

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..'))

from utils.utils import colors, to_signed_256, get_events, get_price_from_timestamp
from utils.settings import *

CPUs = 1 #multiprocessing.cpu_count()

BLOCK_RANGE = 100

DEBUG_MODE = False

# Decentralized Exchanges
UNISWAP_V2 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822" # UNISWAP V2 (Swap)
UNISWAP_V3 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67" # UNISWAP V3 (Swap)

ETH  = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
WETH = "0x4200000000000000000000000000000000000006"

def analyze_block(block_range):
    start = time.time()
    print("Analyzing block range: "+colors.INFO+str(block_range[0])+"-"+str(block_range[1])+colors.END)

    # Get all the events at once and order them by block
    events_per_block = dict()
    try:
        events = list()
        events += get_events(w3, client_version, {"fromBlock": block_range[0], "toBlock": block_range[1], "topics": [UNISWAP_V2]},  BASE_PROVIDER, "base")
        events += get_events(w3, client_version, {"fromBlock": block_range[0], "toBlock": block_range[1], "topics": [UNISWAP_V3]},  BASE_PROVIDER, "base")
        for i in range(block_range[0], block_range[1]+1):
            events_per_block[i] = list()
        for event in events:
            events_per_block[event["blockNumber"]].append(event)
    except Exception as e:
        print(colors.FAIL+str(traceback.format_exc())+colors.END)
        print(colors.FAIL+"Error: "+str(e)+" @ block range: "+str(block_range[0])+"-"+str(block_range[1])+colors.END)
        end = time.time()
        return end - start

    execution_time = 0
    for block_number in events_per_block:
        status = mongo_connection["cross_chain_arbitrage"]["detected_single_chain_arbitrage_base_status"].find_one({"block_number": block_number})
        if status and not DEBUG_MODE:
            print("Block "+colors.INFO+str(block_number)+colors.END+" already analyzed!")
            execution_time += status["execution_time"]
            continue

        swaps = dict()
        transaction_index_to_hash = dict()

        events = events_per_block[block_number]
        try:
            for event in events:
                # Search for Uniswap V2 swaps
                if event["topics"][0].lower() in UNISWAP_V2.lower():
                    if not event["transactionIndex"] in transaction_index_to_hash:
                        transaction_index_to_hash[event["transactionIndex"]] = event["transactionHash"]
                    if not event["transactionIndex"] in swaps:
                        swaps[event["transactionIndex"]] = list()
                    _amount0In  = int(event["data"].replace("0x", "")[0:64], 16)
                    _amount1In  = int(event["data"].replace("0x", "")[64:128], 16)
                    _amount0Out = int(event["data"].replace("0x", "")[128:192], 16)
                    _amount1Out = int(event["data"].replace("0x", "")[192:256], 16)
                    exchange_contract = w3.eth.contract(address=event["address"], abi=[
                        {"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},
                        {"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}
                    ])
                    if not event["address"]+":token0" in cache:
                        try:
                            _token0 = exchange_contract.functions.token0().call()
                            cache[event["address"]+":token0"] = _token0
                        except:
                            _token0 = None
                            cache[event["address"]+":token0"] = _token0
                    _token0 = cache[event["address"]+":token0"]
                    if not event["address"]+":token1" in cache:
                        try:
                            _token1 = exchange_contract.functions.token1().call()
                            cache[event["address"]+":token1"] = _token1
                        except:
                            _token1 = None
                            cache[event["address"]+":token1"] = _token1
                    _token1 = cache[event["address"]+":token1"]
                    if _token0 == None or _token1 == None:
                        continue
                    if _amount0In == 0 and _amount1Out == 0:
                        amount_in  = _amount1In
                        amount_out = _amount0Out
                        in_token   = _token1
                        out_token  = _token0
                    elif _amount1In == 0 and _amount0Out == 0:
                        amount_in  = _amount0In
                        amount_out = _amount1Out
                        in_token   = _token0
                        out_token  = _token1
                    else:
                        continue
                    if not in_token+":name" in cache:
                        try:
                            token_contract = w3.eth.contract(address=in_token, abi=[{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"type":"function"}])
                            in_token_name = token_contract.functions.name().call()
                            cache[in_token+":name"] = in_token_name
                        except:
                            try:
                                token_contract = w3.eth.contract(address=in_token, abi=[{"name": "name", "outputs": [{"type": "bytes32", "name": "out"}], "inputs": [], "type": "function"}])
                                in_token_name = token_contract.functions.name().call().decode("utf-8").replace(u"\u0000", "")
                                cache[in_token+":name"] = in_token_name
                            except:
                                in_token_name = in_token
                                cache[in_token+":name"] = in_token_name
                    if not out_token+":name" in cache:
                        try:
                            token_contract = w3.eth.contract(address=out_token, abi=[{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"type":"function"}])
                            out_token_name = token_contract.functions.name().call()
                            cache[out_token+":name"] = out_token_name
                        except:
                            try:
                                token_contract = w3.eth.contract(address=out_token, abi=[{"name": "name", "outputs": [{"type": "bytes32", "name": "out"}], "inputs": [], "type": "function"}])
                                out_token_name = token_contract.functions.name().call().decode("utf-8").replace(u"\u0000", "")
                                cache[out_token+":name"] = out_token_name
                            except:
                                out_token_name = out_token
                                cache[out_token+":name"] = out_token_name
                    in_token_name = cache[in_token+":name"].replace(".", " ").replace("$", "")
                    out_token_name = cache[out_token+":name"].replace(".", " ").replace("$", "")
                    swaps[event["transactionIndex"]].append({"index": event["logIndex"], "in_token": in_token, "in_token_name": in_token_name, "out_token": out_token, "out_token_name": out_token_name, "in_amount": amount_in, "out_amount": amount_out, "exchange": event["address"], "protocol_name": "Uniswap V2"})
                    swaps[event["transactionIndex"]] = sorted(swaps[event["transactionIndex"]], key=lambda d: d["index"])

                # Search for Uniswap V3 swaps
                elif event["topics"][0].lower() in UNISWAP_V3.lower():
                    if not event["transactionIndex"] in transaction_index_to_hash:
                        transaction_index_to_hash[event["transactionIndex"]] = event["transactionHash"]
                    if not event["transactionIndex"] in swaps:
                        swaps[event["transactionIndex"]] = list()
                    _amount0 = to_signed_256(int(event["data"].replace("0x", "")[0:64], 16))
                    _amount1 = to_signed_256(int(event["data"].replace("0x", "")[64:128], 16))
                    exchange_contract = w3.eth.contract(address=event["address"], abi=[
                        {"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},
                        {"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}
                    ])
                    if not event["address"]+":token0" in cache:
                        _token0 = exchange_contract.functions.token0().call()
                        cache[event["address"]+":token0"] = _token0
                    _token0 = cache[event["address"]+":token0"]
                    if not event["address"]+":token1" in cache:
                        _token1 = exchange_contract.functions.token1().call()
                        cache[event["address"]+":token1"] = _token1
                    _token1 = cache[event["address"]+":token1"]
                    if _amount0 < 0:
                        amount_in  = _amount1
                        amount_out = abs(_amount0)
                        in_token   = _token1
                        out_token  = _token0
                    else:
                        amount_in  = _amount0
                        amount_out = abs(_amount1)
                        in_token   = _token0
                        out_token  = _token1
                    if not in_token+":name" in cache:
                        try:
                            token_contract = w3.eth.contract(address=in_token, abi=[{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"type":"function"}])
                            in_token_name = token_contract.functions.name().call()
                            cache[in_token+":name"] = in_token_name
                        except:
                            try:
                                token_contract = w3.eth.contract(address=in_token, abi=[{"name": "name", "outputs": [{"type": "bytes32", "name": "out"}], "inputs": [], "type": "function"}])
                                in_token_name = token_contract.functions.name().call().decode("utf-8").replace(u"\u0000", "")
                                cache[in_token+":name"] = in_token_name
                            except:
                                in_token_name = in_token
                                cache[in_token+":name"] = in_token_name
                    if not out_token+":name" in cache:
                        try:
                            token_contract = w3.eth.contract(address=out_token, abi=[{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"type":"function"}])
                            out_token_name = token_contract.functions.name().call()
                            cache[out_token+":name"] = out_token_name
                        except:
                            try:
                                token_contract = w3.eth.contract(address=out_token, abi=[{"name": "name", "outputs": [{"type": "bytes32", "name": "out"}], "inputs": [], "type": "function"}])
                                out_token_name = token_contract.functions.name().call().decode("utf-8").replace(u"\u0000", "")
                                cache[out_token+":name"] = out_token_name
                            except:
                                out_token_name = out_token
                                cache[out_token+":name"] = out_token_name
                    in_token_name = cache[in_token+":name"].replace(".", " ").replace("$", "")
                    out_token_name = cache[out_token+":name"].replace(".", " ").replace("$", "")
                    swaps[event["transactionIndex"]].append({"index": event["logIndex"], "in_token": in_token, "in_token_name": in_token_name, "out_token": out_token, "out_token_name": out_token_name, "in_amount": amount_in, "out_amount": amount_out, "exchange": event["address"], "protocol_name": "Uniswap V3"})
                    swaps[event["transactionIndex"]] = sorted(swaps[event["transactionIndex"]], key=lambda d: d["index"])

        except Exception as e:
            print(colors.FAIL+traceback.format_exc()+colors.END)
            print(colors.FAIL+"Error: "+str(e)+" @ block number: "+str(block_number)+colors.END)
            end = time.time()
            return end - start
        
        block = w3.eth.get_block(block_number)
        one_eth_to_usd_price = decimal.Decimal(float(get_price_from_timestamp("base", WETH, block["timestamp"], ALLIUM_API_KEY, cache)))

        try:
            # Search for arbitrage
            for tx_index in swaps:
                arbitrages = list()
                if len(swaps[tx_index]) > 1:
                    if swaps[tx_index][0]["in_amount"]  <= swaps[tx_index][-1]["out_amount"] and \
                       swaps[tx_index][0]["in_token"]   != "" and \
                       swaps[tx_index][-1]["out_token"] != "" and \
                       (swaps[tx_index][0]["in_token"]  == swaps[tx_index][-1]["out_token"] or (swaps[tx_index][0]["in_token"] in [ETH, WETH] and swaps[tx_index][-1]["out_token"] in [ETH, WETH])):
                        valid = True
                        intermediary_swaps = list()
                        intermediary_swaps.append(swaps[tx_index][0])
                        gains = dict()
                        for i in range(1, len(swaps[tx_index])):
                            previous_swap = swaps[tx_index][i-1]
                            current_swap = swaps[tx_index][i]
                            intermediary_swaps.append(current_swap)
                            if previous_swap["out_token"] != current_swap["in_token"]:
                                valid = False
                            if previous_swap["out_amount"] < current_swap["in_amount"]:
                                valid = False
                            if previous_swap["exchange"] == current_swap["exchange"]:
                                valid = False
                            if  valid and (swaps[tx_index][0]["in_token"] == current_swap["out_token"] or (swaps[tx_index][0]["in_token"] in [ETH, WETH] and current_swap["out_token"] in [ETH, WETH])):
                                print()
                                print(colors.FAIL+"Arbitrage detected: "+colors.INFO+transaction_index_to_hash[tx_index]+" ("+str(block_number)+")"+colors.END)

                                intermediary_gains = dict()
                                for swap in intermediary_swaps:
                                    if not swap["in_token"] in intermediary_gains:
                                        # Decimals
                                        decimals = None
                                        if swap["in_token"] == ETH:
                                            decimals = 18
                                        else:
                                            if not swap["in_token"]+":decimals" in cache:
                                                try:
                                                    token_contract = w3.eth.contract(address=swap["in_token"], abi=[{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"type":"function"}])
                                                    decimals = token_contract.functions.decimals().call()
                                                    cache[swap["in_token"]+":decimals"] = decimals
                                                except:
                                                    decimals = None
                                                    cache[swap["in_token"]+":decimals"] = decimals
                                            decimals = cache[swap["in_token"]+":decimals"]
                                        # Token price
                                        one_token_to_usd_price = None
                                        if swap["in_token"] == ETH:
                                            one_token_to_usd_price = one_eth_to_usd_price
                                        else:
                                            try:
                                                one_token_to_usd_price = decimal.Decimal(float(get_price_from_timestamp("base", swap["in_token"], block["timestamp"], ALLIUM_API_KEY, cache)))
                                            except:
                                                one_token_to_usd_price = None
                                        intermediary_gains[swap["in_token"]] = {"token_name": swap["in_token_name"], "amount": 0, "decimals": decimals, "one_token_to_usd_price": one_token_to_usd_price}
                                    intermediary_gains[swap["in_token"]]["amount"] -= swap["in_amount"]

                                    if not swap["out_token"] in intermediary_gains:
                                        # Decimals
                                        decimals = None
                                        if swap["out_token"] == ETH:
                                            decimals = 18
                                        else:
                                            if not swap["out_token"]+":decimals" in cache:
                                                try:
                                                    token_contract = w3.eth.contract(address=swap["out_token"], abi=[{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"type":"function"}])
                                                    decimals = token_contract.functions.decimals().call()
                                                    cache[swap["out_token"]+":decimals"] = decimals
                                                except:
                                                    out_token_decimals = None
                                                    cache[swap["out_token"]+":decimals"] = out_token_decimals
                                            decimals = cache[swap["out_token"]+":decimals"]
                                        # Token price
                                        one_token_to_usd_price = None
                                        if swap["out_token"] == ETH:
                                            one_token_to_usd_price = one_eth_to_usd_price
                                        else:
                                            try:
                                                one_token_to_usd_price = decimal.Decimal(float(get_price_from_timestamp("base", swap["out_token"], block["timestamp"], ALLIUM_API_KEY, cache)))
                                            except:
                                                one_token_to_usd_price = None
                                        intermediary_gains[swap["out_token"]] = {"token_name": swap["out_token_name"], "amount": 0, "decimals": decimals, "one_token_to_usd_price": one_token_to_usd_price}
                                    intermediary_gains[swap["out_token"]]["amount"] += swap["out_amount"]

                                    in_token_decimals = 0
                                    if intermediary_gains[swap["in_token"]]["decimals"]:
                                        in_token_decimals = intermediary_gains[swap["in_token"]]["decimals"]
                                    out_token_decimals = 0
                                    if intermediary_gains[swap["out_token"]]["decimals"]:
                                        out_token_decimals = intermediary_gains[swap["out_token"]]["decimals"]
                                    print(colors.INFO+"Swap"+colors.END, decimal.Decimal(swap["in_amount"]) / 10**in_token_decimals, swap["in_token_name"], colors.INFO+"For"+colors.END, decimal.Decimal(swap["out_amount"]) / 10**out_token_decimals, swap["out_token_name"], colors.INFO+"On"+colors.END, swap["protocol_name"], colors.INFO+"("+swap["exchange"].lower()+")"+colors.END)

                                arbitrage_cost_usd = decimal.Decimal(0)
                                arbitrage_gain_usd = decimal.Decimal(0)
                                for token in intermediary_gains:
                                    if intermediary_gains[token]["amount"] < 0:
                                        if arbitrage_cost_usd != None and intermediary_gains[token]["decimals"] != None and intermediary_gains[token]["one_token_to_usd_price"] != None:
                                            arbitrage_cost_usd += decimal.Decimal(abs(intermediary_gains[token]["amount"])) / 10**intermediary_gains[token]["decimals"] * intermediary_gains[token]["one_token_to_usd_price"]
                                        else:
                                            arbitrage_cost_usd = None
                                    if intermediary_gains[token]["amount"] > 0:
                                        if arbitrage_gain_usd != None and intermediary_gains[token]["decimals"] != None and intermediary_gains[token]["one_token_to_usd_price"] != None:
                                            arbitrage_gain_usd += decimal.Decimal(intermediary_gains[token]["amount"]) / 10**intermediary_gains[token]["decimals"] * intermediary_gains[token]["one_token_to_usd_price"]
                                        else:
                                            arbitrage_gain_usd = None

                                arbitrage = dict()
                                arbitrage["swaps"] = intermediary_swaps
                                arbitrage["token_balance"] = intermediary_gains
                                arbitrage["cost_eth"] = arbitrage_cost_usd / one_eth_to_usd_price if arbitrage_cost_usd != None else None
                                arbitrage["cost_usd"] = arbitrage_cost_usd 
                                arbitrage["gain_eth"] = arbitrage_gain_usd / one_eth_to_usd_price if arbitrage_gain_usd != None else None
                                arbitrage["gain_usd"] = arbitrage_gain_usd 
                                arbitrage["profit_usd"] = arbitrage_gain_usd - arbitrage_cost_usd if arbitrage_gain_usd != None and arbitrage_cost_usd != None else None 
                                arbitrage["profit_eth"] = arbitrage["profit_usd"] / one_eth_to_usd_price if arbitrage["profit_usd"] != None else None
                                
                                if arbitrage["cost_eth"] != None:
                                    print("Cost: "+str(float(arbitrage["cost_eth"]))+" ETH ("+str(float(arbitrage["cost_usd"]))+" USD)")
                                else:
                                    print("Cost: "+str(None)+" ETH ("+str(None)+" USD)")

                                if arbitrage["gain_eth"] != None:
                                    print("Gain: "+str(float(arbitrage["gain_eth"]))+" ETH ("+str(float(arbitrage["gain_usd"]))+" USD)")
                                else:
                                    print("Gain: "+str(None)+" ETH ("+str(None)+" USD)")

                                if arbitrage["profit_eth"] != None:
                                    if arbitrage["profit_eth"] >= 0:
                                        print(colors.OK+"Profit: "+str(float(arbitrage["profit_eth"]))+" ETH ("+str(float(arbitrage["profit_usd"]))+" USD)"+colors.END)
                                    else:
                                        print(colors.FAIL+"Profit: "+str(float(arbitrage["profit_eth"]))+" ETH ("+str(float(arbitrage["profit_usd"]))+" USD)"+colors.END)
                                else:
                                    print("Profit: "+str(None)+" ETH ("+str(None)+" USD)")

                                arbitrages.append(arbitrage)
                                intermediary_swaps = list()
                        if valid:
                            print()

                            # Compute transaction cost
                            tx = w3.eth.get_transaction(transaction_index_to_hash[tx_index])
                            receipt = w3.eth.get_transaction_receipt(tx["hash"])
                            tx_cost = Web3.from_wei(receipt["gasUsed"] * receipt["effectiveGasPrice"], "ether")
                            if "l1FeeScalar" in receipt:
                                tx_cost = tx_cost + Web3.from_wei(int(receipt["l1GasUsed"], 16) * int(receipt["l1GasPrice"], 16) * float(receipt["l1FeeScalar"]), "ether")
                            elif receipt["l1Fee"] != None:
                                tx_cost = tx_cost + Web3.from_wei(int(receipt["l1Fee"], 16), "ether")                        
                            if tx_cost != 0:
                                total_cost_eth = tx_cost
                                total_cost_usd = tx_cost * one_eth_to_usd_price
                            else:
                                total_cost_eth = 0
                                total_cost_usd = 0

                            # Compute cost and gain
                            print("Token balance:")
                            total_gain_eth = 0
                            total_gain_usd = 0
                            total_token_balance = dict()
                            for arbitrage in arbitrages:
                                for token in arbitrage["token_balance"]:
                                    if not token in total_token_balance:
                                        total_token_balance[token] = {"amount": 0, "decimals": arbitrage["token_balance"][token]["decimals"], "one_token_to_usd_price": arbitrage["token_balance"][token]["one_token_to_usd_price"], "token_name": arbitrage["token_balance"][token]["token_name"]}
                                    total_token_balance[token]["amount"] += arbitrage["token_balance"][token]["amount"]
                            for token in total_token_balance:
                                if total_token_balance[token]["decimals"] != None and total_token_balance[token]["one_token_to_usd_price"] != None:
                                    amount_usd = decimal.Decimal(total_token_balance[token]["amount"]) / 10**total_token_balance[token]["decimals"] * total_token_balance[token]["one_token_to_usd_price"]
                                    amount_eth = amount_usd / one_eth_to_usd_price
                                    if amount_eth >= 0:
                                        if total_gain_eth != None:
                                            total_gain_eth += amount_eth
                                            total_gain_usd += amount_usd
                                    else:
                                        if total_cost_eth != None:
                                            total_cost_eth += abs(amount_eth)
                                            total_cost_usd += abs(amount_usd)
                                    print("  "+colors.INFO+total_token_balance[token]["token_name"]+": "+colors.END+str(float(amount_eth))+" ETH ("+str(float(amount_usd))+" USD)")
                                else:
                                    if total_token_balance[token]["amount"] != 0:
                                        total_gain_eth = None
                                        total_gain_usd = None
                                    print("  "+colors.INFO+total_token_balance[token]["token_name"]+": "+colors.END+str(None)+" ETH ("+str(None)+" USD)")
                            print()

                            # Compute total profit
                            if total_gain_eth != None and total_cost_eth != None:
                                total_profit_eth = total_gain_eth - total_cost_eth
                                total_profit_usd = total_profit_eth * one_eth_to_usd_price
                            else:
                                total_profit_eth = None
                                total_profit_usd = None

                            print("Transaction cost: "+str(float(tx_cost))+" ETH ("+str(float(tx_cost * one_eth_to_usd_price))+" USD)")

                            if total_cost_eth != None:
                                print("Total cost: "+str(float(total_cost_eth))+" ETH ("+str(float(total_cost_usd))+" USD)")
                            else:
                                print("Total cost: "+str(None)+" ETH ("+str(None)+" USD)")

                            if total_gain_eth != None:
                                print("Total gain: "+str(float(total_gain_eth))+" ETH ("+str(float(total_gain_usd))+" USD)")
                            else:
                                print("Total gain: "+str(None)+" ETH ("+str(None)+" USD)")

                            if total_profit_eth != None:
                                if total_profit_eth >= 0:
                                    print(colors.OK+"Total profit: "+str(float(total_profit_eth))+" ETH ("+str(float(total_profit_usd))+" USD)"+colors.END)
                                else:
                                    print(colors.FAIL+"Total profit: "+str(float(total_profit_eth))+" ETH ("+str(float(total_profit_usd))+" USD)"+colors.END)
                            else:
                                print("Total profit: "+str(None)+" ETH ("+str(None)+" USD)")

                            tx = dict(tx)
                            del tx["blockNumber"]
                            del tx["blockHash"]
                            del tx["r"]
                            del tx["s"]
                            del tx["v"]
                            tx["value"] = str(tx["value"])
                            tx["input"] = tx["input"].hex()
                            tx["hash"] = tx["hash"].hex()

                            for i in range(len(arbitrages)):
                                for j in range(len(arbitrages[i]["swaps"])):
                                    arbitrages[i]["swaps"][j]["in_amount"] = str(arbitrages[i]["swaps"][j]["in_amount"])
                                    arbitrages[i]["swaps"][j]["out_amount"] = str(arbitrages[i]["swaps"][j]["out_amount"])
                                    arbitrages[i]["swaps"][j]["out_token_name"] = ''.join(arbitrages[i]["swaps"][j]["out_token_name"].split('\x00'))
                                    arbitrages[i]["swaps"][j]["in_token_name"] = ''.join(arbitrages[i]["swaps"][j]["in_token_name"].split('\x00'))
                                for j in arbitrages[i]["token_balance"]:
                                    arbitrages[i]["token_balance"][j]["amount"] = str(arbitrages[i]["token_balance"][j]["amount"])
                                    arbitrages[i]["token_balance"][j]["one_token_to_usd_price"] = float(arbitrages[i]["token_balance"][j]["one_token_to_usd_price"]) if arbitrages[i]["token_balance"][j]["one_token_to_usd_price"] != None else arbitrages[i]["token_balance"][j]["one_token_to_usd_price"]
                                    arbitrages[i]["token_balance"][j]["token_name"] = ''.join(arbitrages[i]["token_balance"][j]["token_name"].split('\x00'))
                                arbitrages[i]["cost_eth"] = float(arbitrages[i]["cost_eth"]) if arbitrages[i]["cost_eth"] != None else None
                                arbitrages[i]["cost_usd"] = float(arbitrages[i]["cost_usd"]) if arbitrages[i]["cost_usd"] != None else None
                                arbitrages[i]["gain_eth"] = float(arbitrages[i]["gain_eth"]) if arbitrages[i]["gain_eth"] != None else None
                                arbitrages[i]["gain_usd"] = float(arbitrages[i]["gain_usd"]) if arbitrages[i]["gain_usd"] != None else None
                                arbitrages[i]["profit_eth"] = float(arbitrages[i]["profit_eth"]) if arbitrages[i]["profit_eth"] != None else None
                                arbitrages[i]["profit_usd"] = float(arbitrages[i]["profit_usd"]) if arbitrages[i]["profit_usd"] != None else None

                            for i in total_token_balance:
                                total_token_balance[i]["amount"] = str(total_token_balance[i]["amount"])
                                total_token_balance[i]["one_token_to_usd_price"] = float(total_token_balance[i]["one_token_to_usd_price"]) if total_token_balance[i]["one_token_to_usd_price"] != None else total_token_balance[i]["one_token_to_usd_price"]
                                total_token_balance[i]["token_name"] = ''.join(total_token_balance[i]["token_name"].split('\x00'))

                            h = hashlib.sha256()
                            h.update(str(str(block["number"])+":"+str(tx["transactionIndex"])).encode('utf-8'))

                            finding = {
                                "id": h.hexdigest(),
                                "block_number": block_number,
                                "block_timestamp": block["timestamp"],
                                "miner": block["miner"],
                                "transaction": tx,
                                "arbitrages": arbitrages,
                                "token_balance": total_token_balance,
                                "eth_usd_price": float(one_eth_to_usd_price),
                                "total_cost_eth": float(total_cost_eth) if total_cost_eth != None else None,
                                "total_cost_usd": float(total_cost_usd) if total_cost_usd != None else None,
                                "total_gain_eth": float(total_gain_eth) if total_gain_eth != None else None,
                                "total_gain_usd": float(total_gain_usd) if total_gain_usd != None else None,
                                "total_profit_eth": float(total_profit_eth) if total_profit_eth != None else None,
                                "total_profit_usd": float(total_profit_usd) if total_profit_usd != None else None,
                                "transaction_cost_eth": float(tx_cost),
                                "transaction_cost_usd": float(tx_cost * one_eth_to_usd_price),
                            }

                            collection = mongo_connection["cross_chain_arbitrage"]["detected_single_chain_arbitrage_base"]
                            try:
                                if DEBUG_MODE:
                                    import pprint
                                    pprint.pprint(finding)
                                else:
                                    collection.insert_one(finding)
                            except pymongo.errors.DuplicateKeyError:
                                pass
                            # Indexing...
                            if 'id' not in collection.index_information():
                                collection.create_index('id', unique=True)
                                collection.create_index('block_number')
                                collection.create_index('block_timestamp')
                                collection.create_index('miner')
                                collection.create_index('transaction.hash')
                                collection.create_index('arbitrages.swaps.protocol_name')
                                collection.create_index('eth_usd_price')
                                collection.create_index('total_cost_eth')
                                collection.create_index('total_cost_usd')
                                collection.create_index('total_gain_eth')
                                collection.create_index('total_gain_usd')
                                collection.create_index('total_profit_eth')
                                collection.create_index('total_profit_usd')
                                collection.create_index('transaction_cost_eth')
                                collection.create_index('transaction_cost_usd')

        except Exception as e:
            print(colors.FAIL+traceback.format_exc()+colors.END)
            print(colors.FAIL+"Error: "+str(e)+" @ block number: "+str(block_number)+colors.END)
            end = time.time()
            return end - start

        end = time.time()
        collection = mongo_connection["cross_chain_arbitrage"]["detected_single_chain_arbitrage_base_status"]
        try:
            if not DEBUG_MODE:
                collection.insert_one({"block_number": block_number, "execution_time": end-start})
        except pymongo.errors.DuplicateKeyError:
            pass
        # Indexing...
        if 'block_number' not in collection.index_information():
            collection.create_index('block_number', unique=True)

    end = time.time()
    return end - start


def init_process(_cache):
    global w3
    global client_version
    global mongo_connection
    global cache

    provider = BASE_PROVIDER
    w3 = Web3(provider)
    if w3.is_connected():
        client_version = w3.client_version
        print("Connected worker to "+colors.INFO+client_version+" ("+provider.endpoint_uri+")"+colors.END)
    else:
        client_version = ""
        print(colors.FAIL+"Error: Could not connect to Base client. Please check the provider!"+colors.END)
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    cache = _cache
    mongo_connection = pymongo.MongoClient("mongodb://"+MONGO_HOST+":"+str(MONGO_PORT), maxPoolSize=None)


def main():
    global CPUs
    global DEBUG_MODE

    if len(sys.argv) != 2:
        print(colors.FAIL+"Error: Please provide a block range to be analyzed: 'python3 "+sys.argv[0]+" <BLOCK_RANGE_START>:<BLOCK_RANGE_END>'"+colors.END)
        sys.exit(-1)
    if not ":" in sys.argv[1]:
        print(colors.FAIL+"Error: Please provide a valid block range: 'python3 "+sys.argv[0]+" <BLOCK_RANGE_START>:<BLOCK_RANGE_END>'"+colors.END)
        sys.exit(-2)
    block_range_start, block_range_end = sys.argv[1].split(":")[0], sys.argv[1].split(":")[1]
    if not block_range_start.isnumeric() or not block_range_end.isnumeric():
        print(colors.FAIL+"Error: Please provide integers as block range: 'python3 "+sys.argv[0]+" <BLOCK_RANGE_START>:<BLOCK_RANGE_END>'"+colors.END)
        sys.exit(-3)
    block_range_start, block_range_end = int(block_range_start), int(block_range_end)

    counter = 0
    block_range = list()
    block_ranges = list()
    mongo_connection = pymongo.MongoClient("mongodb://"+MONGO_HOST+":"+str(MONGO_PORT), maxPoolSize=None)
    for block in range(block_range_start, block_range_end+1):
        counter += 1
        if counter == 1:
            block_range.append(block)
        if counter == BLOCK_RANGE or block == block_range_end:
            block_range.append(block)
            count = mongo_connection["cross_chain_arbitrage"]["detected_single_chain_arbitrage_base_status"].count_documents({"block_number": {"$gte": block_range[0], "$lte" : block_range[1]}})
            if count != block_range[1] - block_range[0] + 1 or DEBUG_MODE:
                block_ranges.append(block_range)
            block_range = list()
            counter = 0

    # Tests
    # Uniswap V2:  21814941
    # Uniswap V3:  21814940

    manager = multiprocessing.Manager()
    cache = manager.dict()

    execution_times = []
    if sys.platform.startswith("linux"):
        multiprocessing.set_start_method("fork", force=True)
    if DEBUG_MODE:
        CPUs = 1
    print("Running detection of arbitrage with "+colors.INFO+str(CPUs)+colors.END+" CPUs")
    print("Initializing workers...")
    with multiprocessing.Pool(processes=CPUs, initializer=init_process, initargs=(cache, )) as pool:
        start_total = time.time()
        execution_times += pool.map(analyze_block, block_ranges)
        end_total = time.time()
        print("Total execution time: "+colors.INFO+str(end_total - start_total)+colors.END)
        print()
        if execution_times:
            print("Max execution time: "+colors.INFO+str(numpy.max(execution_times))+colors.END)
            print("Mean execution time: "+colors.INFO+str(numpy.mean(execution_times))+colors.END)
            print("Median execution time: "+colors.INFO+str(numpy.median(execution_times))+colors.END)
            print("Min execution time: "+colors.INFO+str(numpy.min(execution_times))+colors.END)


if __name__ == "__main__":
    main()
