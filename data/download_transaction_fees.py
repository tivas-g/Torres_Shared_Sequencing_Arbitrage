#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import requests
import pandas as pd

STARTING_DATE  = "2024-11-01"
ENDING_DATE    = "2024-11-01"
API_KEY        = "W8ax4JoLX4xa9pX1Qow-uMrPGnImFEHo-YjKM4ixU2A4b7WhxDuGVishO0djeMKb6Bt-5ouZBn8aTJqDGQF43w"
CHAINS         = ["arbitrum", "optimism", "base"]
MAX_LENGTH     = 5

def main():
    for chain in CHAINS:
        for length in range(2, MAX_LENGTH + 1):

            if not os.path.exists("fees/"+chain+"/"):
                os.makedirs("fees/"+chain+"/")
        
            period_range = pd.period_range(start=STARTING_DATE, end=ENDING_DATE, freq="D")
            dates = tuple([(period.year, period.month, period.day) for period in period_range])
            
            for date in dates:
                start_date = str(date[0])+"-"+"{:02d}".format(date[1])+"-"+"{:02d}".format(date[2])+"T00:00:00"
                end_date = str(date[0])+"-"+"{:02d}".format(date[1])+"-"+"{:02d}".format(date[2])+"T23:59:59"

                if not os.path.exists("fees/"+chain+"/"+chain+"_"+str(length)+"_transaction_fees_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json"):

                    print("Downloading transaction fees on "+chain.capitalize()+" for length "+str(length)+" from", start_date, "to", end_date)
                    download_start = time.time()

                    # Create a query run
                    response = requests.post(
                        "https://api.allium.so/api/v1/explorer/queries/Fsgqyna1VrcP3LLYKp5b/run-async",
                        json={"parameters": {"chain": chain, "swap_count": str(length), "block_timestamp_start": start_date, "block_timestamp_end": end_date}, "run_config": {"limit": 250000}},
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

                    fees = list()
                    for fee in response.json()["data"]:
                        fees.append(fee)

                    with open("fees/"+chain+"/"+chain+"_"+str(length)+"_transaction_fees_"+str(date[0])+"_"+"{:02d}".format(date[1])+"_"+"{:02d}".format(date[2])+".json", "w") as f:
                        json.dump(fees, f, indent=4)
                    print("Saved", len(fees), "transaction fee instances.")


if __name__ == "__main__":
    main()
