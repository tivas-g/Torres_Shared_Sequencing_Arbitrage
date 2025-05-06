#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import csv
import requests

# OUTPUT
OUTPUT_FILE = "top_pools_over_100m.csv"

class colors:
    INFO = '\033[94m'
    OK = '\033[92m'
    FAIL = '\033[91m'
    END = '\033[0m'


def main():
    if len(sys.argv) < 2:
        print(colors.FAIL+"Error: Please provide a Dune API key to download top pools over 100m yearly volume: 'python3 "+sys.argv[0]+" <DUNE_API_KEY>'"+colors.END)
        sys.exit(-1)

    dune_api_key = sys.argv[1]

    response = requests.get("https://api.dune.com/api/v1/query/4573557/results?limit=1000", headers={"X-Dune-API-Key": dune_api_key})
    rows = response.json()["result"]["rows"]
    
    if rows > 0:
        with open(OUTPUT_FILE, "w") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(rows[0].keys())
            for row in rows:
                csv_writer.writerow(row.values())


if __name__ == "__main__":
    main()
