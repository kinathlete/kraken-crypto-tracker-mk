import requests
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pandas as pd
import os

import datetime
import pytz

import gspread as gs
from oauth2client.service_account import ServiceAccountCredentials

from pathlib import Path
from dataclasses import dataclass, asdict

# CREATHE THE DATACLASS SCHEMA
@dataclass
class KrakenTicker:
    timestamp: str
    symbol: str
    ask_price: str
    ask_lot_vol: str
    bid_price: str
    bid_lot_vol: str
    volume_today: str
    volume_last24h: str
    number_of_trades_today: str
    number_of_trades_last24h: str
    low_today: str
    low_last24h: str
    high_today: str
    high_last24h: str
    day_opening_price:str

# MAIN FUNCTION: CREATES A GOOGLE CLOUD CONNECTION TO THE SHEET, REQUESTS THE
# KRAKEN PUBLIC API FOR DATA AND APPENDS ROWS TO GOOGLE SHEET.

def main():

    print("Starting the update run.")

    # Create a Google Cloud connection
    gc_keys_file_name = "gc_keys"
    path_to_key_file = Path("." + os.sep + gc_keys_file_name + ".json")
    gc = gs.service_account(filename=path_to_key_file)

    pairs = ['ETHCHF','XETHZEUR', 'ADAEUR', 'DOTEUR']
    rows = []

    # Get data from Kraken
    try:
        for p in pairs:
            response = requests.get(f'https://api.kraken.com/0/public/Ticker?pair={p}')
            json_data = json.loads(response.text)
            res = json_data['result']
            row = KrakenTicker(
                timestamp = datetime.datetime.now(pytz.timezone('Europe/Zurich')).strftime("%x %X"),
                symbol = p,
                ask_price = res[f'{p}']['a'][0],
                ask_lot_vol = res[f'{p}']['a'][2],
                bid_price = res[f'{p}']['b'][0],
                bid_lot_vol = res[f'{p}']['b'][2],
                volume_today = res[f'{p}']['v'][0],
                volume_last24h = res[f'{p}']['v'][1],
                number_of_trades_today = res[f'{p}']['t'][0],
                number_of_trades_last24h = res[f'{p}']['t'][1],
                low_today = res[f'{p}']['l'][0],
                low_last24h = res[f'{p}']['l'][1],
                high_today = res[f'{p}']['h'][0],
                high_last24h = res[f'{p}']['h'][1],
                day_opening_price = res[f'{p}']['o']
            )
            rows.append(row)
        
        print(rows)
            
    # Catch error if any
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)

    # Connect to Google Sheet and insert data in the first empty row
    try:
        sh = gc.open_by_key('1jI1wM5xFvI5jgzQ-dZR_RjlCP6tTltKOCEVirWl1a-c')
        ws = sh.worksheet('quotes')
        last_row_number = len(ws.col_values(1)) + 1
        ready_rows = [list(asdict(row).values()) for row in rows]
        ws.append_rows(ready_rows)

    except gs.exceptions.APIError as e:
        print(e)

    print("Data updated.")

if __name__ == '__main__':
    main()