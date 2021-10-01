from typing import final
from pandas.core.indexes import period
import os
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

import pandas as pd
import gspread as gs
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials

from pathlib import Path
from dataclasses import dataclass, asdict
from cryptowatch.api_client import Client

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

    client = Client()

    print("Starting the update run.")

    # Create a Google Cloud connection
    gc_keys_file_name = "gc_keys"
    path_to_key_file = Path("." + os.sep + gc_keys_file_name + ".json")
    gc = gs.service_account(filename=path_to_key_file)

    pairs = ['ethchf', 'etheur', 'adaeur', 'doteur']
    dfs = []

    # Get data from Kraken
    for p in pairs:

        before_unix = 1633069800
        after_unix = 1627799400
        periods = 3600

        data = {
            'exchange': 'kraken',
            'pair': p,
            'route': 'ohlc',
            'params': {
                'before': before_unix,
                'after': after_unix,
                'periods': periods
            }
        }

        response = client.get_markets(data=data)
        print(response['allowance'])
        res = response['result']['3600']
        df_new = pd.DataFrame(data=res, columns=['Timestamp', 'OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice', 'Volume', 'QuoteVolume'])
        df_new['Pair'] = p
        print(df_new)
        dfs.append(df_new)

    df_final = pd.concat(dfs)
    print(df_final)

    # Connect to Google Sheet and insert data in the first empty row
    try:
        sh = gc.open_by_key('1KfRklC5OK01nd5oqnaxvOIUlR5-V5Lco8fQuokwlVaw')
        ws = sh.worksheet('quotes')
        current = gd.get_as_dataframe(ws)
        updated = current.append(df_final)
        gd.set_with_dataframe(ws, updated)
        print("Data updated.")

    except gs.exceptions.APIError as e:
        print(e)
        print("Data not updated!")

if __name__ == '__main__':
    main()