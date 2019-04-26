#!#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import requests
import StringIO

BASE_URL = "https://www.nseindia.com/"
EQ_HEADER = ['SYMBOL', 'SERIES', 'DATE1', 'PREV_CLOSE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'LAST_PRICE', 'CLOSE_PRICE', 'AVG_PRICE', 
'TTL_TRD_QNTY', 'TURNOVER_LACS', 'NO_OF_TRADES', 'DELIV_QTY', 'DELIV_PER']

HEADER_MAPPER = {'PREV_CLOSE': float, 'OPEN_PRICE': float, 'HIGH_PRICE': float, 
'LOW_PRICE': float, 'LAST_PRICE': float, 'CLOSE_PRICE': float, 'AVG_PRICE': float,
'NO_OF_TRADES': int, 'DELIV_QTY': float, 'DELIV_PER': float}

BHAV_HEADER_MAPPER = ['SYMBOL', 'SERIES', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST', 'PREVCLOSE', 'TOTTRDQTY', 'TOTTRDVAL', 'TIMESTAMP', 'TOTALTRADES', 'ISIN']
DAILY_HEADER_TO_BHAV = {'PREV_CLOSE': 'PREV', 'OPEN_PRICE': 'OPEN', 'HIGH_PRICE': 'HIGH', 
'LOW_PRICE': 'LOW', 'LAST_PRICE': 'LAST', 'CLOSE_PRICE': 'CLOSE', 'AVG_PRICE': 'AVG',
'NO_OF_TRADES': 'TOTALTRADES', 'DELIV_QTY': float, 'DELIV_PER': float, 'ISIN': 'ISIN'}

INTRA_DAY_EQN = "PREV_CLOSE < CLOSE_PRICE and LOW_PRICE < PREV_CLOSE and CLOSE_PRICE >= OPEN_PRICE and (HIGH_PRICE-LOW_PRICE)>=2 and DELIV_PER>40 and DELIV_QTY>100000 and SERIES=='EQ'"
INTRA_DAY_2_EQN = "PREV_CLOSE < CLOSE_PRICE and LOW_PRICE==PREV_CLOSE and CLOSE_PRICE >= OPEN_PRICE and SERIES=='EQ'"


def download_todays_bhav_copy_csv():
    url = BASE_URL + 'products/content/sec_bhavdata_full.csv'
    response = requests.get(url)
    return StringIO.StringIO(senitize_csv(response.content))

def row_type_conversion(row, mapper):
    for column in row.keys():
        if column in mapper:
            if not row[column] or row[column]=='-':
                row[column] = mapper[column]('0')
            else:
                row[column] = mapper[column](row[column])
    return row
        

def get_daily_eq_csv():
    instream = download_todays_bhav_copy_csv()
    reader_obj =  csv.DictReader(instream, fieldnames=EQ_HEADER)
    next(reader_obj, None) # Skip Headers
    rows = [row_type_conversion(row, HEADER_MAPPER) for row in reader_obj if row]
    return rows

def senitize_csv(data):
    return data.replace(' ', '')

def execute_stock_selector(rows):
    output = dict(intraday_eq=[], intraday_2_eq=[])
    for each_row in rows:
        intraday_select = eval(INTRA_DAY_EQN, globals(), each_row)
        intraday_select_2 = eval(INTRA_DAY_2_EQN, globals(), each_row)
        if intraday_select:
            output['intraday_eq'].append(each_row['SYMBOL'])
        if intraday_select_2:
            output['intraday_2_eq'].append(each_row['SYMBOL'])
    return output
    

def main():
    eq_data = get_daily_eq_csv()
    print execute_stock_selector(eq_data)


if __name__ == '__main__':
    main()
