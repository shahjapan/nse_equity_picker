import os
import datetime
import pandas
import nsepy
import zipfile

from collections import defaultdict


_SYMBOLS = ()
_DATE_RANGE = []    # will contain the first and last date of the retrieved data from bhav-copies
BACKTEST_DAYS_RANGE = 11
DATA_DIR = 'bhavcopies'
PRICE_LIST = None    # date wise price list for each equity
BACKTEST_DATE = datetime.datetime.utcnow()
OUTPUT = defaultdict(list)


def check_nr4_consolidattion_breakout():
    pass


def check_nr7_consolidattion_breakout():
    pass


def day90_breakout():
    pass


def day60_breakout():
    pass


def day30_breakout():
    pass


def day2_same_lower_intra_position(symbol):
    "COLUMNS ==> SYMBOL SERIES   OPEN   HIGH    LOW  CLOSE   LAST  PREVCLOSE  TIMESTAMP"
    # INTRA_DAY_EQN = "PREV_CLOSE < CLOSE_PRICE and LOW_PRICE < PREV_CLOSE and CLOSE_PRICE >= OPEN_PRICE and (HIGH_PRICE-LOW_PRICE)>=2 and DELIV_PER>40 and DELIV_QTY>100000 and SERIES=='EQ'"
    today, yesterday = _DATE_RANGE[-1], _DATE_RANGE[-2]
    output = PRICE_LIST[(PRICE_LIST.SERIES=='EQ') & (PRICE_LIST.SYMBOL==symbol) & 
                        (PRICE_LIST.TIMESTAMP==today) & (PRICE_LIST.PREVCLOSE<=PRICE_LIST.CLOSE) & (PRICE_LIST.LOW<=PRICE_LIST.PREVCLOSE) & 
                        (PRICE_LIST.CLOSE>=PRICE_LIST.OPEN) & (PRICE_LIST.HIGH - PRICE_LIST.LOW)>=2]
    return True if not output.empty else False

NR4_CONSOLIDATION = check_nr4_consolidattion_breakout
NR7_CONSOLIDATION = check_nr7_consolidattion_breakout

DAY90_BREAKOUT = day90_breakout
DAY60_BREAKOUT = day60_breakout
DAY30_BREAKOUT = day30_breakout


CRITERIAS = {'two_day_lower': day2_same_lower_intra_position}


def get_symbols():
    global _SYMBOLS
    _SYMBOLS = _SYMBOLS or tuple(set(PRICE_LIST['SYMBOL']))
    return _SYMBOLS


def get_dates():
    global _DATE_RANGE
    if _DATE_RANGE: return _DATE_RANGE
    _DATE_RANGE = sorted(list(set(PRICE_LIST['TIMESTAMP'])))
    return _DATE_RANGE


def custom_dt_converter(val, fmt='%d-%b-%Y'):
    return datetime.datetime.strptime(val, fmt)


def update_price_list(df):
    global PRICE_LIST
    if PRICE_LIST is None:
        PRICE_LIST = df
    else:
        PRICE_LIST = PRICE_LIST.append(df, ignore_index=True)
        

def fetch_nse_bhavcopies():
    global PRICE_LIST
    for dt_index in range(BACKTEST_DAYS_RANGE):
        current_dt = datetime.datetime.today() - datetime.timedelta(days=dt_index)
        if current_dt.weekday() in (5, 6):
            print "Holiday...", current_dt.date(), current_dt.strftime('%a')
            continue

        date_str = current_dt.date().strftime('%Y-%m-%d')
        date_wise_csv = os.path.join(DATA_DIR, 'bhavcopy_%s.csv'  % date_str)
        if not os.path.exists(date_wise_csv):
            try:
                ret_val = nsepy.history.get_price_list(dt=current_dt.date())
                ret_val.to_csv(date_wise_csv)
                update_price_list(ret_val)
            except zipfile.BadZipfile:
                print "Content might not be available for date: ", date_str
                continue
        else:
            ret_val = pandas.read_csv(date_wise_csv)
            update_price_list(ret_val)
            print "Loaded from local storage....", date_str

    PRICE_LIST['TIMESTAMP'] = PRICE_LIST['TIMESTAMP'].apply(custom_dt_converter)
    PRICE_LIST = PRICE_LIST.drop(columns=['TOTALTRADES', 'TOTTRDVAL', 'ISIN', 'TOTTRDQTY' ,'Unnamed: 0'])
    print "NSE India BhavCopies fetched successfully.......\n"
    print "Dates.......", get_dates()


def main():
    fetch_nse_bhavcopies()
    symbols = get_symbols()
    print "\n* * * ......................STARTING Execution.................... * * *"
    for symbol in symbols:
        for criteria, function in CRITERIAS.items():
            ret_val = function(symbol)
            if ret_val:
                OUTPUT[criteria].append(symbol)

    print "BUY: %s" % OUTPUT


if __name__ == "__main__":
    main()
