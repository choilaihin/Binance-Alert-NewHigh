import config
import sqlite3
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager

client = Client(config.API_KEY, config.API_SECRET)

symbol = "ETHUSDT"

try:
    conn = sqlite3.connect('daily_database.db')
except:
    print("Unable to connect to database")

# Check whether table exist for symbol
cursor = conn.execute(f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{symbol}'")

if cursor.fetchone()[0]==1:
	print('Table exists.')
else:
    print(f'Table does not exist, created table for {symbol}')
    conn.execute(f'''CREATE TABLE {symbol}
         (TIME REAL PRIMARY KEY     NOT NULL,
         OPEN           REAL    NOT NULL,
         HIGH           REAL    NOT NULL,
         LOW           REAL    NOT NULL,
         CLOSE           REAL    NOT NULL,
         VOLUME           REAL    NOT NULL);''')


candlesticks= client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1DAY, "1 Sep, 2021", "10 Sep, 2021")

start_inserting = False
for candlestick in candlesticks:
    if start_inserting:
        # print('Inserting...')
        conn.execute(f"INSERT INTO {symbol} (TIME,OPEN,HIGH,LOW,CLOSE,VOLUME) VALUES ({candlestick[0]},{float(candlestick[1])},{float(candlestick[2])},{float(candlestick[3])},{float(candlestick[4])},{float(candlestick[5])})")
    else:
        # check whether next candlestick exist in table
        cursor = conn.execute(f"SELECT rowid FROM {symbol} WHERE TIME = ?", (candlestick[0]+86400000,))
        if cursor.fetchone() is None:
            # update current candlestick and insert rest
            # print('The next candlestick time is not found')
            cursor = conn.execute(f"SELECT rowid FROM {symbol} WHERE TIME = ?", (candlestick[0],))
            if cursor.fetchone() is None:
                # the current candlestick time is not found too
                # print('The current candlestick time is not found too, inserting...')
                conn.execute(f"INSERT INTO {symbol} (TIME,OPEN,HIGH,LOW,CLOSE,VOLUME) VALUES ({candlestick[0]},{float(candlestick[1])},{float(candlestick[2])},{float(candlestick[3])},{float(candlestick[4])},{float(candlestick[5])})")
            else:
                # print('The current candlestick time is found, updating...')
                conn.execute(f"UPDATE {symbol} set OPEN = ?, HIGH = ?, LOW = ?, CLOSE = ?, VOLUME = ? where TIME = {candlestick[0]}", (float(candlestick[1]),float(candlestick[2]),float(candlestick[3]),float(candlestick[4]),float(candlestick[5]),))
            start_inserting = True
        else:
            # do nothing
            # print('The next candlestick time is found')

    #to remove microseconds from time
    # candlestick[0] = candlestick[0] / 1000
    # print(candlestick)

conn.commit()


'''
get_historical_klines
[
  [
    1499040000000,      // Open time
    "0.01634790",       // Open
    "0.80000000",       // High
    "0.01575800",       // Low
    "0.01577100",       // Close
    "148976.11427815",  // Volume
    1499644799999,      // Close time
    "2434.19055334",    // Quote asset volume
    308,                // Number of trades
    "1756.87402397",    // Taker buy base asset volume
    "28.46694368",      // Taker buy quote asset volume
    "17928899.62484339" // Ignore.
  ]
]

starting with oldest
candlestick:
[1631059200.0, '27.97000000', '28.88000000', '25.20000000', '28.10000000', '15565076.06000000', 1631145599999, '425253719.40400000', 580764, '7617231.99000000', '208281019.54890000', '0']
'''