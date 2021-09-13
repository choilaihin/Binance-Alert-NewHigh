'''
This script notifies the user via Telegram whenever an USDT cryptocurrency pair makes a new 30 day, 60 day or all time high.
For each cryptocurrency pair, the notification only happens once a day.

Description:
1. Start a websocket stream with Binance's ThreadedWebsocketManager to get All Market Tickers
2. Filter unwanted tickers and store desired ticker symbols into a list
3. Get historical daily candlesticks and store into a SQLite database table for each ticker
4. Store the 30 day, 60 day, all time high value into a dictionary
5. Continuously compare current price with the dictionary value and send a Telegram message if greater
6. Update the list of tickers and database at the start of the next day (UTC +0)
'''

import config, sqlite3, datetime, time, requests, logging
# python-binance library
from binance import ThreadedWebsocketManager
from binance.client import Client

# Setting up Binance API client
api_key = config.API_KEY
api_secret = config.API_SECRET
client = Client(api_key, api_secret)

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def setup_logger(name, log_file, level):
    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

loggerdailyhigh = setup_logger('first_logger', 'dailyhigh.log', logging.INFO)

# function to send telegram message
def telegram_bot_sendtext(bot_message):
    bot_token = config.bot_token
    bot_chatID = config.bot_chatID
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message
    response = requests.get(send_text)
    return response.json()

record = True
record_time = datetime.datetime.now()
initialisation_1min = record_time + datetime.timedelta(seconds=20)
initialisation_phase = True
database_phase = False
# Set first_run to True if database is not available
first_run = False
ticker_list = []
ticker_high_dict = {}
streams = ['!ticker@arr']
loggerdailyhigh.info("Getting desired symbols...")

def main():
    # Initialise ThreadedWebsocketManager
    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
    # start is required to initialise its internal loop
    twm.start()

    # Exclude undesired symbols such as stable coins
    exclusion_usdt_symbols = ["BUSDUSDT", "USDCUSDT", "TUSDUSDT", "SUSDUSDT","PAXUSDT"]
    
    # This function is executed whenever streaming data is received.
    def handle_socket_message(msg):
        global record, record_time, initialisation_phase, database_phase, initialisation_1min, first_run, ticker_list, ticker_high_dict, streams

        # check for new tickers at the start of the next day (UTC +0) and add to a list
        now = datetime.datetime.now()
        if record_time.day != now.day and now.hour == 8:
            record = True
            initialisation_phase = True
            record_time = datetime.datetime.now()
            # allocate 20 seconds to record all tickers
            initialisation_1min = record_time + datetime.timedelta(seconds=20)

        # variables for transitioning to database updating phase which runs once a day too
        if now>initialisation_1min and initialisation_phase:
            initialisation_phase = False
            record = False
            database_phase = True
            loggerdailyhigh.info(f"There are {len(ticker_list)} symbols, updating database...")

        # record symbol and price of desired tickers into a list
        if msg['stream'] == '!ticker@arr' and record:
            for ticker in msg['data']:
                symbol = ticker["s"]
                price = float(ticker["c"])
                if symbol.endswith('USDT') and not symbol.endswith('UPUSDT') and not symbol.endswith('DOWNUSDT') and (symbol not in exclusion_usdt_symbols):
                    if record and symbol not in ticker_list:
                        ticker_list.append(symbol)

        # update SQLite database after getting tickers
        if database_phase:
            try:
                conn = sqlite3.connect('daily_database.db')
            except Exception as e:
                loggerdailyhigh.info(f"Unable to connect to database. Error: {e}")

            for symbol in ticker_list:
                # SQLite does not allow table name to start with a number (e.g. 1INCHUSDT) thus "1INCHUSDT" is used
                symbol_string = '"' + symbol + '"'
                try:
                    # Check whether table exist for symbol
                    cursor = conn.execute(f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name={symbol_string}")

                    # create table for symbol if does not exist
                    if cursor.fetchone()[0]!=1:
                        conn.execute(f'''CREATE TABLE {symbol_string}
                            (TIME REAL PRIMARY KEY     NOT NULL,
                            OPEN           REAL    NOT NULL,
                            HIGH           REAL    NOT NULL,
                            LOW           REAL    NOT NULL,
                            CLOSE           REAL    NOT NULL,
                            VOLUME           REAL    NOT NULL);''')
                    
                    # get historical data, only update previous day's data if database already exist
                    if first_run:
                        candlesticks= client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1DAY, "1 Jan, 2021", "10 Sep, 2021")
                    else:
                        candlesticks= client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1DAY, "2 day ago UTC")

                    # Update database
                    start_inserting = False
                    for candlestick in candlesticks:
                        if start_inserting:
                            # insert the remaining candlesticks without checking for existence
                            conn.execute(f"INSERT INTO {symbol_string} (TIME,OPEN,HIGH,LOW,CLOSE,VOLUME) VALUES ({candlestick[0]},{float(candlestick[1])},{float(candlestick[2])},{float(candlestick[3])},{float(candlestick[4])},{float(candlestick[5])})")
                        else:
                            # check whether next candlestick exist in table
                            cursor = conn.execute(f"SELECT rowid FROM {symbol_string} WHERE TIME = ?", (candlestick[0]+86400000,))
                            if cursor.fetchone() is None:
                                # insert/update current candlestick and insert rest if the next candlestick is not found
                                cursor = conn.execute(f"SELECT rowid FROM {symbol_string} WHERE TIME = ?", (candlestick[0],))
                                if cursor.fetchone() is None:
                                    # insert current candlestick since it is not found too
                                    conn.execute(f"INSERT INTO {symbol_string} (TIME,OPEN,HIGH,LOW,CLOSE,VOLUME) VALUES ({candlestick[0]},{float(candlestick[1])},{float(candlestick[2])},{float(candlestick[3])},{float(candlestick[4])},{float(candlestick[5])})")
                                else:
                                    # update current candlestick since it exists
                                    conn.execute(f"UPDATE {symbol_string} set OPEN = ?, HIGH = ?, LOW = ?, CLOSE = ?, VOLUME = ? where TIME = {candlestick[0]}", (float(candlestick[1]),float(candlestick[2]),float(candlestick[3]),float(candlestick[4]),float(candlestick[5]),))
                                # insert the remaining candlesticks without checking for existence
                                start_inserting = True
                except Exception as e:
                    loggerdailyhigh.info(f"Error for {symbol}: {e}")
            first_run = False
            conn.commit()

            # store daily highs into a dictionary
            # multiple time.time() by 1000 to convert from s to ms, each day differs by 86400000 ms, multiple by x+1 for x day high
            lower_thres_time_30d = time.time()*1000 - (86400000*31)
            lower_thres_time_60d = time.time()*1000 - (86400000*61)
            upper_thres_time = time.time()*1000 - 86400000
            for symbol in ticker_list:
                symbol_string = '"' + symbol + '"'
                try:
                    # obtain the maximum high of the respective intervals (e.g. 30 day, 60 day or all time high)
                    cursor_30d = conn.execute(f"SELECT MAX(HIGH) FROM {symbol_string} WHERE TIME > {lower_thres_time_30d} AND TIME < {upper_thres_time}")
                    cursor_60d = conn.execute(f"SELECT MAX(HIGH) FROM {symbol_string} WHERE TIME > {lower_thres_time_60d} AND TIME < {upper_thres_time}")
                    cursor_alltime = conn.execute(f"SELECT MAX(HIGH) FROM {symbol_string} WHERE TIME < {upper_thres_time}")
                    # store the values into a dictionary
                    ticker_high_dict[symbol]= {'alltime':cursor_alltime.fetchone()[0],'60d':cursor_60d.fetchone()[0],'30d':cursor_30d.fetchone()[0]}
                except Exception as e:
                    loggerdailyhigh.info(f"Error for {symbol}: {e}")
            try:
                conn.close()
            except Exception as e:
                loggerdailyhigh.info(f"Error: {e}")
            loggerdailyhigh.info("Database and dictionary updated")
            database_phase = False

        # check current price against database highs
        if not database_phase and not initialisation_phase:
            if msg['stream'] == '!ticker@arr':
                for ticker in msg['data']:
                    symbol = ticker["s"]
                    price = float(ticker["c"])
                    # ensure that values are available before comparison
                    if symbol in ticker_high_dict and price and ticker_high_dict[symbol]['alltime'] and ticker_high_dict[symbol]['60d'] and ticker_high_dict[symbol]['30d']:
                        if price > ticker_high_dict[symbol]['alltime']:
                            now = datetime.datetime.now()
                            telegram_bot_sendtext(f"{now:%Y-%m-%d %H:%M:%S} {symbol}\nall time high of {price}")
                            loggerdailyhigh.info(f'{now:%Y-%m-%d %H:%M:%S} {symbol} reached all time high of {price}')
                            # to prevent repeat alert for the day
                            del ticker_high_dict[symbol]
                            # to skip subsequent comparisons for this symbol
                            continue
                        if price > ticker_high_dict[symbol]['60d']:
                            now = datetime.datetime.now()
                            telegram_bot_sendtext(f"{now:%Y-%m-%d %H:%M:%S} {symbol}\n60 day high of {price}")
                            loggerdailyhigh.info(f'{now:%Y-%m-%d %H:%M:%S} {symbol} reached 60 day high of {price}')
                            del ticker_high_dict[symbol]
                            continue
                        if price > ticker_high_dict[symbol]['30d']:
                            now = datetime.datetime.now()
                            telegram_bot_sendtext(f"{now:%Y-%m-%d %H:%M:%S} {symbol}\n30 day high of {price}")
                            loggerdailyhigh.info(f'{now:%Y-%m-%d %H:%M:%S} {symbol} reached 30 day high of {price}')
                            del ticker_high_dict[symbol]
                        
 
    stream_name = twm.start_multiplex_socket(callback=handle_socket_message, streams=streams)
    # Join to main thread to keep the ThreadedWebsocketManager running.
    twm.join()

if __name__ == "__main__":
   main()


'''
References:
https://python-binance.readthedocs.io/en/latest/websockets.html#threadedwebsocketmanager-websocket-usage
https://github.com/binance-us/binance-official-api-docs/blob/master/rest-api.md#klinecandlestick-data

!ticker@arr stream provide the stream's data in the form of a list of dictionary
{'stream': '!ticker@arr', 
'data': 
    [{
        'e': '24hrTicker', 
        'E': 1629721891800, 
        's': 'ETHBTC', 
        'p': '0.00012900', 
        'P': '0.195', 
        'w': '0.06597492', 
        'x': '0.06627600', 
        'c': '0.06641800', 
        'Q': '5.00900000', 
        'b': '0.06641700', 
        'B': '1.22200000', 
        'a': '0.06641800', 
        'A': '8.52900000', 
        'o': '0.06628900', 
        'h': '0.06691000', 
        'l': '0.06486600', 
        'v': '132820.90500000', 
        'q': '8762.84861751', 
        'O': 1629635491406, 
        'C': 1629721891406, 
        'F': 290818106, 
        'L': 291008032, 
        'n': 189927}, ...
    ]
}

get_historical_klines provides a list of lists (daily candlesticks)
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
'''