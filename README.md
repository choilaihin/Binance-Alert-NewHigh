# Binance-Alert-NewHigh

This script notifies the user via Telegram whenever an USDT cryptocurrency pair makes a new 30 day, 60 day or all time high.  
For each cryptocurrency pair, the notification only happens once a day.

## Description

1. Start a websocket stream with Binance's ThreadedWebsocketManager to get All Market Tickers
2. Filter unwanted tickers and store desired ticker symbols into a list
3. Get historical daily candlesticks and store into a SQLite database table for each ticker
4. Store the 30 day, 60 day, all time high value into a dictionary
5. Continuously compare current price with the dictionary value and send a Telegram message if greater
6. Update the list of tickers and database at the start of the next day

## Technologies Used

- Using python-binance ThreadedWebsocketManager Websocket  
  https://python-binance.readthedocs.io/en/latest/index.html

- Parsing candlestick data  
  https://github.com/binance-us/binance-official-api-docs/blob/master/rest-api.md#klinecandlestick-data
