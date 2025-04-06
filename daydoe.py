import time
import yaml
import pandas as pd
import numpy as np

# For stocks (Alpaca)
import alpaca_trade_api as tradeapi

# For crypto (CCXT)
import ccxt

##########################
#   Simple SMA Strategy
##########################
def sma_crossover_signals(df, short_window, long_window):
    """
    Given a DataFrame with a 'close' column,
    compute SMA crossover signals: 'buy', 'sell', or 'hold'.
    """
    df['SMA_short'] = df['close'].rolling(window=short_window).mean()
    df['SMA_long']  = df['close'].rolling(window=long_window).mean()

    # If we don't have enough data points for the rolling window, return 'hold'
    if df['SMA_short'].isna().any() or df['SMA_long'].isna().any():
        return 'hold'

    short_sma_latest = df['SMA_short'].iloc[-1]
    long_sma_latest  = df['SMA_long'].iloc[-1]

    if short_sma_latest > long_sma_latest:
        return 'buy'
    elif short_sma_latest < long_sma_latest:
        return 'sell'
    else:
        return 'hold'

##########################
#  Config Loader
##########################
def load_config(filepath='config.yaml'):
    with open(filepath, 'r') as f:
        return yaml.safe_load(f)

##########################
#  Alpaca Functions
##########################
def run_alpaca_bot(config):
    """
    Fetches historical data from Alpaca (using the Polygon or Alpaca data API),
    generates a signal, and places an order if conditions are met.
    """
    # Initialize the Alpaca API client
    alpaca_config = config['alpaca']
    api_key = alpaca_config['api_key']
    api_secret = alpaca_config['api_secret']
    base_url = alpaca_config['base_url']
    symbol = alpaca_config['symbol']
    notional = alpaca_config['notional']

    # Connect to Alpaca
    api = tradeapi.REST(api_key, api_secret, base_url, api_version='v2')

    # Let's fetch the last 200 15-minute bars (or whatever timeframe you want).
    # Alpaca uses a separate method to fetch historical data:
    barset = api.get_bars(
        symbol,
        '15Minute',   # Options: '1Min', '5Min', '15Min', '1Hour', '1Day'
        limit=50      # fetch up to 50 bars
    )

    # Convert barset to a DataFrame
    df = pd.DataFrame([{
        'timestamp': bar.t,
        'open': float(bar.o),
        'high': float(bar.h),
        'low': float(bar.l),
        'close': float(bar.c),
        'volume': float(bar.v)
    } for bar in barset])

    if df.empty:
        print("No data returned from Alpaca, skipping.")
        return

    # Generate signal
    signal = sma_crossover_signals(
        df,
        config['short_window'],
        config['long_window']
    )
    print(f"Signal for {symbol}: {signal}")

    # Check current position (if any)
    try:
        position = api.get_position(symbol)
        current_qty = float(position.qty)
    except tradeapi.rest.APIError:
        # Means no existing position
        current_qty = 0

    # Buy if signal is 'buy' and we have no position
    if signal == 'buy' and current_qty == 0:
        print(f"Placing market BUY order for {symbol}")
        order = api.submit_order(
            symbol=symbol,
            notional=notional,  # or qty=...
            side='buy',
            type='market',
            time_in_force='gtc'
        )
        print(order)

    # Sell if signal is 'sell' and we have a position
    elif signal == 'sell' and current_qty > 0:
        print(f"Placing market SELL order for {symbol} of size {current_qty}")
        order = api.submit_order(
            symbol=symbol,
            qty=current_qty,
            side='sell',
            type='market',
            time_in_force='gtc'
        )
        print(order)


##########################
#   CCXT Functions
##########################
def run_ccxt_bot(config):
    """
    Fetches OHLCV data from a crypto exchange via CCXT,
    generates a signal, and places an order if conditions are met.
    """
    ccxt_config = config['ccxt']
    exchange_name = ccxt_config['exchange']
    api_key = ccxt_config['api_key']
    api_secret = ccxt_config['api_secret']
    symbol = ccxt_config['symbol']
    trade_amount = ccxt_config['trade_amount']
    timeframe = config['time_interval']  # e.g., '15m'

    # Initialize the exchange
    klass = getattr(ccxt, exchange_name)
    exchange = klass({
        'apiKey': api_key,
        'secret': api_secret,
        'enableRateLimit': True
    })

    # Fetch OHLCV data
    limit = 50
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

    if df.empty:
        print("No data returned from exchange, skipping.")
        return

    # Generate signal
    signal = sma_crossover_signals(
        df,
        config['short_window'],
        config['long_window']
    )
    print(f"Signal for {symbol} on {exchange_name}: {signal}")

    # Check current holdings
    balances = exchange.fetch_balance()
    base_symbol = symbol.split('/')[0]  # e.g., 'BTC' in 'BTC/USDT'
    base_free = balances.get(base_symbol, {}).get('free', 0)

    # If signal is 'buy' and we have no position
    if signal == 'buy' and base_free < 1e-8:  # basically 0
        print(f"Placing MARKET BUY order for {symbol}, amount={trade_amount}")
        try:
            order = exchange.create_market_buy_order(symbol, trade_amount)
            print(order)
        except Exception as e:
            print(f"Error placing buy order: {e}")

    # If signal is 'sell' and we do have a position
    elif signal == 'sell' and base_free > 1e-8:
        print(f"Placing MARKET SELL order for {symbol}, amount={base_free}")
        try:
            order = exchange.create_market_sell_order(symbol, base_free)
            print(order)
        except Exception as e:
            print(f"Error placing sell order: {e}")


##########################
#         Main
##########################
def main():
    config = load_config('config.yaml')
    broker = config['broker'].lower()  # "alpaca" or "ccxt"

    while True:
        try:
            if broker == 'alpaca':
                run_alpaca_bot(config)
            elif broker == 'ccxt':
                run_ccxt_bot(config)
            else:
                print(f"Unknown broker: {broker}")
                break
        except Exception as e:
            print(f"Main loop error: {e}")

        # Sleep between iterations
        time.sleep(config['loop_interval_sec'])


if __name__ == "__main__":
    main()
