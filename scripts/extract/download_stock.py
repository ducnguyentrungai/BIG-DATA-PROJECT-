import yfinance as yf
import pandas as pd

def extract_stock_data(symbol: str) -> pd.DataFrame:
    """
    Extract historical stock data from Yahoo Finance.
    """
    print(f"📥 Fetching stock data from Yahoo Finance for symbol '{symbol}'...")
    data = yf.download(tickers=symbol, interval='1d', period='max')

    # Handle MultiIndex returned by yfinance
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)

    data.reset_index(inplace=True)

    if data.empty:
        raise ValueError("❌ No data found.")
    return data
