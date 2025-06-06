import os

def save_to_parquet(data, path: str):
    import pandas as pd
    os.makedirs(os.path.dirname(path), exist_ok=True)

    data['Date'] = pd.to_datetime(data['Date']).astype('datetime64[ms]')
    data.to_parquet(path, index=False, engine='pyarrow')
    print(f"✅ Parquet saved: {path}")
