import pandas as pd


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(by=["Year", "Month"])

    # Tạo các đặc trưng trễ (lag)
    for col in ["Avg_Adj_Close", "Average_VWAP", "Average_MA20"]:
        for lag in range(1, 4):
            df[f"{col}_lag{lag}"] = df[col].shift(lag)

    # Tạo thêm đặc trưng mục tiêu: return của tháng tới
    df["Target_Return"] = df["Avg_Adj_Close"] / df["Avg_Adj_Close_lag1"] - 1

    # Giữ lại các cột đặc trưng có thể dùng cho model
    selected_cols = [
        # Target
        "Target_Return",

        # Cột đặc trưng gốc từ mart
        "Average_Return", "Average_GapPercent", "Average_CumulativeReturn",
        "Total_Volume", "Average_STD20",

        # Các lag
        "Avg_Adj_Close_lag1", "Avg_Adj_Close_lag2", "Avg_Adj_Close_lag3",
        "Average_VWAP_lag1", "Average_VWAP_lag2", "Average_VWAP_lag3",
        "Average_MA20_lag1", "Average_MA20_lag2", "Average_MA20_lag3"
    ]

    df = df.dropna().reset_index(drop=True)
    df = df[[c for c in selected_cols if c in df.columns]]
    return df

