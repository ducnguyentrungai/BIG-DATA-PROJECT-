import pandas as pd
import os
def save_to_parquet(data: pd.DataFrame, path: str):
    """
    Lưu DataFrame dưới dạng Parquet (engine: pyarrow), chuẩn hóa cột Date.

    Args:
        data (pd.DataFrame): Dữ liệu cần lưu
        path (str): Đường dẫn file đầu ra (.parquet)

    Returns:
        bool: True nếu lưu thành công, False nếu có lỗi
    """

    try:
        # 1. Kiểm tra dữ liệu rỗng
        if data is None or data.empty:
            print("⚠️ DataFrame rỗng. Không có gì để lưu.")
            return False

        # 2. Tạo thư mục nếu chưa tồn tại
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # 3. Chuẩn hóa cột 'Date' nếu tồn tại
        if 'Date' in data.columns:
            data['Date'] = pd.to_datetime(data['Date'], errors='coerce').astype('datetime64[ms]')
            if data['Date'].isna().any():
                print("⚠️ Một số dòng có 'Date' không hợp lệ sẽ bị ghi thành null.")

        # 4. Lưu file parquet
        data.to_parquet(path, index=False, engine='pyarrow')
        print(f"✅ Parquet saved: {path}")
        print(f"🔢 Số dòng: {len(data)} | Cột: {list(data.columns)}")
        return True

    except Exception as e:
        print(f"❌ Lỗi khi lưu Parquet: {e}")
        return False
