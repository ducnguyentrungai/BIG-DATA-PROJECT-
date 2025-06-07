import os
import pandas as pd

import pandas as pd
import os

def transform_stock_data(data: pd.DataFrame, file_name: str) -> pd.DataFrame:
    """
    Làm sạch và lưu dữ liệu chứng khoán vào file CSV.
    - Nếu file đã tồn tại: gộp và loại bỏ trùng lặp theo 'Date'
    - Nếu chưa: tạo mới file
    - Đảm bảo ngày hợp lệ, sort theo ngày tăng dần
    """

    try:
        # Làm sạch dữ liệu mới
        print("🧹 Đang làm sạch dữ liệu mới...")
        data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
        data.dropna(subset=['Date'], inplace=True)
        data.drop_duplicates(subset=['Date'], keep='last', inplace=True)

        # Tạo thư mục nếu chưa tồn tại
        os.makedirs(os.path.dirname(file_name), exist_ok=True)

        if os.path.exists(file_name):
            print("🔄 Đã có CSV → tiến hành merge với dữ liệu cũ...")

            try:
                if os.path.getsize(file_name) == 0:
                    print("⚠️ CSV cũ rỗng, bỏ qua.")
                    existing = pd.DataFrame()
                else:
                    existing = pd.read_csv(file_name)
                    existing['Date'] = pd.to_datetime(existing['Date'], errors='coerce')
                    existing.dropna(subset=['Date'], inplace=True)
                    existing.drop_duplicates(subset=['Date'], keep='last', inplace=True)
            except Exception as e:
                print(f"❌ Lỗi đọc CSV cũ: {e}")
                existing = pd.DataFrame()

            # Gộp dữ liệu
            combined = pd.concat([existing, data])
            combined.drop_duplicates(subset=['Date'], keep='last', inplace=True)
            combined.sort_values(by='Date', inplace=True)
            combined.to_csv(file_name, index=False)
            print(f"✅ CSV updated thành công: {file_name}")
            return combined
        else:
            # Nếu chưa có file → tạo mới
            data.sort_values(by='Date', inplace=True)
            data.to_csv(file_name, index=False)
            print(f"✅ CSV created: {file_name}")
            return data

    except Exception as e:
        print(f"❌ Lỗi khi xử lý/lưu dữ liệu CSV: {e}")
        return pd.DataFrame()

