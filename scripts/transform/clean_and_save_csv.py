# import os
# import pandas as pd

# def transform_stock_data(data: pd.DataFrame, file_name: str) -> pd.DataFrame:
#     # Chuẩn hóa dữ liệu mới
#     data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
#     data.dropna(subset=['Date'], inplace=True)
#     data.drop_duplicates(subset=['Date'], keep='last', inplace=True)
    
#     # Tạo thư mục nếu chưa có
#     os.makedirs(os.path.dirname(file_name), exist_ok=True)

#     if os.path.exists(file_name):
#         print("🔄 Merging with existing CSV...")
#         existing = pd.read_csv(file_name)
#         existing['Date'] = pd.to_datetime(existing['Date'], errors='coerce')
#         existing.dropna(subset=['Date'], inplace=True)
#         existing.drop_duplicates(subset=['Date'], keep='last', inplace=True)

#         # Gộp và xóa trùng theo 'Date'
#         combined = pd.concat([existing, data])
#         combined.drop_duplicates(subset=['Date'], keep='last', inplace=True)
#         combined.sort_values(by='Date', inplace=True)

#         # Ghi đè vào cùng 1 file
#         combined.to_csv(file_name, index=False)
#         print(f"✅ CSV updated: {file_name}")
#         return combined
#     else:
#         # Nếu chưa tồn tại, ghi mới
#         data.sort_values(by='Date', inplace=True)
#         data.to_csv(file_name, index=False)
#         print(f"✅ CSV created: {file_name}")
#         return data

import os
import pandas as pd

def transform_stock_data(data: pd.DataFrame, file_name: str) -> pd.DataFrame:
    """
    Làm sạch và lưu dữ liệu chứng khoán vào file CSV.
    - Nếu file đã tồn tại: gộp và loại bỏ trùng lặp theo 'Date'
    - Nếu chưa: tạo mới file
    - Đảm bảo ngày hợp lệ, sort theo ngày tăng dần

    Args:
        data (pd.DataFrame): Dữ liệu mới thu thập
        file_name (str): Đường dẫn đến file CSV đầu ra

    Returns:
        pd.DataFrame: Dữ liệu đã được hợp nhất và lưu
    """

    try:
        # 1. Chuẩn hóa dữ liệu mới
        print("🧹 Đang làm sạch dữ liệu mới...")
        data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
        data.dropna(subset=['Date'], inplace=True)
        data.drop_duplicates(subset=['Date'], keep='last', inplace=True)

        # Tạo thư mục nếu chưa tồn tại
        os.makedirs(os.path.dirname(file_name), exist_ok=True)

        if os.path.exists(file_name):
            print("🔄 Đã có CSV → tiến hành merge với dữ liệu cũ...")

            # 2. Đọc file cũ nếu có
            existing = pd.read_csv(file_name)
            existing['Date'] = pd.to_datetime(existing['Date'], errors='coerce')
            existing.dropna(subset=['Date'], inplace=True)
            existing.drop_duplicates(subset=['Date'], keep='last', inplace=True)

            # 3. Gộp dữ liệu mới với dữ liệu cũ
            combined = pd.concat([existing, data])
            combined.drop_duplicates(subset=['Date'], keep='last', inplace=True)
            combined.sort_values(by='Date', inplace=True)

            # 4. Ghi đè file CSV
            combined.to_csv(file_name, index=False)
            print(f"✅ CSV updated thành công: {file_name}")
            print(f"📈 Tổng số dòng sau cập nhật: {len(combined)}")
            return combined
        else:
            # 5. Nếu chưa có file → tạo mới
            print("📁 CSV chưa tồn tại → tạo mới.")
            data.sort_values(by='Date', inplace=True)
            data.to_csv(file_name, index=False)
            print(f"✅ CSV created: {file_name}")
            print(f"📈 Tổng số dòng: {len(data)}")
            return data

    except Exception as e:
        print(f"❌ Lỗi khi xử lý/lưu dữ liệu CSV: {e}")
        return pd.DataFrame()  # Trả về DF rỗng để không lỗi pipeline
