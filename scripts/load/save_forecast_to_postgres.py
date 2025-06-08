import psycopg2
from datetime import datetime

def save_forecast_to_postgres(symbol: str, current_price: float, predicted_return: float, predicted_price: float):
    try:
        conn = psycopg2.connect(
            dbname="airflow", user="airflow", password="airflow",
            host="postgres", port=5432
        )
        cursor = conn.cursor()

        forecast_month = datetime.today().replace(day=1)
        insert_sql = """
            INSERT INTO forecast_result (symbol, current_price, predicted_return, predicted_price, forecast_month)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(insert_sql, (symbol, current_price, predicted_return, predicted_price, forecast_month))
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Dự đoán đã được ghi vào bảng forecast_result.")
    except Exception as e:
        print(f"❌ Ghi vào PostgreSQL thất bại: {e}")
