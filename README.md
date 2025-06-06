# 📊 Big Data Pipeline for Stock Data

**Author:** Nguyễn Trung Đức  
**Technologies:** Hadoop · PySpark · Apache Kafka · Apache Airflow · Docker · Python  

## 🚀 Mô tả dự án

Dự án xây dựng một **data pipeline** hoàn chỉnh để thu thập, xử lý và lưu trữ dữ liệu chứng khoán theo thời gian thực. Hệ thống sử dụng các công nghệ Big Data hiện đại nhằm đảm bảo khả năng mở rộng, tính tự động và hiệu suất cao.

## 🧱 Kiến trúc tổng quan


- **Kafka:** Truyền tải dữ liệu thời gian thực.
- **PySpark:** Xử lý dữ liệu phân tán với Spark.
- **Hadoop HDFS:** Lưu trữ dữ liệu đầu ra.
- **Airflow:** Lập lịch và điều phối quy trình ETL.
- **Docker:** Môi trường triển khai đồng nhất và dễ kiểm soát.

## 📂 Cấu trúc thư mục

scripts
├── extract
│   ├── download_stock.py
│   ├── __init__.py
│   └── __pycache__
│       ├── download_stock.cpython-312.pyc
│       └── __init__.cpython-312.pyc
├── load
│   ├── __init__.py
│   ├── load_mart_to_postgres.py
│   ├── __pycache__
│   │   ├── __init__.cpython-312.pyc
│   │   ├── load_mart_to_postgres.cpython-312.pyc
│   │   ├── save_parquet.cpython-312.pyc
│   │   └── send_to_kafka.cpython-312.pyc
│   ├── save_parquet.py
│   └── send_to_kafka.py
├── main
│   ├── __init__.py
│   └── run_pipeline.py
└── transform
    ├── artifacts
    ├── business_to_mart.py
    ├── clean_and_save_csv.py
    ├── core_to_business.py
    ├── __init__.py
    ├── __pycache__
    │   ├── business_to_mart.cpython-312.pyc
    │   ├── clean_and_save_csv.cpython-312.pyc
    │   ├── core_to_business.cpython-312.pyc
    │   ├── __init__.cpython-312.pyc
    │   └── staging_to_core.cpython-312.pyc
    └── staging_to_core.py

## 🖼️ Kiến trúc Pipeline

![Pipeline Kiến trúc](images/pipeline_bigdata.svg)
