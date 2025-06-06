# 📊 Big Data Pipeline for Real-Time Stock Analytics

**👨‍💻 Tác giả:** Nguyễn Trung Đức  
**💻 Môi trường phát triển:** Ubuntu 24.04

## 🛠️ Công nghệ sử dụng

<p align="center">
  <img src="https://www.python.org/static/community_logos/python-logo.png" alt="Python" height="50"/>
  <img src="https://upload.wikimedia.org/wikipedia/commons/0/0a/Apache_kafka-icon.svg" alt="Kafka" height="50"/>
  <img src="https://airflow.apache.org/docs/apache-airflow/stable/_images/pin_large.png" alt="Airflow" height="50"/>
  <img src="https://upload.wikimedia.org/wikipedia/commons/0/0e/Hadoop_logo.svg" alt="Hadoop" height="50"/>
  <img src="https://upload.wikimedia.org/wikipedia/commons/f/f3/Apache_Spark_logo.svg" alt="Spark" height="50"/>
  <img src="https://www.postgresql.org/media/img/about/press/elephant.png" alt="PostgreSQL" height="50"/>
  <img src="https://superset.apache.org/images/superset-logo-horiz.png" alt="Superset" height="50"/>
  <img src="https://www.docker.com/wp-content/uploads/2022/03/Moby-logo.png" alt="Docker" height="50"/>
  <img src="https://assets.ubuntu.com/v1/29985a98-ubuntu-logo32.png" alt="Ubuntu" height="50"/>
</p>

---

## 🚀 Giới thiệu dự án

Dự án xây dựng một hệ thống **Data Pipeline** hoàn chỉnh giúp:
- Tự động thu thập dữ liệu chứng khoán hằng ngày theo thời gian thực.
- Xử lý dữ liệu với Spark.
- Truyền tải dữ liệu qua Kafka.
- Lưu trữ và phân tích với Hadoop, PostgreSQL và Superset.

Mục tiêu là đảm bảo hệ thống **mở rộng**, **tự động hóa cao**, và **phân tích dữ liệu hiệu quả** theo chuẩn Big Data hiện đại.

---

## 🧱 Thành phần kiến trúc chính

| Thành phần         | Vai trò                                                                 |
|--------------------|-------------------------------------------------------------------------|
| **Kafka**          | Giao tiếp streaming, truyền tải dữ liệu theo thời gian thực.            |
| **PySpark**        | Xử lý dữ liệu phân tán tốc độ cao.                                      |
| **Hadoop HDFS**    | Lưu trữ dữ liệu lớn theo dạng phân tán.                                 |
| **Airflow**        | Orchestration và lập lịch thực thi các tác vụ ETL.                      |
| **PostgreSQL**     | Cơ sở dữ liệu lưu trữ dữ liệu mart, dùng cho phân tích và báo cáo.      |
| **Superset**       | Công cụ trực quan hóa dữ liệu mạnh mẽ, kết nối PostgreSQL để tạo Dashboard. |
| **Docker**         | Đóng gói toàn bộ hệ thống trong container nhất quán, dễ triển khai.     |

---

## 📁 Cấu trúc thư mục

---

## 🖼️ Kiến trúc hệ thống Pipeline

![Pipeline Kiến trúc](images/pipeline_bigdata.svg)

Luồng dữ liệu như sau:

1. **Extract**: Tải dữ liệu chứng khoán qua `yfinance`.
2. **Transform**: Làm sạch & xử lý qua các tầng Staging → Core → Business → Mart.
3. **Load**:
   - Lưu bản Parquet.
   - Gửi bản Business vào Kafka topic.
   - Đẩy bản Mart vào PostgreSQL.
4. **Visualize**: Truy cập từ Superset để phân tích/giám sát.

---

## 📊 Trực quan hóa với Apache Superset

- **Nguồn dữ liệu**: kết nối trực tiếp với PostgreSQL chứa dữ liệu mart.
- **Dashboard**: dễ dàng tạo biểu đồ xu hướng chứng khoán, khối lượng giao dịch theo tháng, v.v.
- **Phân quyền**: tích hợp người dùng xem dashboard theo role.

---

## ⚙️ Hướng dẫn sử dụng

### 1. 🐳 Khởi động môi trường

```bash
docker compose up --build
