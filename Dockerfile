FROM apache/airflow:2.9.0

USER root

# Cài Java 17 + các công cụ hỗ trợ (vim, curl,...)
RUN apt-get update && \
    apt-get install -y software-properties-common curl gnupg vim netcat-openbsd && \
    add-apt-repository "deb http://deb.debian.org/debian bullseye main contrib non-free" && \
    apt-get update && \
    apt-get install -y openjdk-17-jdk tree && \
    apt-get clean

# Thiết lập JAVA_HOME cho PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow

# Cài Python packages
COPY requirements.txt /
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt
