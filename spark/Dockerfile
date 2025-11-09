FROM apache/spark:3.5.5-scala2.12-java17-python3-ubuntu

USER root

# Установка дополнительных зависимостей
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Установка Python пакетов
RUN pip3 install --no-cache-dir \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.9

# Копируем streaming job
COPY streaming_job.py /app/streaming_job.py

WORKDIR /app