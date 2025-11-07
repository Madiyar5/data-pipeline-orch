-- =====================================================
-- Создаем пользователя airflow СНАЧАЛА
-- =====================================================

-- Создаем пользователя airflow (если его нет)
DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'airflow') THEN
      CREATE ROLE airflow LOGIN PASSWORD 'airflow';
   END IF;
END
$$;

-- =====================================================
-- Создаем БД для Airflow
-- =====================================================

-- Создаем БД airflow_db (если её нет) с владельцем airflow
SELECT 'CREATE DATABASE airflow_db OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec

-- =====================================================
-- Создаем таблицы в telecom_db (текущая БД)
-- =====================================================

-- Мы уже в telecom_db, просто создаем таблицы
CREATE TABLE IF NOT EXISTS events (
    event_id VARCHAR(255) PRIMARY KEY,
    msisdn VARCHAR(50),
    event_type VARCHAR(50),
    event_subtype VARCHAR(50),
    duration_seconds INTEGER,
    data_mb FLOAT,
    amount FLOAT,
    region VARCHAR(100),
    cell_tower_id INTEGER,
    timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS real_time_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    event_type VARCHAR(50),
    event_count BIGINT,
    total_duration BIGINT,
    total_data_mb FLOAT,
    total_amount FLOAT,
    region VARCHAR(100),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS anomalies (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(255),
    event_type VARCHAR(50),
    anomaly_type VARCHAR(100),
    anomaly_description TEXT,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_stats (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    event_type VARCHAR(50),
    region VARCHAR(100),
    total_events BIGINT,
    total_duration_hours FLOAT,
    total_data_tb FLOAT,
    total_amount FLOAT,
    avg_duration_seconds FLOAT,
    avg_data_per_user_mb FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, event_type, region)
);

-- Индексы для оптимизации
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_region ON events(region);

CREATE INDEX IF NOT EXISTS idx_metrics_window ON real_time_metrics(window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_metrics_type ON real_time_metrics(event_type);

CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_stats(date);
CREATE INDEX IF NOT EXISTS idx_daily_type ON daily_stats(event_type);