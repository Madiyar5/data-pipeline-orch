from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, sum as spark_sum, 
    avg, current_timestamp, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, FloatType, TimestampType
)

print("=" * 60)
print("🚀 Запуск Telecom Real-time Streaming Analytics")
print("=" * 60)

#spark session

spark = SparkSession.builder \
    .appName("TelecomStreamingAnalytics") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.7.1") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark Session создан успешно!")

#data

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("msisdn", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_subtype", StringType(), True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("data_mb", FloatType(), True),
    StructField("amount", FloatType(), True),
    StructField("region", StringType(), True),
    StructField("cell_tower_id", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

print("✅ Схема событий определена")

#kafka

print("🔌 Подключаемся к Kafka...")

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "telecom_events") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print("✅ Подключились к Kafka топику 'telecom_events'")

#json parsing

events_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), event_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp")))

print("✅ Парсинг JSON событий настроен")

#aggregesion by windows

print("📊 Настраиваем агрегацию по 1-минутным окнам...")

aggregated_df = events_df \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("event_type"),
        col("region")
    ) \
    .agg(
        count("*").alias("event_count"),
        spark_sum("duration_seconds").alias("total_duration"),
        spark_sum("data_mb").alias("total_data_mb"),
        spark_sum("amount").alias("total_amount")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("event_type"),
        col("event_count"),
        col("total_duration"),
        col("total_data_mb"),
        col("total_amount"),
        col("region"),
        current_timestamp().alias("processed_at")
    )

print("✅ Агрегация настроена: окна 1 минута, watermark 2 минуты")

#write in postgresql 

jdbc_url = "jdbc:postgresql://postgres:5432/telecom_db"
db_properties = {
    "user": "telecom_user",
    "password": "telecom_pass",
    "driver": "org.postgresql.Driver"
}

def write_to_postgres(batch_df, batch_id):
    """
    batch в PostgreSQL
    """
    if batch_df.count() == 0:
        print(f"⚠️  Batch {batch_id}: нет данных для записи")
        return
    
    try:
        # Показываем что записываем 
        print(f"\n📝 Batch {batch_id}: записываем {batch_df.count()} строк")
        batch_df.show(5, truncate=False)
        
        # Записываем в PostgreSQL
        batch_df.write \
            .jdbc(
                url=jdbc_url,
                table="real_time_metrics",
                mode="append",
                properties=db_properties
            )
        
        print(f"✅ Batch {batch_id}: успешно записано в PostgreSQL\n")
        
    except Exception as e:
        print(f"❌ Ошибка записи batch {batch_id}: {e}\n")

print("🔌 Подключение к PostgreSQL настроено")

#streaming query

print("=" * 60)
print("🚀 STREAMING QUERY ЗАПУЩЕН!")
print("=" * 60)
print("📊 Обрабатываем события каждые 30 секунд")
print("💾 Записываем агрегированные метрики в PostgreSQL")
print("⏰ Окна агрегации: 1 минута")
print("💧 Watermark: 2 минуты (для late arrivals)")
print("\nНажми Ctrl+C для остановки\n")
print("=" * 60)

query = aggregated_df \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

# Ждем завершения
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n\n🛑 Получен сигнал остановки...")
    query.stop()
    print("✅ Streaming Query остановлен корректно")
    spark.stop()
    print("✅ Spark Session закрыт")