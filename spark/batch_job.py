from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, lit, row_number
from pyspark.sql.window import Window
import sys
import logging

# ==================== ЛОГИРОВАНИЕ ====================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== ПАРАМЕТРЫ ====================
if len(sys.argv) != 2:
    logger.error("Usage: spark-submit batch_job.py <processing_date>")
    sys.exit(1)

processing_date = sys.argv[1]  # Формат: "2025-11-09"
logger.info(f"🚀 Starting batch job for date: {processing_date}")

# ==================== SPARK SESSION ====================
spark = SparkSession.builder \
    .appName(f"DailySubscriberAggregation-{processing_date}") \
    .config("spark.jars.packages", 
            "org.postgresql:postgresql:42.7.1") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==================== JDBC ПАРАМЕТРЫ ====================
pg_url = "jdbc:postgresql://postgres:5432/telecom_db" 
pg_properties = {
    "user": "telecom_user",
    "password": "telecom_pass",
    "driver": "org.postgresql.Driver",
    "socketTimeout": "600",  # Таймаут сокета (10 минут)
    "loginTimeout": "60",    # Таймаут входа
    "sslmode": "disable"
}

# ==================== ЧТЕНИЕ ДАННЫХ ====================
logger.info("📊 Reading real-time metrics from PostgreSQL...")
query = f"""
    (SELECT * FROM real_time_metrics 
     WHERE DATE(window_start) = '{processing_date}') AS daily_metrics
"""

try:
    metrics_df = spark.read.jdbc(url=pg_url, table=query, properties=pg_properties)
    logger.warning("⚠️  сюда зашел")
    if metrics_df.rdd.isEmpty():
        logger.warning(f"⚠️  No metrics found for date {processing_date}")
        logger.warning("⚠️  ты лох")
        spark.stop()
        sys.exit(0)
    
    logger.info(f"✅ Found {metrics_df.count()} records for {processing_date}")
    
except Exception as e:
    logger.error(f"❌ Error reading data: {e}")
    spark.stop()
    sys.exit(1)

# ==================== АГРЕГАЦИЯ ПО РЕГИОНАМ ====================
logger.info("📈 Aggregating metrics by region...")
region_metrics = metrics_df.groupBy("region", "event_type").agg(
    spark_sum("event_count").alias("total_events"),
    spark_sum("total_duration").alias("total_duration_seconds"),
    spark_sum("total_data_mb").alias("total_data_mb"),
    spark_sum("total_amount").alias("total_amount"),
    avg("total_duration").alias("avg_duration_seconds"),
    avg("total_data_mb").alias("avg_data_per_user_mb")
).withColumn("total_duration_hours", col("total_duration_seconds") / 3600) \
 .withColumn("total_data_tb", col("total_data_mb") / 1024 / 1024) \
 .withColumn("date", lit(processing_date).cast("date")) \
 .select(
     "date", "event_type", "region",
     "total_events", "total_duration_hours", "total_data_tb",
     "total_amount", "avg_duration_seconds", "avg_data_per_user_mb"
 )

# ==================== ОБЩИЕ МЕТРИКИ ====================
logger.info("📊 Aggregating general metrics...")
general_metrics = metrics_df.groupBy("event_type").agg(
    spark_sum("event_count").alias("total_events"),
    spark_sum("total_duration").alias("total_duration_seconds"),
    spark_sum("total_data_mb").alias("total_data_mb"),
    spark_sum("total_amount").alias("total_amount"),
    avg("total_duration").alias("avg_duration_seconds"),
    avg("total_data_mb").alias("avg_data_per_user_mb")
).withColumn("total_duration_hours", col("total_duration_seconds") / 3600) \
 .withColumn("total_data_tb", col("total_data_mb") / 1024 / 1024) \
 .withColumn("region", lit("ALL")) \
 .withColumn("date", lit(processing_date).cast("date")) \
 .select(
     "date", "event_type", "region",
     "total_events", "total_duration_hours", "total_data_tb",
     "total_amount", "avg_duration_seconds", "avg_data_per_user_mb"
 )

# ==================== САМЫЙ АКТИВНЫЙ РЕГИОН ====================
logger.info("🏆 Finding most active region...")
window_spec = Window.orderBy(col("total_events").desc())
active_region_df = region_metrics.withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .select(
        lit(processing_date).cast("date").alias("date"),
        lit("most_active").alias("event_type"),
        col("region"),
        col("total_events"),
        col("total_duration_hours"),
        col("total_data_tb"),
        col("total_amount"),
        col("avg_duration_seconds"),
        col("avg_data_per_user_mb")
    )

# ==================== ОБЪЕДИНЕНИЕ ====================
final_df = region_metrics.union(general_metrics).union(active_region_df)

logger.info(f"📝 Total aggregated records: {final_df.count()}")

# ==================== ИДЕМПОТЕНТНАЯ ЗАПИСЬ ====================
logger.info("💾 Writing results to daily_stats table...")

try:
    # Шаг 1: Удаляем старые данные за эту дату
    logger.info(f"🗑️  Deleting old data for date {processing_date}...")
    delete_query = f"DELETE FROM daily_stats WHERE date = '{processing_date}'"
    
    # Используем JDBC для удаления
    from py4j.java_gateway import java_import
    java_import(spark._jvm, "java.sql.DriverManager")
    
    conn = spark._jvm.DriverManager.getConnection(pg_url, pg_properties["user"], pg_properties["password"])
    stmt = conn.createStatement()
    deleted_rows = stmt.executeUpdate(delete_query)
    stmt.close()
    conn.close()
    
    logger.info(f"✅ Deleted {deleted_rows} old records")

    # Шаг 2: Записываем новые данные
    final_df.repartition(1).write \
        .format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", "daily_stats") \
        .option("user", pg_properties["user"]) \
        .option("password", pg_properties["password"]) \
        .option("driver", pg_properties["driver"]) \
        .mode("append") \
        .save()
    
    logger.info(f"✅ Successfully wrote {final_df.count()} records to daily_stats")
    
except Exception as e:
    logger.error(f"❌ Error writing data: {e}")
    spark.stop()
    sys.exit(1)

logger.info("🎉 Batch job completed successfully!")
spark.stop()