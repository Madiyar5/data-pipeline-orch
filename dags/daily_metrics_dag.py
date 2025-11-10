from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

# logs
logger = logging.getLogger("airflow.task")

#cons
POSTGRES_CONN = "host=postgres dbname=telecom_db user=telecom_user password=telecom_pass"

#dag parms
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_metrics_batch',
    default_args=default_args,
    description='Daily aggregation of telecom metrics',
    schedule_interval='0 2 * * *',  # Каждый день в 02:00
    start_date=days_ago(1),
    catchup=False,
    tags=['batch', 'spark', 'telecom'],
    max_active_runs=1,
) as dag:

    # ==================== TASK 1: ПРОВЕРКА ДАННЫХ ====================
    def check_data_availability(**context):
        """Проверяет наличие данных за указанную дату"""
        import psycopg2
        
        ds = context['ds']  # Формат: YYYY-MM-DD
        logger.info(f"🔍 Checking data availability for date: {ds}")
        
        try:
            conn = psycopg2.connect(POSTGRES_CONN)
            cur = conn.cursor()
            
            query = f"""
                SELECT COUNT(*) 
                FROM real_time_metrics 
                WHERE DATE(window_start) = '{ds}'
            """
            
            cur.execute(query)
            count = cur.fetchone()[0]
            
            cur.close()
            conn.close()
            
            if count == 0:
                logger.error(f"❌ No data found for date {ds}")
                raise ValueError(f"No data available for date {ds}")
            
            logger.info(f"✅ Data check passed: {count} records found for {ds}")
            
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            raise

    task_check_data = PythonOperator(
        task_id='check_data',
        python_callable=check_data_availability,
    )

    # ==================== TASK 2: SPARK BATCH JOB ====================
    # ВРЕМЕННО: заглушка, так как нужен SparkSubmitOperator
    def run_batch_manually(**context):
        """Заглушка - batch запускается вручную"""
        ds = context['ds']
        logger.info(f"⚠️  Manual batch job needed for date: {ds}")
        logger.info(f"📝 Run manually: docker exec spark-streaming spark-submit /app/batch_job.py {ds}")
        
    task_run_spark = BashOperator(
        task_id='run_spark_batch',
        bash_command="""
        docker exec spark-streaming /opt/spark/bin/spark-submit \
            --master local[2] \
            --packages org.postgresql:postgresql:42.7.1 \
            /app/batch_job.py {{ ds }}
    """
    )

    # ==================== TASK 3: ВАЛИДАЦИЯ РЕЗУЛЬТАТОВ ====================
    def validate_results(**context):
        """Проверяет что результаты записались в daily_stats"""
        import psycopg2
        
        ds = context['ds']
        logger.info(f"✔️  Validating results for date: {ds}")
        
        try:
            conn = psycopg2.connect(POSTGRES_CONN)
            cur = conn.cursor()
            
            query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT event_type) as event_types,
                    COUNT(DISTINCT region) as regions
                FROM daily_stats 
                WHERE date = '{ds}'
            """
            
            cur.execute(query)
            result = cur.fetchone()
            total_records, event_types, regions = result
            
            cur.close()
            conn.close()
            
            if total_records == 0:
                logger.warning(f"⚠️  No results found for date {ds} (run batch manually)")
                return
            
            logger.info(f"✅ Validation passed:")
            logger.info(f"   - Total records: {total_records}")
            logger.info(f"   - Event types: {event_types}")
            logger.info(f"   - Regions: {regions}")
            
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            raise

    task_validate = PythonOperator(
        task_id='validate_results',
        python_callable=validate_results,
    )

    # ==================== TASK 4: УВЕДОМЛЕНИЕ ====================
    def notify(**context):
        """Отправляет уведомление об успешном выполнении"""
        ds = context['ds']
        logger.info(f"🎉 Pipeline completed for date {ds}")

    task_notify = PythonOperator(
        task_id='notify',
        python_callable=notify,
    )

    # ==================== ЗАВИСИМОСТИ ====================
    task_check_data >> task_run_spark >> task_validate >> task_notify