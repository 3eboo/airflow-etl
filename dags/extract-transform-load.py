from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

SPARK_MASTER = "spark://spark:7077"
POSTGRES_DRIVER_JAR = "/usr/local/spark/resources/jars/postgresql-9.4.1207.jar"
CSV_FILE_PATH = "/usr/local/spark/resources/data/target_data.csv"

###############################################
# DAG Definition
###############################################

now = datetime.now()
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}
dag = DAG(
    dag_id="extract-transform-load",
    description="This DAG loads file add backend_id field to pyspark df, compute several aggregate tables.",
    default_args=default_args,
    schedule_interval=timedelta(1)
)

extract_load_dataframe = SparkSubmitOperator(
    task_id="extract_load_dataframe",
    application="/usr/local/spark/app/extract_load_dataframe.py", # Spark application path created in airflow and spark cluster # noqa
    name="extract_load_dataframe",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master": SPARK_MASTER},
    application_args=[CSV_FILE_PATH],
    jars=POSTGRES_DRIVER_JAR,
    driver_class_path=POSTGRES_DRIVER_JAR,
    dag=dag
)

duration_per_region_transform = SparkSubmitOperator(
    task_id="duration_per_region_transform",
    application="/usr/local/spark/app/aggregate_sum_per_attributes.py",
    name="duration-per-region",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master": SPARK_MASTER},
    application_args=['regionid', 'duration'],
    jars=POSTGRES_DRIVER_JAR,
    driver_class_path=POSTGRES_DRIVER_JAR,
    dag=dag)

duration_per_device_transform = SparkSubmitOperator(
    task_id="duration_per_device_transform",
    application="/usr/local/spark/app/aggregate_sum_per_attributes.py",
    name="duration_per_device",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master": SPARK_MASTER},
    application_args=['device', 'duration'],
    jars=POSTGRES_DRIVER_JAR,
    driver_class_path=POSTGRES_DRIVER_JAR,
    dag=dag)

distinct_session_id_per_region_transform = SparkSubmitOperator(
    task_id="distinct_session_id_per_region_transform",
    application="/usr/local/spark/app/aggregate_distinct_count_per_attributes.py",
    name="distinct_session_id_per_region",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master": SPARK_MASTER},
    application_args=['regionid', 'unique_sess_id'],
    jars=POSTGRES_DRIVER_JAR,
    driver_class_path=POSTGRES_DRIVER_JAR,
    dag=dag)

distinct_session_id_per_device_transform = SparkSubmitOperator(
    task_id="distinct_session_id_per_device_transform",
    application="/usr/local/spark/app/aggregate_distinct_count_per_attributes.py",
    name="distinct_session_id_per_device",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master": SPARK_MASTER},
    application_args=['device', 'unique_sess_id'],
    jars=POSTGRES_DRIVER_JAR,
    driver_class_path=POSTGRES_DRIVER_JAR,
    dag=dag)

number_of_sessions_per_region_transform = SparkSubmitOperator(
    task_id="number_of_sessions_per_region_transform",
    application="/usr/local/spark/app/aggregate_count_per_attributes.py",
    name="number_of_sessions_per_region",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master": SPARK_MASTER},
    application_args=['regionid', 'unique_sess_id'],
    jars=POSTGRES_DRIVER_JAR,
    driver_class_path=POSTGRES_DRIVER_JAR,
    dag=dag)

number_of_sessions_per_device_transform = SparkSubmitOperator(
    task_id="number_of_sessions_per_device_transform",
    application="/usr/local/spark/app/aggregate_count_per_attributes.py",
    name="number_of_sessions_per_device",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master": SPARK_MASTER},
    application_args=['device', 'unique_sess_id'],
    jars=POSTGRES_DRIVER_JAR,
    driver_class_path=POSTGRES_DRIVER_JAR,
    dag=dag)

extract_load_dataframe >> [duration_per_device_transform, duration_per_region_transform,
                           distinct_session_id_per_device_transform, distinct_session_id_per_region_transform,
                           number_of_sessions_per_device_transform, number_of_sessions_per_region_transform]
