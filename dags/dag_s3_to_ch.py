"""
DAG 2: dag_s3_to_ch
ExternalTaskSensor → Spark → ClickHouse

start >> wait_for_dag_nasa_to_s3 >> prepare_spark_env >> spark_s3_to_ch >> verify_clickhouse_load >> end
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Параметры
# ---------------------------------------------------------------------------
MINIO_BUCKET = Variable.get("MINIO_BUCKET", default_var="raw-data")
KEY_PREFIX   = Variable.get("KEY_PREFIX",   default_var="asteroids")
CH_TABLE     = Variable.get("CH_TABLE",     default_var="nasa.asteroids")
CH_MIN_ROWS  = int(Variable.get("CH_MIN_ROWS", default_var="1"))

SPARK_JOB_PATH = "/opt/airflow/spark/spark_job.py"
SPARK_JARS     = ",".join([
    "/opt/spark/extra-jars/hadoop-aws-3.3.4.jar",
    "/opt/spark/extra-jars/aws-java-sdk-bundle-1.12.262.jar",
    "/opt/spark/extra-jars/clickhouse-jdbc-0.7.1-all.jar",
])

DEFAULT_ARGS = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,
}


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------
def prepare_spark_env(**context):
    """Собирает env из Airflow Connections и пушит в XCom."""
    logical_date = context["logical_date"]
    date_part    = logical_date.strftime("%Y/%m/%d")

    minio_conn = BaseHook.get_connection("minio_s3")
    ch_conn    = BaseHook.get_connection("clickhouse_default")

    bucket  = minio_conn.extra_dejson.get("bucket_name") or MINIO_BUCKET
    s3_path = f"s3a://{bucket}/{KEY_PREFIX}/{date_part}/data.parquet"

    env = {
        "MINIO_ENDPOINT":   minio_conn.host,
        "MINIO_ACCESS_KEY": minio_conn.login,
        "MINIO_SECRET_KEY": minio_conn.password,
        "CH_HOST":          ch_conn.host,
        "CH_PORT":          str(ch_conn.port or 8123),
        "CH_DATABASE":      ch_conn.schema or "nasa",
        "CH_USER":          ch_conn.login or "default",
        "CH_PASSWORD":      ch_conn.password or "",
        "S3_PATH":          s3_path,
    }

    context["ti"].xcom_push(key="spark_env", value=env)
    log.info("Spark env готов, s3_path=%s", s3_path)


def verify_load(**context):
    """Проверяет что строки за нужный день появились в ClickHouse."""
    import clickhouse_connect

    logical_date = context["logical_date"]
    date_str     = logical_date.strftime("%Y-%m-%d")
    ch_conn      = BaseHook.get_connection("clickhouse_default")

    client = clickhouse_connect.get_client(
        host=ch_conn.host,
        port=ch_conn.port or 8123,
        database=ch_conn.schema or "nasa",
        username=ch_conn.login or "default",
        password=ch_conn.password or "",
    )

    result = client.query(f"""
        SELECT count() AS cnt
        FROM {CH_TABLE} FINAL
        WHERE close_approach_date = '{date_str}'
    """)

    row_count = result.first_row[0]
    log.info("Строк в CH за %s: %d (минимум: %d)", date_str, row_count, CH_MIN_ROWS)

    if row_count < CH_MIN_ROWS:
        raise ValueError(
            f"Verification failed: {CH_TABLE} за {date_str} "
            f"содержит {row_count} строк, ожидалось ≥ {CH_MIN_ROWS}"
        )

    log.info("Verification OK ✓")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dag_s3_to_ch",
    default_args=DEFAULT_ARGS,
    description="ExternalTaskSensor → SparkSubmit → ClickHouse",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2025, 5, 19, tz="UTC"),
    catchup=True,
    max_active_runs=3,
    tags=["nasa", "spark", "clickhouse"],
) as dag:

    start = EmptyOperator(task_id="start")

    t_sensor = ExternalTaskSensor(
        task_id="wait_for_dag_nasa_to_s3",
        external_dag_id="dag_nasa_to_s3",
        external_task_id=None,
        execution_date_fn=lambda dt: dt,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60 * 2,
        check_existence=True,
    )

    t_prepare = PythonOperator(
        task_id="prepare_spark_env",
        python_callable=prepare_spark_env,
    )

    t_spark = SparkSubmitOperator(
        task_id="spark_s3_to_ch",
        conn_id="spark_default",
        application=SPARK_JOB_PATH,
        jars=SPARK_JARS,
        application_args=[
            "--s3-path",  "{{ ti.xcom_pull(task_ids='prepare_spark_env', key='spark_env')['S3_PATH'] }}",
            "--ch-table", CH_TABLE,
        ],
        env_vars={
            "MINIO_ENDPOINT":   "{{ ti.xcom_pull(task_ids='prepare_spark_env', key='spark_env')['MINIO_ENDPOINT'] }}",
            "MINIO_ACCESS_KEY": "{{ ti.xcom_pull(task_ids='prepare_spark_env', key='spark_env')['MINIO_ACCESS_KEY'] }}",
            "MINIO_SECRET_KEY": "{{ ti.xcom_pull(task_ids='prepare_spark_env', key='spark_env')['MINIO_SECRET_KEY'] }}",
            "CH_HOST":          "{{ ti.xcom_pull(task_ids='prepare_spark_env', key='spark_env')['CH_HOST'] }}",
            "CH_PORT":          "{{ ti.xcom_pull(task_ids='prepare_spark_env', key='spark_env')['CH_PORT'] }}",
            "CH_DATABASE":      "{{ ti.xcom_pull(task_ids='prepare_spark_env', key='spark_env')['CH_DATABASE'] }}",
            "CH_USER":          "{{ ti.xcom_pull(task_ids='prepare_spark_env', key='spark_env')['CH_USER'] }}",
            "CH_PASSWORD":      "{{ ti.xcom_pull(task_ids='prepare_spark_env', key='spark_env')['CH_PASSWORD'] }}",
        },
        execution_timeout=timedelta(hours=1),
        verbose=True,
    )

    t_verify = PythonOperator(
        task_id="verify_clickhouse_load",
        python_callable=verify_load,
    )

    end = EmptyOperator(task_id="end")

    start >> t_sensor >> t_prepare >> t_spark >> t_verify >> end