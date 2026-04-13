from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

TEAM          = "team2"
PROJECT       = "cabpulse360"
BATCH_ID      = "batch_01"
# BATCH_ID      = "batch_02"
# BATCH_ID      = "batch_03"
# BATCH_ID      = "batch_04"
# BATCH_ID      = "batch_05"

# BATCH_ID      = "batch_06"

NOTEBOOK_ROOT = "/Workspace/Shared/cabpulse360"

default_args = {
    "owner": TEAM,
    "depends_on_past": False,
    "retries": 0
}

# Serverless cluster config
serverless_config = {
    "run_name": "cabpulse360_run",
    "tasks": [
        {
            "task_key": "notebook_task",
            "notebook_task": {
                "notebook_path": "PLACEHOLDER",
                "base_parameters": {}
            },
            "job_cluster_key": None
        }
    ],
    "job_clusters": [],
    "environments": [
        {
            "environment_key": "Default",
            "spec": {
                "client": "2"
            }
        }
    ]
}

def make_serverless_task(task_id, notebook_path, params={}):
    return DatabricksSubmitRunOperator(
        task_id=task_id,
        databricks_conn_id="databricks_default",
        json={
            "run_name": task_id,
            "tasks": [
                {
                    "task_key": task_id,
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "base_parameters": params
                    },
                    "environment_key": "Default"
                }
            ],
            "environments": [
                {
                    "environment_key": "Default",
                    "spec": {
                        "client": "2"
                    }
                }
            ]
        }
    )

with DAG(
    dag_id=f"{PROJECT}_pipeline",
    default_args=default_args,
    description="CabPulse 360 end-to-end pipeline for team2",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=[PROJECT, TEAM],
) as dag:

    run_bronze = make_serverless_task(
        "run_bronze",
        f"{NOTEBOOK_ROOT}/01_bronze",
        {"batch_id": BATCH_ID}
    )

    run_silver = make_serverless_task(
        "run_silver",
        f"{NOTEBOOK_ROOT}/02_silver"
    )

    run_gold = make_serverless_task(
        "run_gold",
        f"{NOTEBOOK_ROOT}/03_gold"
    )

    run_dq = make_serverless_task(
        "run_dq",
        f"{NOTEBOOK_ROOT}/04_dq_checks",
        {"batch_id": BATCH_ID}
    )

    export_gold_to_s3 = make_serverless_task(
        "export_gold_to_s3",
        f"{NOTEBOOK_ROOT}/05_export_gold_to_s3"
    )

    refresh_snowflake = SQLExecuteQueryOperator(
        task_id="refresh_snowflake",
        conn_id="snowflake_default",
        sql="""
            USE DATABASE CABPULSE360_DB;
            USE SCHEMA STAGING;
            USE WAREHOUSE CABPULSE360_WH;
            TRUNCATE TABLE IF EXISTS STAGING.DIM_DATE;
            COPY INTO STAGING.DIM_DATE
              FROM @cabpulse360_stage/dim_date.parquet
              FILE_FORMAT=(TYPE='PARQUET')
              MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
            TRUNCATE TABLE IF EXISTS STAGING.DIM_VENDOR;
            COPY INTO STAGING.DIM_VENDOR
              FROM @cabpulse360_stage/dim_vendor.parquet
              FILE_FORMAT=(TYPE='PARQUET')
              MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
            TRUNCATE TABLE IF EXISTS STAGING.DIM_TAXI_ZONE;
            COPY INTO STAGING.DIM_TAXI_ZONE
              FROM @cabpulse360_stage/dim_taxi_zone.parquet
              FILE_FORMAT=(TYPE='PARQUET')
              MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
            TRUNCATE TABLE IF EXISTS STAGING.DIM_RATE_CODE;
            COPY INTO STAGING.DIM_RATE_CODE
              FROM @cabpulse360_stage/dim_rate_code.parquet
              FILE_FORMAT=(TYPE='PARQUET')
              MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
            TRUNCATE TABLE IF EXISTS STAGING.DIM_PAYMENT_TYPE;
            COPY INTO STAGING.DIM_PAYMENT_TYPE
              FROM @cabpulse360_stage/dim_payment_type.parquet
              FILE_FORMAT=(TYPE='PARQUET')
              MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
            TRUNCATE TABLE IF EXISTS STAGING.DIM_TIME_BLOCK;
            COPY INTO STAGING.DIM_TIME_BLOCK
              FROM @cabpulse360_stage/dim_time_block.parquet
              FILE_FORMAT=(TYPE='PARQUET')
              MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
            TRUNCATE TABLE IF EXISTS STAGING.FACT_TRIPS;
            COPY INTO STAGING.FACT_TRIPS
              FROM @cabpulse360_stage/fact_trips.parquet
              FILE_FORMAT=(TYPE='PARQUET')
              MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
            TRUNCATE TABLE IF EXISTS STAGING.FACT_DAILY_ZONE_SUMMARY;
            COPY INTO STAGING.FACT_DAILY_ZONE_SUMMARY
              FROM @cabpulse360_stage/fact_daily_zone_summary.parquet
              FILE_FORMAT=(TYPE='PARQUET')
              MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
        """
    )

    run_bronze >> run_silver >> run_gold >> run_dq >> export_gold_to_s3 >> refresh_snowflake