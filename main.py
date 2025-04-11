from airflow import DAG
from airflow.providers.google.cloud.operators.functions import CloudFunctionInvokeFunctionOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Configurable values
PROJECT_ID = 'neural-guard-454017-q6'
REGION = 'us-central1'
FTP_FUNCTION_NAME = 'ftpToGcsFunction'
BUCKET = 'incoming_ftp'
BQ_DATASET = 'anz_ftp_sftp'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ftp_to_gcs_and_load_to_two_tables',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['ftp', 'gcs', 'bq'],
) as dag:

    # ✅ Step 1: Trigger to fetch retail_transactions.csv
    trigger_retail_csv = CloudFunctionInvokeFunctionOperator(
        task_id='trigger_retail_csv',
        project_id=PROJECT_ID,
        location=REGION,
        function_id=FTP_FUNCTION_NAME,
        input_data={'filename': 'economic_indicators_external.csv'},
    )

    # ✅ Step 2: Trigger to fetch terminal_info.csv
    trigger_terminal_csv = CloudFunctionInvokeFunctionOperator(
        task_id='trigger_terminal_csv',
        project_id=PROJECT_ID,
        location=REGION,
        function_id=FTP_FUNCTION_NAME,
        input_data={'filename': 'terminal_info.csv'},
    )

    # ✅ Step 3: Load retail_transactions.csv to BigQuery
    load_csv1_to_bq = GCSToBigQueryOperator(
        task_id='load_csv1_to_bq',
        bucket=BUCKET,
        source_objects=['ftp-data/macroeconomics data-20250408-151835.csv'],
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET}.economic_indicators_external',
        schema_fields=[
            {'name': 'txn_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'txn_date', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'terminal_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        ],
        skip_leading_rows=1,
        source_format='CSV',
        write_disposition='WRITE_APPEND',
    )

    # ✅ Step 4: Load terminal_info.csv to BigQuery
    load_csv2_to_bq = GCSToBigQueryOperator(
        task_id='load_csv2_to_bq',
        bucket=BUCKET,
        source_objects=['ftp-data/retail_transactions-20250408-151835.csv'],
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET}.store_terminal_external',
        schema_fields=[
            {'name': 'terminal_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'store_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'store_category', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'region', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'geo_lat', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'geo_long', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'last_updated', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        ],
        skip_leading_rows=1,
        source_format='CSV',
        write_disposition='WRITE_APPEND',
    )

    # ✅ Define task dependencies
    trigger_retail_csv >> load_csv1_to_bq
    trigger_terminal_csv >> load_csv2_to_bq
