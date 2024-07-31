from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from pathlib import Path
from cosmos import DbtTaskGroup
from cosmos.config import ProjectConfig, ProfileConfig
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig

GCP_BUCKET_NAME = 'project-etl-saksit'
GCP_PROJECT_NAME = 'ornate-chemist-425808-e2'
GCP_DATASET_NAME = 'retail_project'
GCP_TABLE_NAME = 'raw_invoices'

#cosmos config
dbt_config = ProfileConfig(
    profile_name='dbt_project',
    target_name='dev',
    profiles_yml_filepath=Path('/home/airflow/myenv/dbt_project/profiles.yml')
)

dbt_project_config = ProjectConfig(
    dbt_project_path = '/home/airflow/myenv/dbt_project/'
)

default_args = {
    'owner': 'Saksit',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='retail_pipeline',
    default_args=default_args, 
    schedule_interval="@daily",
    tags=['project', 'retail']
) as dag:

    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id = 'upload_csv_to_gcs',
        src="/home/airflow/dataset/retail.csv", 
        dst='raw/retail.csv',
        bucket= GCP_BUCKET_NAME,
        gcp_conn_id="gcp",
        mime_type="text/csv"
    )

    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id = 'create_retail_dataset',
        dataset_id = GCP_DATASET_NAME,
        exists_ok=True,
        gcp_conn_id="gcp"
    )
    
    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id = 'gcs_to_bigquery',
        bucket=GCP_BUCKET_NAME,
        source_objects='raw/retail.csv',
        destination_project_dataset_table=f'{GCP_DATASET_NAME}.{GCP_TABLE_NAME}',
        schema_fields=[
            {'name': 'InvoiceNo', 'type': 'STRING', },
            {'name': 'StockCode', 'type': 'STRING'},
            {'name': 'Description', 'type': 'STRING'},
            {'name': 'Quantity', 'type': 'INTEGER'},
            {'name': 'InvoiceDate', 'type': 'STRING'},
            {'name': 'UnitPrice', 'type': 'FLOAT'},
            {'name': 'CustomerID', 'type': 'FLOAT'},
            {'name': 'Country', 'type': 'STRING'}
        ],
        source_format="CSV",
        gcp_conn_id="gcp",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE"
        #autodetect=True
    )

    transform = DbtTaskGroup(
        group_id='transform',
        project_config=dbt_project_config,
        profile_config=dbt_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )

    report = DbtTaskGroup(
        group_id='report',
        project_config=dbt_project_config,
        profile_config=dbt_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/report']
        )
    )

    #create depencies
    upload_csv_to_gcs >> create_retail_dataset >> gcs_to_bigquery >> transform >> report
