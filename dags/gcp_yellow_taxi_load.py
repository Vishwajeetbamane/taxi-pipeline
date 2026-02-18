from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime
from airflow.exceptions import AirflowException
import logging


default_args = {
    'owner': 'Vishwajeet',
    'retries':'0',
}

log = logging.getLogger("airflow.task")


from datetime import datetime

def base_file_path(params, data_interval_start):
    year = int(params.get("year") or data_interval_start.year)
    month = int(params.get("month") or data_interval_start.month)
    current_year = datetime.now().year

    if not (2019 <= year <= datetime.now().year):
        log.error(f"Invalid year detected: {year} > {current_year}")
        raise AirflowException(
            f"Invalid year {year}. Cannot be greater than current year {current_year}.")

    if not (1 <= month <= 12):
        log.error(f"Invalid month detected: {month}")
        raise AirflowException(
            f"Invalid year {month}. Month must be between 1 and 12.")


    return f"yellow_tripdata_{year}-{month:02d}"

with DAG(
    dag_id ='gcp_taxi_ETL',
    default_args=default_args,
    start_date=datetime(2018, 12, 12),
    schedule= "@monthly",
    max_active_runs = 2

    params = {
        "year":Param(
            title= "enter the Year",
            type = ["null", "integer"],
            default = None
        ),
        "month":Param(
            title= "enter the Month",
            type = ["null", "integer"],
            default = None
                        
        )
    },

    user_defined_macros = {
        "base_file_path" : base_file_path,
        "bucket" : "dt_yellow_trip_data",
        "GCP_PROJECT_ID" : "dtc-course-486211",
        "BQ_DATASET" : "taxi_dataset"
    }
    
) as dag:

    pull_file = BashOperator(

        task_id="pull_csv",
        bash_command="wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{{ base_file_path(params, data_interval_start) }}.csv.gz | gunzip > /tmp/{{ base_file_path(params, data_interval_start) }}.csv"        
        )

    delete_if_exists = GCSDeleteObjectsOperator(
        task_id="delete_if_exists",
        bucket_name  = "{{ bucket }}",
        prefix = "{{ base_file_path(params, data_interval_start) }}.csv",
        gcp_conn_id = "conn_gcp"
        ) 
    
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        gcp_conn_id = "conn_gcp",
        src = "/tmp/{{ base_file_path(params, data_interval_start) }}.csv",
        dst = "{{ base_file_path(params, data_interval_start) }}.csv",
        bucket= "{{ bucket }}"
        )
    
    

    create_main_table_structure = BigQueryInsertJobOperator(
        task_id="create_main_table_structure",
        gcp_conn_id = "conn_gcp",
        project_id = "{{ GCP_PROJECT_ID }}",
        configuration={
            "query": {
                "query": "{% include 'sql/table_structure_query.sql' %}",
                "useLegacySql": False,
                "priority": "BATCH",
                }
            }
        )

    create_external_table = BigQueryInsertJobOperator(
        task_id="create_external_table",
        gcp_conn_id = "conn_gcp",
        project_id = "{{ GCP_PROJECT_ID }}",
        configuration={
            "query": {
                "query": "{% include 'sql/table_external_create.sql' %}",
                "useLegacySql": False,
                "priority": "BATCH",
                }
            }
        )

    create_tmp_table = BigQueryInsertJobOperator(
        task_id="create_tmp_table",
        gcp_conn_id = "conn_gcp",
        project_id = "{{ GCP_PROJECT_ID }}",
        configuration={
            "query": {
                "query": "{% include 'sql/table_tmp_create.sql' %}",
                "useLegacySql": False,
                "priority": "BATCH",
                }
            }
        )

    merge_table = BigQueryInsertJobOperator(
        task_id="merge_table",
        gcp_conn_id = "conn_gcp",
        project_id = "{{ GCP_PROJECT_ID }}",
        configuration={
            "query": {
                "query": "{% include 'sql/table_merge.sql' %}",
                "useLegacySql": False,
                "priority": "BATCH",
                }
            }
        )
    
    cleanUp_tmp = BashOperator(

        task_id="cleanUp_tmp",
        bash_command="rm -f /tmp/{{ base_file_path(params, data_interval_start) }}.csv"
        )
    


    
    pull_file >> delete_if_exists >> upload_to_gcs >> create_main_table_structure >> create_external_table >> create_tmp_table >> merge_table >> cleanUp_tmp
