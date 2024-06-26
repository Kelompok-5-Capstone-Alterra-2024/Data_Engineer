from datetime import datetime, timedelta
from airflow import DAG
from custom_operator.etl_all_operator import ExtractData, CleaningData, TransformationDataWarehouseSchema, LoadDatabaseLocal, LoadFirebase, LoadGoogleBigQuery


default_args = {
    "owner":"Kelompok 5 Capstone DE",
    "retries":4,
    "retry_delay":timedelta(minutes=2),
}

with DAG(
    dag_id="peduli_pintar_elt_all_dags_v2",
    default_args=default_args,
    description="Proses ETL Capstone Project DE",
    start_date=datetime(2024, 6, 23),
    schedule_interval="@daily",
) as dag:
    
    extract_data = ExtractData(
        task_id='extract_data',
        mysql_conn_id='EXTERNAL_DB_CONN_CAPSTONE_DE',
        database='capstone5',
        python_callable=ExtractData.execute,
        provide_context=True,
    )

    cleaning_raw_data = CleaningData(
        task_id='cleaning_raw_data',
        python_callable=CleaningData.execute,
        provide_context=True
    )
    
    load_firebase = LoadFirebase(
        task_id='load_firebase',
        python_callable=LoadFirebase.execute,
        provide_context=True,
    )
    
    transform_dw_schema = TransformationDataWarehouseSchema(
        task_id='transform_dw_schema',
        python_callable=TransformationDataWarehouseSchema.execute,
        provide_context=True,
    )
    
    load_db_local = LoadDatabaseLocal(
        task_id='load_db_local',
        python_callable=LoadDatabaseLocal.execute,
        provide_context=True,
        mysql_conn_id='mysql_default',
    )
    
    load_gbq = LoadGoogleBigQuery(
        task_id='load_gbq',
        python_callable=LoadGoogleBigQuery.execute,
        provide_context=True,
        gcp_conn='gcp_conn_capstone_de',
    )
    
    
    extract_data >> [cleaning_raw_data, load_firebase] 
    cleaning_raw_data >> transform_dw_schema >> [load_db_local, load_gbq]