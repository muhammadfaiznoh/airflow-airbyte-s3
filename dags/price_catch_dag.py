from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.sensors.filesystem import FileSensor
from airflow.datasets import Dataset
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

import pendulum

AIRBYTE_CONNECTION_ID_ITEM = '6cdcde32-8634-4328-a2ca-5d544995f33d'
AIRBYTE_CONNECTION_ID_PREMISE = 'b42d0771-9d86-4b65-b8ff-0db8799463dc'
RAW_PRODUCTS_FILE = '/tmp/airbyte_local/json_from_faker/_airbyte_raw_products.jsonl'
COPY_OF_RAW_PRODUCTS = '/tmp/airbyte_local/json_from_faker/moved_raw_products.jsonl'
example_dataset = Dataset("s3://pricecatcher/data/lookup_item/2024_01_24_1706081849415_0.parquet")
my_s3_connection ='{"conn_type": "s3", "login": "test", "password": "test", "host": "http://host.docker.internal:4566"}'

with DAG(dag_id='data_gov_lookup',
        default_args={'owner': 'airflow'},
        schedule='@monthly',
        start_date=pendulum.today('UTC').add(days=-1)
   ) as dag:

   trigger_airbyte_sync_premise = AirbyteTriggerSyncOperator(
       task_id='airbyte_trigger_sync_premise',
       airbyte_conn_id='airflow-airbyte-pricecatch',
       connection_id=AIRBYTE_CONNECTION_ID_PREMISE,
       asynchronous=True
   )
   
   trigger_airbyte_sync_item = AirbyteTriggerSyncOperator(
       task_id='airbyte_trigger_sync_item',
       airbyte_conn_id='airflow-airbyte-pricecatch',
       connection_id=AIRBYTE_CONNECTION_ID_ITEM,
       asynchronous=True
   )

   wait_item_sync_completion = AirbyteJobSensor(
       task_id='airbyte_check_item_sync',
       airbyte_conn_id='airflow-airbyte-pricecatch',
       airbyte_job_id=trigger_airbyte_sync_item.output
   )
   
   wait_premise_sync_completion = AirbyteJobSensor(
       task_id='airbyte_check_premise_sync',
       airbyte_conn_id='airflow-airbyte-pricecatch',
       airbyte_job_id=trigger_airbyte_sync_premise.output
   )
   
   sensor_one_key = S3KeySensor(
        task_id="sensor_one_key",
        bucket_name='pricecatcher',
        bucket_key=['data/lookup_item_2024_01.parquet',
                    'data/lookup_premise_2024_01.parquet'],
        aws_conn_id="s3-test",
        verify=None
    )

#    raw_products_file_sensor = FileSensor(
#        task_id='check_if_file_exists_task',
#        timeout=5,
#        filepath=RAW_PRODUCTS_FILE,
#        fs_conn_id='airflow-file-connector'
#    )

#    move_raw_products_file = BashOperator(
#        task_id='move_raw_products_file',
#        bash_command=f'mv {RAW_PRODUCTS_FILE} {COPY_OF_RAW_PRODUCTS}'
#    )

trigger_airbyte_sync_premise >> wait_premise_sync_completion >> sensor_one_key
trigger_airbyte_sync_item >> wait_item_sync_completion >> sensor_one_key
#    [trigger_airbyte_sync_premise , trigger_airbyte_sync_item] >> wait_for_sync_completion >> sensor_one_key
   
   ##>>  raw_products_file_sensor >> move_raw_products_file