# Imports related to airflow
import airflow 
import datetime
from airflow import DAG
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator	
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators import gcs_to_gcs
from airflow.operators import dummy_operator
from airflow.operators.bash_operator import BashOperator

# General python imports
import gzip

# Imports related to GCS library
from google.cloud import logging, storage 
# import pendulum

dag_variables = Variable.get("spikeywinery_dtl", deserialize_json=True)
#common = dag_variables["spikeywinery_dtl"] Airflow varable available in JSON format 

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

# setting the default argumnets , these arguments can be inherit to each task. Also , these can be overwridden at task level

default_args = {
    'owner': dag_variables['owner'],                                      #The owner of the task 
    'depends_on_past': False,
    'email': [''],                                                        #The email address in 'to email list'
    'email_on_failure': dag_variables['email_on_failure'],
    'email_on_retry': dag_variables['email_on_retry'],                    #The email address to sent , when task is retried
    'retries': 1,                                                         #the number of retries that should be performed before failing the task
    'retry_delay': datetime.timedelta(minutes=5),                         #delay between retries
    'start_date': YESTERDAY,
    'project': dag_variables['project'],
    'region': dag_variables['region']
}

dag = DAG('composer7_sample_dag',
          catchup=False,
          schedule_interval=datetime.timedelta(days=1),
          default_args=default_args,
          description='dataflow DAG')

start_task = BashOperator(
             task_id='start_task',
             bash_command='echo "{0}" '.format(default_args),
             dag=dag,)

#Instantiate a DAG
validate_input_file_job = DataflowTemplateOperator(
          task_id='validate_input_file_job',
          template=dag_variables['template_location'],   
          dag=dag,
          dataflow_default_options={
                            'project': dag_variables['project'],
                            'region': dag_variables['region'],
                            'zone': dag_variables['zone'],
                            'tempLocation': dag_variables['temp_location']
           },
          job_name='test_dataload_via_composer' + '-' + datetime.datetime.now().strftime("%Y%m%d%H"))

DataflowTemplateOperator(
# task_id='validate_input_file_job_'+ file_pattern,
# template=dag_variables['template_location'],
# dag=dag_subdag1,
# parameters={
# 'input': dag_variables['common_input'] + file_pattern + '_'+ datetime.datetime.now().strftime("%d%m%Y") + dag_variables['file_expension'],
# 'output': dag_variables['common_output'] + file_pattern +'_' +datetime.datetime.now().strftime("%d%m%Y") + dag_variables['file_expension']
# 'schema_columns': dag_variables['column_name']+file_pattern
# },
# dataflow_default_options={qq
# 'project': dag_variables['project'],
# 'region': dag_variables['region'],
# 'zone': dag_variables['zone'],
# },		  
		  
		  
copy_file_to_output_bucket_job = gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator(
    task_id='copy_file_to_output_bucket_job',
    source_bucket=dag_variables['source_bucket'],
    source_object=dag_variables['source_object'],
    destination_bucket=dag_variables['destination_bucket'],
    destination_object=dag_variables['destination_object'],
    dag=dag,
    )

load_input_file_to_big_query = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
	task_id='load_input_file_to_big_query',
	bucket=dag_variables['bucket'],
	source_objects=[dag_variables['source_objects']],
	destination_project_dataset_table=dag_variables['destination_project_dataset_table'],
	skip_leading_rows=2,
	create_disposition='CREATE_IF_NEEDED',
	field_delimiter='^',
	#schema_object='gs://bts_test_bucket_for_composer/input/table_schema.json',
	schema_fields=[
		{'name': 'BILLING_ACCOUNT_KEY', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CONSOLIDATED_ACCOUNT_KEY', 'type': 'STRING', 'mode': 'NULLABLE'},	
		{'name': 'COMPANY_KEY', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'MARKETING_COMPANY_KEY', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'BILLING_ACCOUNT_STATUS', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'BILLING_ACCOUNT_NAME', 'type': 'STRING', 'mode': 'NULLABLE'},	
		{'name': 'BILLING_SYSTEM', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'BILL_MEDIA', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'BILL_FREQUENCY', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'BILL_PAYMENT_METHOD', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'LATEST_BILL_DATE', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'NEXT_BILL_DATE', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CREATE_DT', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'UPDATE_DT', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'HASH_VALUE', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'MASK_BILLING_ACCOUNT_NUMBER', 'type': 'STRING', 'mode': 'NULLABLE'},
	],
	write_disposition='WRITE_TRUNCATE',
	dag=dag,
	)
	
#state^gender^year^name^number^doj	
start_task >> validate_input_file_job >> copy_file_to_output_bucket_job >> load_input_file_to_big_query

