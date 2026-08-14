from airflow import DAG                                       
"""DAG = Directed Acyclic Graph #In Airflow, a DAG defines the sequence of tasks in a data pipeline, their dependencies, and when they should run.Think of it as a workflow blueprint."""
import os                                                     #Python's built-in module for interacting with the operating system.
import fnmatch                                                #Match filenames using wildcard patterns.
from datetime import timedelta                                #Used to represent a period of time.#If a task fails it waits for time period for again retry
from airflow.providers.sftp.hooks.sftp import SFTPHook        #This is an Airflow hook for communicating with an SFTP server.
from airflow.providers.google.cloud.hooks.gcs import GCSHook  #This is an Airflow hook for communicating with an Google Cloud Storage.
from airflow.operators.python import PythonOperator           #PythonOperator runs a Python function.
import datetime                                               #use to represent datetime
from pendulum.datetime import DateTime                               
from pendulum.tz.timezone import Timezone                     #use to represent timezone
from airflow.operators.bash import BashOperator               #BashOperator runs a Bash/Linux shell command.
from airflow.utils.trigger_rule import TriggerRule
"""Trigger Rule determines when a task is allowed to run based on the status of its upstream (parent) tasks."""

dag_kwargs = {'dag_id': 'INBOUND_SFTP', 'schedule': None, 'params': {'COD_FILE': 'IN_SFTP'}, 'default_args': None, 'catchup': False, 'start_date': DateTime (2000, 1, 1, 0, 0, 0, tzinfo-Timezone('Europe/Rome)), 'template_searchpath': ['/home/airflow/gcs'], 'tags':["SFTP", "INBOUND_SFTP"]}

dag DAG(**dag_kwargs)

def read_from_bq_to_xcom(**kwargs):
    
"""Python callable to be used with the PythonOperator Airflow operator.
Executes a Postgres query on CloudSQL according to connection, impersonation and configuration
settings and stores retrieved results into Airflow's XComs.
Results can be stored in XComs in different formats available from this code.
* IMPORTANT *
Parameters must be passed configuring the relevant keys in the op_kwargs property of the PythonOperator Airflow operator.
:param kwargs: dictionary of properties that drive the code:
pg_query: MANDATORY The Google CloudSQL-Postgres query that produces the results to store into Airflow's XComs.
xcom_format: MANDATORY The format to use to store results in XCom (case insensitive) MUST be one of the following:
JSON_DATA: stores a single XCom entry in JSON format with an array of objects, one object per result record. In each object, keys are field names and values are field values.
UNPACKED_JSON_DATA: produces an XCom entry for each result record, naming the entry with the concatenation of a prefix and the value of the field set as 'xcom_key_field' (see below), storing each record in JSON format, which keys are field names and values are field values.
JSON_DATA_SCHEMA: stores in JSON format an object with a 'data' component as in JSON_DATA and a 'schema' component.
VALUES: raw records as an array of arrays, each being the ordered list of field values.
xcom_var_name: Optional The name of the XCom variable to store results in, defaults to 'return_value'.
If 'xcom_format' is UNPACKED_JSON_DATA, this is the prefix of the keys of XCom entries generated, that result from the concatenation of this string (including any trailing underscores) and the values of the field named in 'xcom_key_field' returned by the query.
xcom_key_field: Optional relevant only if 'xcom_format' is UNPACKED_JSON_DATA, this field returned by the query distinguishes the different results to be stored in XCom keys (i.e. is a key of produced results).
gcp_conn_id: Optional Airflow connection used for GCP credentials (connection to the GCP platform, defaults to 'google_cloud_default').
:return: --"""

assert 'bq_query' in kwargs, \
    "DAG: {d} | task: {t) | run: {r) Cannot execute because 'bq_query' was not provided in parameter dictionary".format( d=kwargs['ti'].dag_id, t=kwargs['ti'].task_id, r=kwargs['ti'].run_id )
    
assert 'xcom_format' in kwargs, \
    "DAG: (d) | task: {t} | run: {r} Cannot execute because 'xcom_format' was not provided in parameter dictionary".format( d=kwargs['ti'].dag_id, t=kwargs['ti'].task_id, r=kwargs['ti'].run_id )
    
valid_xcom_format_mappings = dict({
"json_data": "records",
"unpacked_json_data": "records", 
"json_data_schema": "table", 
"values": "values"
})

xcom_format = kwargs['xcom_format'].lower()
assert xcom_format in valid_xcom_format_mappings.keys(),\
    "DAG: {d} | task: {t} | run: {r} Cannot execute because 'xcom_format' is '{x}'; it must be in: {l}".format(    
        d=kwargs['ti'].dag_id,
        t=kwargs['ti'].task_id,
        r=kwargs['ti'].run_id,
        x=xcom_format,
        l="'" + "','".join(valid_xcom_format_mappings.keys()) + "'" 
    )
if xcom_format.startswith("unpacked_"):
    assert 'xcom_key_field' in kwargs, \
        "DAG: {d} | task: {t} | run: {r} - Cannot execute because 'xcom_format' is {f}, but 'xcom_key_field' was not provided in parameter dictionary".format(
            d=kwargs['ti'].dag_id, t=kwargs['ti'].task_id, r=kwargs['ti'].run_id, f=xcom_format
    ) 

xcom_var_name = kwargs.get('xcom_var_name', 'return_value')

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook  #Hook to BigQuery through the connection to the GCP platform

bqh = BigQueryHook(
    gcp_conn_id=kwargs.get('gcp_conn_id', GoogleBaseHook.default_conn_name),
    use_legacy_sql=False,
    location=kwargs.get('bq_query_location', None),
    api_resource_configs=kwargs.get('bq_job_api_config_dict', None),
    impersonation_chain=kwargs.get('impersonation_chain', None)
)

# Extract the results produced by the query from BigQuery into a Pandas DataFrame
df_recs = bqh.get_pandas_df(sql=kwargs['bq_query'], dialect='standard', progress_bar_type=None)

# Store into XCom data in the required format
EXTRACT_CONFIG=df_recs.to_json(orient=valid_xcom_format_mappings [xcom_format])
#print(EXTRACT_CONFIG)

for config in eval(EXTRACT_CONFIG):
    dictionary={config[0]:config[1].replace('\\/','/')}
    for key_dict, value_dict in dictionary.items():
        kwargs["ti"].xcom_push(key=key_dict,value=value_dict)
        if key_dict=="VCLOUD_OUTPUT_PATH":
            bucket_name, folder_path=value_dict.replace("gs://", "").split("/", 1)
            kwargs["ti"].xcom_push(key="OUTBOUND_BUCKET", valüe=bucket_name)
            kwargs["ti"].xcom_push(key="OUTBOUND_FOLDER", value=folder_path)
        else:
            continue

read_from_bq_to_xcom_ref = read_from_bq_to_xcom

tsk_1_load_config_entries_kwargs = \
    {'op_args': None,
     'op_kwargs': {'bq_query': 'SELECT KEY, VALUE FROM DP_CONFIG.V_DP_FLOW_PARAMS where' "COD_FILE='{{ params.COD_FILE }}'",
                    'xcom_format': 'VALUES'},
     'show_return_value_in_logs': True,
     'templates_dict': None,
     'templates_exts': None,
     'task_id': 'T1.Load_Config_entries'}

tsk_1_load_config_entries_kwargs['python_callable'] = read_from_bq_to_xcom_ref

tsk_1_load_config_entries = PythonOperator (**tsk_1_load_config_entries_kwargs, dag=dag)

tsk_101_event_flow_start_kwargs = \
     {'env': {'vPython_Script_ED": "{{task_instance.xcom_pull(key='vPython_Script_ED', task_ids= 'T1.Load_Config_entries')}}",
              'ed_flag': "{{task_instance.xcom_pull(key='VED_FLAG_START', task_ids= 'T1.Load_Config_entries')}}",
              'data': "{{task_instance.xcom_pull (key='VED_MSG_START', task_ids=_T1. Load_Config_entries')}}",
              'attributes': "{{task_instance.xcom_pull(key='VED_START_ATTRIBUTES', task_ids= 'T1.Load_Config_entries')}}",
              'project_id': "{{task_instance.xcom_pull(key='VED_PRJ', task_ids= 'T1. Load_Config_entries')}}",
              'topic': "{{task_instance.xcom_pull (key='VED_TOPIC', task_ids= 'T1. Load_Config_entries')}}",
           'dag_start_time': "{{dag_run.get_task_instance(task_id='T1.Load_Config_entries').start_date}}",
           'dag_id": "{{ ti.dag_id }}",
           'run_id": "{{ run_id }}",
           'cod_file': '{{ params.COD_FILE }}'},
       'append_env': False,
       'output_encoding': 'utf-8',
       'cwd': None,
       'skip_exit_code': 99,
       'task_id': 'T101.Event_Flow_Start'}

tsk_101_event_flow_start_kwargs['bash_command"] = """python /home/airflow/gcs/dags/script/$vPython_Script_ED --ed_flag $ed_flag --project_id $project_id topic $topic --dag_id $dag_id --dag_start_time "$dag_start_time" --data "$data" --attributes "${attributes)" --run_id $run_id --cod_file $cod_file"""

tsk_101_event_flow_start = BashOperator (**tsk_101_event_flow_start_kwargs, dag=dag)

tsk_2_filename_kwargs = \
     {'env': {'VFILE_NAME": "{{task_instance.xcom_pull(key='vFILE_NAME',task_ids= 'T1.Load_Config_entries')}}"},
      'append_env': True,
      'output_encoding': 'utf-8',
      'cwd': None,
      'skip_exit_code': 99,
      'task_id': 'T2.Create_File_Name'}

tsk_2_filename_kwargs['bash_command'] = """bq query -q --use_legacy_sql=false --format csv "SELECT $VFILE_NAME" | sed '/^$/d' | sed '1d' """

tsk_2_filename = BashOperator(**tsk_2_filename_kwargs, dag=dag)

def download_multiple_sftp_and_upload_to_gcs(**kwargs):
    sftp_conn_id = kwargs['vSFTP_CONN_ID']
    gcs_conn_id = kwargs['vGCS_CONN_ID']
    remote_dir = kwargs['vREMOTE_PATH']
    file_pattern = kwargs['INPUT_FILE_NAME']
    local_dir = kwargs['vTMP_PATH']
    bucket_name = kwargs['OUTBOUND_BUCKET']
    gcs_prefix = kwargs['OUTBOUND_FOLDER']
    sftp_hook = SFTPHook(ssh_conn_id=sftp_conn_id)
    
    with sftp_hook.get_conn() as sftp_client:
        remote files = sftp client.listdir(remote_dir)
        matching_files fnmatch.filter(remote_files, file_pattern)
        print("matching_files:", matching_files)
        
        if not matching_files:
            print(f"No file with pattern (file_pattern) in (remote_dir). exit.")
            return # <--- return 0
            
        gcs_hook = GCSHook(ecp.conn_id=gcs_conn_id)
        os.makedirs(local_dir, exist_ok=True)
        
        for filename in matching_files:
            remote_path = f"{remote_dir}/{filename}"
            local_path = os.path.join(local_dir, filename)
            
            sftp_client.get(remote_path, local_path)
            gcs_object = f"(gcs_prefix}/{filename}" if gcs_prefix else filename
            
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=gcs_object,
                filename=local_path
            )
            
            print(f"Uploaded: gs://{bucket_name}/{gcs_object}")
            os.remove(local_path)

download_multiple_sftp_and_upload_to_gcs_ref=download_multiple_sftp_and_upload_to_gcs

tsk_3_sftp_to_gcs_kwargs = \
        {'op_args': None,
         'op_kwargs': {'vSFTP_CONN_ID': "{{task_instance.xcom_pull (key='vSFTP_CONN_ID', task_ids= 'T1.Load_Config_entries')}}",
                    'vGCS CONN_ID': "{{task_instance.xcom_pull(key='vGCS_CONN_ID', task_ids= 'T1.Load_Config_entries')}}",
                    'vREMOTE PATH': "{{task_instance.xcom_pull(key='vREMOTE_PATH', task_ids= 'T1.Load_Config_entries')}}",
                    'INPUT_FILE_NAME': "{{task_instance.xcom_pull(key='return_value', task_ids= 'T2.Create_File_Name')}}",
                    'vTMP PATH': "{{task_instance.xcom_pull(key='vTMP_PATH', task_ids= 'T1. Load_Config_entries')}}",
                    'OUTBOUND_BUCKET': "{{task_instance.xcom_pull(key= 'OUTBOUND_BUCKET', task_ids= 'T1.Load_Config_entries')}}",
                    'OUTBOUND_FOLDER': "{{task_instance.xcom_pull(key='OUTBOUND_FOLDER', task_ids='T1.Load_Config_entries')}}",},
         
         'show_return_value_in_logs': True,
         'templates_dict': None,
         ' templates_exts ': None,
         'task_id': 'T3.SFTP_TO_GCS'}

tsk_3_sftp_to_gcs_kwargs['python_callable'] = download_multiple_sftp_and_upload_to_gcs_ref

tsk_3_sftp_to_gcs = PythonOperator (**tsk_3_sftp_to_gcs_kwargs, dag=dag)

tsk_301_event_flow_end_kwargs = \
         {'env': {'vPython_Script_ED': "{{task_instance.xcom_pull(key='vPython_Script_ED', task_ids= 'T1. Load_Config_entries')}}",
                  'ed_flag': "{{task_instance.xcom_pull(key='VED_FLAG_END',task_ids= 'T1 . Load_Config_entries')}}",
                  'data': "{{task_instance.xcom_pull(key='VED_MSG_END', task_ids= 'T1. Load_Config_entries')}}",  
                  'attributes': "{{task_instance.xcom_pull(key='vED_END_ATTRIBUTES', task_ids= 'T1.Load_Config_entries')}}",
                  'project_id': "{{task_instance.xcom_pull(key='vED_PRJ', task_ids= 'T1.Load_Config_entries')}}",
                  'topic': "{{task_instance.xcom_pull(key='VED_TOPIC', task_ids= 'T1. Load_Config_entries')}}",
                  'dag_start_time': "{{dag_run.get_task_instance(task_id='T1.Load_Config_entries').start_date}}",
                  'dag_id': '{{ ti.dag_id}}',
                  'run_id': '{{ run_id }}',
                  'cod_file': '{{ params.COD_FILE }}'},
          'append_env': False,
          'output_encoding': 'utf-8',
          'cwd': None,
          'skip_exit_code': 99,
          'task_id': 'T301.Event_Flow_End'}

tsk_301_event_flow_end_kwargs['bash_command'] = """python /home/airflow/gcs/dags/script/$vPython_Script_ED --ed_flag $ed_flag --project_id $project_id --topic Stopic dag_id $dag_id --dag_start_time "$dag_start_time" --data "$data" --attributes "${attributes)" --run_id $run_id --cod_file $cod_file"""

tsk_301_event_flow_end = BashOperator (**tsk_301_event_flow_end_kwargs, dag=dag)

tsk_101_event_flow_start.set_upstream(tsk_1_load_config_entries)
tsk_2_filename.set_upstream(tsk_101_event_flow_start)
tsk_3_sftp_to_gcs.set_upstream (tsk_2_filename)
tsk_301_event_flow_end.set_upstream(tsk_3_sftp_to_gcs)