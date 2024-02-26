# this dag is used to orchastrate the Ring Central Glue Job and Copy Command
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import airflow.utils.dates
from custom.common.snowflake_audit import snowflake_audit
from custom.common.snowflake_utility import SnowflakeCursor
import boto3
import logging
from datetime import timedelta
from custom.common.email_alert import email_alert
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator

# DAG default arguments
default_args = {
    'owner': 'data-and-analytics',
    'start_date': airflow.utils.dates.days_ago(1),
    'provide_context': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=30)
}

# Initializing DAG
dag = DAG(
    'ringcentral-dag',
    default_args=default_args,
    catchup=False,
    schedule_interval='@daily',
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=300),
    on_failure_callback=email_alert
)


# get the snowflake authentication information
def get_snowflake_info(ssm_client):
    logging.info('Getting env info')
    env_param = ssm_client.get_parameter(Name='env', WithDecryption=True)
    env_value = env_param.get('Parameter').get('Value')

    snf_acc_param = ssm_client.get_parameter(
        Name='/users/snowflake/account', WithDecryption=True)
    snf_acc_value = snf_acc_param.get('Parameter').get('Value')

    snf_user_param = ssm_client.get_parameter(
        Name='/users/snowflake/account/user', WithDecryption=True)
    snf_user_value = snf_user_param.get('Parameter').get('Value')

    snf_key_param = ssm_client.get_parameter(
        Name='/users/snowflake/account/password', WithDecryption=True)
    snf_key_value = snf_key_param.get('Parameter').get('Value')
    env_var = {'env': env_value, 'snf_account': snf_acc_value,
               'snf_user': snf_user_value, 'snf_key': snf_key_value}

    env_var['warehouse'] = 'AWS_WH'
    env_var['schema'] = 'RINGCENTRAL'

    if (env_value == 'qa'):
        env_var['database'] = 'QA_RZ'
        env_var['stage'] = 'STG_RINGCENTRAL'
    elif (env_value == 'prod'):
        env_var['database'] = 'PROD_RZ'
        env_var['stage'] = 'STG_RINGCENTRAL'
    else:
        env_var['database'] = 'DEV_RZ'
        env_var['stage'] = 'STG_RINGCENTRAL'
    return env_var


session = boto3.session.Session()
client = session.client('ssm', region_name='us-east-1')
sf_secret = get_snowflake_info(client)
params = {'snowflake_user': sf_secret['snf_user'],
          'password': sf_secret['snf_key'],
          'account': sf_secret['snf_account'],
          'warehouse': sf_secret['warehouse'],
          'database': sf_secret['database'],
          'schema': sf_secret['schema'],
          'snowflake_role': 'sysadmin'
          }

env = sf_secret['env']


# Copy data from s3 to Ring Central tables
def copy_to_ringcentral_call_task():

    s = snowflake_audit()
    try:
        task_id = 'copy_to_ringcentral_call_task'
        snowflake_cur = SnowflakeCursor(params)
        s.start_audit(snowflake_cur, 'CALLS', sf_secret['schema'],
                      dag.dag_id, task_id)

        DELETE_CALLS_QUERY = ("DELETE FROM " + sf_secret['database'] +
                              "." + sf_secret['schema'] + ".CALLS_DUMP;"
                              )

        COPY_CALLS_QUERY = (
            "copy into "+sf_secret['database']+"." + sf_secret['schema'] +
            ".CALLS_DUMP from (SELECT *, CURRENT_TIMESTAMP() FROM @" +
            sf_secret['stage'] + "/call/ ) FILE_FORMAT = " +
            "( FORMAT_NAME ="+sf_secret['database']+"." + sf_secret['schema'] +
            ".RINGCENTRAL_FILEFORMAT);")

        MERGE_CALLS_QUERY = (
            "merge into "+sf_secret['database']+"." + sf_secret['schema'] +
            ".CALLS t using(select distinct RECORD:id::VARCHAR,RECORD "
            "from "+sf_secret['database']+"." + sf_secret['schema'] +
            ".CALLS_DUMP) s on t.id = s.RECORD:id::varchar "
            "when not matched then insert (ID,RECORD) "
            "values(RECORD:id::varchar,RECORD);"
        )

        snowflake_cur.execute(DELETE_CALLS_QUERY)
        snowflake_cur.execute(COPY_CALLS_QUERY)
        snowflake_cur.execute(MERGE_CALLS_QUERY)
        res = snowflake_cur.fetchall()
        logging.info(res)
        s.end_success_audit(res)
        snowflake_cur.close()
    except Exception as err:
        print(err)
        s.end_failure_audit(err)
        raise (err)


def copy_to_ringcentral_extansion_task():
    s = snowflake_audit()
    try:
        task_id = 'copy_to_ringcentral_extansion_task'
        snowflake_cur = SnowflakeCursor(params)
        s.start_audit(snowflake_cur, 'EXTENSION', sf_secret['schema'],
                      dag.dag_id, task_id)

        DELETE_EXTENSIONS_QUERY = ("DELETE FROM " + sf_secret['database'] +
                                   "." + sf_secret['schema'] +
                                   ".EXTENSIONS_DUMP;"
                                   )
        COPY_EXTENSIONS_QUERY = (
            "copy into "+sf_secret['database']+"." + sf_secret['schema'] +
            ".EXTENSIONS_DUMP from (SELECT *, CURRENT_TIMESTAMP() FROM @" +
            sf_secret['stage'] + "/extension/ ) FILE_FORMAT = " +
            "( FORMAT_NAME ="+sf_secret['database']+"." + sf_secret['schema'] +
            ".RINGCENTRAL_FILEFORMAT);")

        MERGE_EXTENSIONS_QUERY = (
            "merge into "+sf_secret['database']+"." + sf_secret['schema'] +
            ".EXTENSIONS t using(select distinct RECORD:id::VARCHAR,RECORD "
            "from "+sf_secret['database']+"." + sf_secret['schema'] +
            ".EXTENSIONS_DUMP) s on t.id = s.RECORD:id::varchar "
            "when not matched then insert (ID,RECORD) "
            "values(RECORD:id::varchar,RECORD) "
            "when matched then update set t.RECORD = s.RECORD;"
        )

        snowflake_cur.execute(DELETE_EXTENSIONS_QUERY)
        snowflake_cur.execute(COPY_EXTENSIONS_QUERY)
        snowflake_cur.execute(MERGE_EXTENSIONS_QUERY)
        res = snowflake_cur.fetchall()
        logging.info(res)
        s.end_success_audit(res)
        snowflake_cur.close()
    except Exception as err:
        print(err)
        s.end_failure_audit(err)
        raise (err)


def copy_to_ringcentral_IVR_task():
    s = snowflake_audit()
    try:
        task_id = 'copy_to_ringcentral_IVR_task'
        snowflake_cur = SnowflakeCursor(params)
        s.start_audit(snowflake_cur, 'IVR_CALLS', sf_secret['schema'],
                      dag.dag_id, task_id)

        DELETE_CALLS_QUERY = ("DELETE FROM " + sf_secret['database'] +
                              "." + sf_secret['schema'] + ".IVR_CALLS_DUMP;"
                              )

        COPY_CALLS_QUERY = (
            "copy into "+sf_secret['database']+"." + sf_secret['schema'] +
            ".IVR_CALLS_DUMP from (SELECT *, CURRENT_TIMESTAMP() FROM @" +
            sf_secret['stage'] + "/ivr_call/ ) FILE_FORMAT = " +
            "( FORMAT_NAME ="+sf_secret['database']+"." + sf_secret['schema'] +
            ".RINGCENTRAL_FILEFORMAT);")

        MERGE_CALLS_QUERY = (
            "merge into "+sf_secret['database']+"." + sf_secret['schema'] +
            ".IVR_CALLS t using(select distinct RECORD:id::VARCHAR,RECORD "
            "from "+sf_secret['database']+"." + sf_secret['schema'] +
            ".IVR_CALLS_DUMP) s on t.id = s.RECORD:id::varchar "
            "when not matched then insert (ID,RECORD) "
            "values(RECORD:id::varchar,RECORD);"
        )

        snowflake_cur.execute(DELETE_CALLS_QUERY)
        snowflake_cur.execute(COPY_CALLS_QUERY)
        snowflake_cur.execute(MERGE_CALLS_QUERY)
        res = snowflake_cur.fetchall()
        logging.info(res)
        s.end_success_audit(res)
        snowflake_cur.close()
    except Exception as err:
        print(err)
        s.end_failure_audit(err)
        raise (err)


def copy_to_ringcentral_IVR_extansion_task():
    s = snowflake_audit()
    try:
        task_id = 'copy_to_ringcentral_IVR_extansion_task'
        snowflake_cur = SnowflakeCursor(params)
        s.start_audit(snowflake_cur, 'IVR_EXTENSIONS', sf_secret['schema'],
                      dag.dag_id, task_id)

        DELETE_EXTENSIONS_QUERY = ("DELETE FROM " + sf_secret['database'] +
                                   "." + sf_secret['schema'] +
                                   ".IVR_EXTENSIONS_DUMP;"
                                   )
        COPY_EXTENSIONS_QUERY = (
            "copy into "+sf_secret['database']+"." + sf_secret['schema'] +
            ".IVR_EXTENSIONS_DUMP from (SELECT *, CURRENT_TIMESTAMP() FROM @" +
            sf_secret['stage'] + "/ivr_extension/ ) FILE_FORMAT = " +
            "( FORMAT_NAME ="+sf_secret['database']+"." + sf_secret['schema'] +
            ".RINGCENTRAL_FILEFORMAT);")

        MERGE_EXTENSIONS_QUERY = (
            "merge into "+sf_secret['database']+"." + sf_secret['schema'] +
            ".IVR_EXTENSIONS t using(select distinct RECORD:id::VARCHAR,RECORD "
            "from "+sf_secret['database']+"." + sf_secret['schema'] +
            ".IVR_EXTENSIONS_DUMP) s on t.id = s.RECORD:id::varchar "
            "when not matched then insert (ID,RECORD) "
            "values(RECORD:id::varchar,RECORD) "
            "when matched then update set t.RECORD = s.RECORD;"
        )

        snowflake_cur.execute(DELETE_EXTENSIONS_QUERY)
        snowflake_cur.execute(COPY_EXTENSIONS_QUERY)
        snowflake_cur.execute(MERGE_EXTENSIONS_QUERY)
        res = snowflake_cur.fetchall()
        logging.info(res)
        s.end_success_audit(res)
        snowflake_cur.close()
    except Exception as err:
        print(err)
        s.end_failure_audit(err)
        raise (err)


# Glue Job details
glue_job_name = "datalake-ringcentral-"+env+"-invh"

ringcentral_glue_job_task = AwsGlueJobOperator(
    task_id='ringcentral_glue_job_task',
    job_name=glue_job_name,
    script_args={
        "--TASK_ID": 'copy_to_ringcentral_table_task',
        "--DAG_ID": dag.dag_id
    },
    dag=dag, on_failure_callback=email_alert
)

copy_to_ringcentral_call_task = PythonOperator(
    task_id='copy_to_ringcentral_call_task',
    python_callable=copy_to_ringcentral_call_task,
    dag=dag, on_failure_callback=email_alert
)

copy_to_ringcentral_extansion_task = PythonOperator(
    task_id='copy_to_ringcentral_extansion_task',
    python_callable=copy_to_ringcentral_extansion_task,
    dag=dag, on_failure_callback=email_alert
)

copy_to_ringcentral_IVR_task = PythonOperator(
    task_id='copy_to_ringcentral_IVR_task',
    python_callable=copy_to_ringcentral_IVR_task,
    dag=dag, on_failure_callback=email_alert
)

copy_to_ringcentral_IVR_extansion_task = PythonOperator(
    task_id='copy_to_ringcentral_IVR_extansion_task',
    python_callable=copy_to_ringcentral_IVR_extansion_task,
    dag=dag, on_failure_callback=email_alert
)

ringcentral_glue_job_task >> copy_to_ringcentral_call_task
copy_to_ringcentral_call_task >> copy_to_ringcentral_extansion_task
ringcentral_glue_job_task >> copy_to_ringcentral_IVR_task
copy_to_ringcentral_IVR_task >> copy_to_ringcentral_IVR_extansion_task