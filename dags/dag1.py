#########################################################################
# The Airflow DAG is used to orchestrate the ARI PIPELINE for Data Drift
# DAG is Scheduled on 2nd of Every Month and triggers the glue job
# After glue job finished, the DAG compares new data with Snowlake Table
# The Data which are new will be lodaed into Snowflake Table.
#########################################################################
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
import boto3
import io
import logging
import pandas as pd
import snowflake.connector as sf
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from custom.common.snowflake_audit import snowflake_audit
from datetime import datetime
from datetime import timedelta
from custom.common.email_alert import email_alert

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 6, 1),
    'provide_context': True,
    'retries': 0
}

dag = DAG(
    'ari-monthly-dag',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 2 2 * *',
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
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
    env_var['schema'] = 'NATIONAL_RTM'

    if (env_value == 'qa'):
        env_var['database'] = 'QA_TZ'
    elif (env_value == 'prod'):
        env_var['database'] = 'PROD_TZ'
    else:
        env_var['database'] = 'DEV_TZ'
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
# Parameters Assignment
table_name = 'ari-api-config'
# Fuel Params
fuel_param_filename = "fuel_historical_data_" +\
    datetime.now().strftime('%Y%m%d%H')+".json"
fuel_extract_id = 'ari-fuel-monthly'
# Maintenance Params
maintenance_param_filename = "maintenance_historical_data_" + \
    datetime.now().strftime('%Y%m%d%H')+".json"
maintenance_extract_id = 'ari-maintenance-monthly'

# Get S3 Bucket Info
s3_bucket = boto3.client('s3', region_name='us-east-1')
config_bucket_name = f"datalake-jobs-code-{env}-invh"
# Get Config File Information
config_file = 'dna-datalake-airflow/dag/ari-config.txt'
config_obj = s3_bucket.get_object(
    Bucket=config_bucket_name,
    Key=config_file
)
config_df = pd.read_csv(io.BytesIO(config_obj['Body'].read()), sep="|")


# Compare Fuel data with monthly data


def compare_fuel_rz_to_s3():
    s = snowflake_audit()
    try:
        task_id = 'compare_fuel_data_task'
        sf_secret = get_snowflake_info(client)
        conn = sf.connect(
            user=sf_secret['snf_user'],
            password=sf_secret['snf_key'],
            account=sf_secret['snf_account'],
            warehouse=sf_secret['warehouse'],
            database=sf_secret['database'],
            schema=sf_secret['schema'],
            role='sysadmin'
        )
        s.start_s3_audit('ARI_FUEL', sf_secret['schema'],
                         dag.dag_id, task_id)
        # fetch columns from config file
        fuel_config_columns = config_df[config_df.API ==
                                        fuel_extract_id].values.tolist()
        fuel_compare_columns = fuel_config_columns[0][1].split(",")
        fuel_columns = fuel_config_columns[0][2].split(",")
        cursor = conn.cursor()
        cursor.execute(
            "select ARIVEHICLENUMBER,TRANSACTIONDATE, AMOUNT from " +
            sf_secret['database'] + "." + sf_secret['schema'] + ".ARI_FUEL",
            timeout=600)
        query_id = cursor.sfqid
        cursor.get_results_from_sfqid(query_id)
        results = cursor.fetchall()
        df_snf = pd.DataFrame(
            results, columns=fuel_compare_columns
        )
        filename = 'ari-data/fuel_historical_data/'+fuel_param_filename
        print(filename)
        s3 = boto3.client('s3', region_name='us-east-1')
        bucket = f"snowflake-datalake-{env}-invh"
        fileobj = s3.get_object(
            Bucket=bucket,
            Key=filename
        )
        filedata = fileobj['Body'].read()
        df = pd.read_json(filedata)
        if df.empty:
            print('empty file')
            s.s3_success_audit(0)
        else:
            df[fuel_compare_columns] = df[fuel_compare_columns].astype(str)
            engine = create_engine(URL(
                user=sf_secret['snf_user'],
                password=sf_secret['snf_key'],
                account=sf_secret['snf_account'],
                warehouse=sf_secret['warehouse'],
                database=sf_secret['database'],
                schema=sf_secret['schema'],
                role='sysadmin',
            ))
            # Compare Data with TZ Database
            df3 = pd.merge(
                df, df_snf, left_on=fuel_compare_columns,
                right_on=fuel_compare_columns,
                how='left', indicator='Exist')
            df3 = df3.loc[df3['Exist'] != 'both']
            df4 = df3.drop(['Exist'], axis=1)
            df4.columns = fuel_columns
            df4.to_sql('ARI_FUEL', con=engine, if_exists='append', index=False)
            s.s3_success_audit(len(df4))
    except Exception as err:
        print(err)
        s.end_failure_audit(err)
        raise (err)

# Compare Maintenance data with monthly data


def compare_maintenance_rz_to_s3():
    s = snowflake_audit()
    try:
        task_id = 'compare_maintenance_data_task'
        sf_secret = get_snowflake_info(client)
        conn = sf.connect(
            user=sf_secret['snf_user'],
            password=sf_secret['snf_key'],
            account=sf_secret['snf_account'],
            warehouse=sf_secret['warehouse'],
            database=sf_secret['database'],
            schema=sf_secret['schema'],
            role='sysadmin'
        )
        s.start_s3_audit('ARI_Maintenance', sf_secret['schema'],
                         dag.dag_id, task_id)
        # fetch columns from config file
        maintenance_config_columns = config_df[config_df.API ==
                                               maintenance_extract_id
                                               ].values.tolist()
        maintenance_compare_columns = maintenance_config_columns[0][1].split(
            ",")
        maintenance_columns = maintenance_config_columns[0][2].split(",")
        cursor = conn.cursor()
        cursor.execute(
            "select ARIVEHICLENUMBER,BILLPAIDDATE,POTOTALLINECOST from " +
            sf_secret['database'] + "." +
            sf_secret['schema'] + ".ARI_MAINTENANCE",
            timeout=600)
        query_id = cursor.sfqid
        cursor.get_results_from_sfqid(query_id)
        results = cursor.fetchall()
        df_snf = pd.DataFrame(
            results, columns=maintenance_compare_columns
        )
        filename = (
            'ari-data/maintenance_historical_data/' +
            maintenance_param_filename)
        print(filename)
        s3 = boto3.client('s3', region_name='us-east-1')
        bucket = f"snowflake-datalake-{env}-invh"
        fileobj = s3.get_object(
            Bucket=bucket,
            Key=filename
        )
        filedata = fileobj['Body'].read()
        new_columns = ['lesseeCode', 'ariVehicleNumber', 'clientVehicleNumber',
                       'vin', 'odometer', 'hourMeter', 'poNumber', 'vendorId',
                       'vendorName', 'vendorAddressLine1',
                       'vendorAddressLine2', 'vendorCity',
                       'vendorStateProvince', 'vendorZipPostalCode',
                       'customerPoNumber',
                       'invoiceNumber', 'invoiceDate', 'repairDate', 'poDate',
                       'billPaidDate', 'poDetailsId', 'poTotalLineCost',
                       'quantity', 'cost', 'type',
                       'complaint', 'cause', 'ataCode', 'ataDescription',
                       'vendorType',
                       'division', 'prefix', 'clientData1', 'clientData2',
                       'clientData3',
                       'clientData4', 'clientData5', 'clientData6',
                       'clientData7',
                       'driverClass', 'exec', 'auxData1', 'auxData2',
                       'auxData3',
                       'auxData4', 'auxData5', 'auxData6', 'auxData7',
                       'auxData8',
                       'auxData9', 'auxData10',
                       'auxData11', 'auxData12', 'auxData13', 'auxData14',
                       'auxDate1',
                       'auxDate2', 'auxDate3', 'auxDate4', 'firstName',
                       'lastName',
                       'parentVendor',
                       'record_id']
        df = pd.read_json(filedata)
        if df.empty:
            print('empty file')
            s.s3_success_audit(0)
        else:
            df = df.reindex(columns=new_columns)
            df[maintenance_compare_columns] = df[maintenance_compare_columns
                                                 ].astype(str)
            engine = create_engine(URL(
                user=sf_secret['snf_user'],
                password=sf_secret['snf_key'],
                account=sf_secret['snf_account'],
                warehouse=sf_secret['warehouse'],
                database=sf_secret['database'],
                schema=sf_secret['schema'],
                role='sysadmin',
            ))
            # Compare Data with TZ Database
            df3 = pd.merge(
                df, df_snf, left_on=maintenance_compare_columns,
                right_on=maintenance_compare_columns,
                how='left', indicator='Exist')
            df3 = df3.loc[df3['Exist'] != 'both']
            df4 = df3.drop(['Exist'], axis=1)
            df4.columns = maintenance_columns
            df4.to_sql('ARI_MAINTENANCE', con=engine,
                       if_exists='append', index=False)
            s.s3_success_audit(len(df4))
    except Exception as err:
        print(err)
        s.end_failure_audit(err)
        raise (err)


# Glue Job details
glue_job_name = f"datalake-ari-api-{env}-invh"

# Task Details
fuel_glue_job_task = AwsGlueJobOperator(
    task_id='fuel_glue_job_task',
    job_name=glue_job_name,
    script_args={
        "--param_filename": fuel_param_filename,
        "--TASK_ID": 'fuel_glue_job_task',
        "--DAG_ID": dag.dag_id,
        "--table_name": table_name,
        "--extract_id": fuel_extract_id
    },
    dag=dag, on_failure_callback=email_alert
)
maintenance_glue_job_task = AwsGlueJobOperator(
    task_id='maintenance_glue_job_task',
    job_name=glue_job_name,
    script_args={
        "--param_filename": maintenance_param_filename,
        "--TASK_ID": 'maintenance_glue_job_task',
        "--DAG_ID": dag.dag_id,
        "--table_name": table_name,
        "--extract_id": maintenance_extract_id
    },
    dag=dag, on_failure_callback=email_alert
)

compare_fuel_data_task = PythonOperator(
    task_id='compare_fuel_data_task',
    python_callable=compare_fuel_rz_to_s3,
    dag=dag, on_failure_callback=email_alert
)

compare_maintenance_data_task = PythonOperator(
    task_id='compare_maintenance_data_task',
    python_callable=compare_maintenance_rz_to_s3,
    dag=dag, on_failure_callback=email_alert
)
fuel_glue_job_task >> compare_fuel_data_task
compare_fuel_data_task >> maintenance_glue_job_task
maintenance_glue_job_task >> compare_maintenance_data_task
