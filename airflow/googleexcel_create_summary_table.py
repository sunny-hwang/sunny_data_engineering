from airflow import DAG
from airflow.operators import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from datetime import timedelta
import requests
import logging
import psycopg2

from airflow.exceptions import AirflowException



######################################
# Google Excel 연동
######################################
import gspread
from oauth2client.service_account import ServiceAccountCredentials
scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive',
        ]
json_file_name = '/var/lib/airflow/sunny_google_drive.json'
credentials = ServiceAccountCredentials.from_json_keyfile_name(json_file_name, scope)
gc = gspread.authorize(credentials)
spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1MADratRm6cqG4cJxjnMvNQNBAXEDG36U6BPAlgXmDXA/edit?usp=sharing'
# 스프레스시트 문서 가져오기 
doc = gc.open_by_url(spreadsheet_url)
# 시트 선택하기
worksheet = doc.worksheet('시트1')
row_data = worksheet.row_values(2)


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id = 'redshift_dev_db')
    return hook.get_conn().cursor()


def execSQL(**context):

    #인수받기
    schema = context['params']['schema'] 
    table = context['params']['table']
    select_sql = context['params']['sql']

    logging.info(schema)
    logging.info(table)
    logging.info(select_sql)

    # Redshif 연결
    cur = get_Redshift_connection()


    #쿼리수행
    try:
        sql = "BEGIN;"
        #sql += """DROP TABLE IF EXISTS {schema}.{table}; CREATE TABLE {schema__}.{table} AS """.format(schema=schema, table=table)
        sql += """DROP TABLE IF EXISTS {schema}.{table}; CREATE TABLE {schema}.{table} AS """.format(schema=schema, table=table)
        sql += select_sql
        sql += "END;"
        logging.info(sql)
        cur.execute(sql)
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise AirflowException("")


#############################
# DAG Setting
#############################

dag = DAG(
        dag_id = "create_summary_table_with_google_excel",
        start_date = datetime(2021,2,7),
        schedule_interval = '@once',
        catchup = False
    )

execsql = PythonOperator(
        task_id = 'execsql',
        python_callable = execSQL,
        params = {
            #구글 엑셀에 있는 값으로 세팅하기
            'schema' : row_data[1],
            'table': row_data[2],
            'sql' : row_data[3]
        },
        provide_context = True,
        dag = dag
        )

execsql
