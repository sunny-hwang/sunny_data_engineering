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
        dag_id = "create_summary_table",
        start_date = datetime(2021,2,7),
        schedule_interval = '@once',
        catchup = False
    )

execsql = PythonOperator(
        task_id = 'execsql',
        python_callable = execSQL,
        params = {
            'schema' : 'sunhee_bigdata',
            'table': 'sunny_summary',
            'sql' : 
'SELECT \
	DISTINCT A.userid,\
    FIRST_VALUE(A.channel) over(partition by A.userid order by B.ts rows between unbounded preceding and unbounded following) AS First_Channel,\
    LAST_VALUE(A.channel) over(partition by A.userid order by B.ts rows between unbounded preceding and unbounded following) AS Last_Channel \
FROM raw_data.user_session_channel A \
LEFT JOIN raw_data.session_timestamp B \
ON A.sessionid = B.sessionid;'
        },
        provide_context = True,
        dag = dag
        )

execsql
