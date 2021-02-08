# -*- coding: utf-8 -*-


##############
#DAG Setting
##############
from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime

dag = DAG(
        dag_id = "sunny_etl_dag",
        start_date = datetime(2021,1,31),
        schedule_interval = '@once'
    )




#############
#Python code
#############


import psycopg2

# Redshift connection 함수
def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "sunhee_bigdata"
    redshift_pass = "Sunhee_Bigdata!1"
    port = 5439
    dbname = "prod"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()

"""# ETL 함수를 하나씩 정의"""

import requests

# csv파일을 str로 저장

def extract(url):
    f = requests.get(url)
    return (f.text)

# str을 list로 변환함

def transform(**context):
    text = context['task_instance'].xcom_pull(task_ids='exec_extract')
    lines = text.split("\n")
    return lines

def load(**context):
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    # BEGIN;DELETE FROM ;INSERT INTO TABLE VALUES ('KEEYONG', 'MALE');END;

    lines = context['task_instance'].xcom_pull(task_ids='exec_transform')
    cur = get_Redshift_connection()

    
    cur.execute("BEGIN")
    cur.execute("DELETE FROM sunhee_bigdata.name_gender")

    try:
      # 헤더라인 무시
      for r in lines[1:]:
          if r != '':
              (name, gender) = r.split(",")
              #print(name, "-", gender)
              #sql = "INSERT INTO sunhee_bigdata.name_genderEEEE VALUES ('{n}', '{g}')".format(n=name, g=gender)
              sql = "INSERT INTO sunhee_bigdata.name_gender VALUES ('{n}', '{g}')".format(n=name, g=gender)
              print(sql)
              cur.execute(sql)
      cur.execute("END")

    except:
      cur.execute("ROLLBACK")

"""# 이제 Extract부터 함수를 하나씩 실행"""

#link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"

#data = extract(link)

#lines = transform(data)

#load(lines)



####################
# Dag Task Setting
####################

exec_extract = PythonOperator(
        task_id = 'exec_extract',
        python_callable = extract,
        op_kwargs={'url': 'https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv'},
        dag = dag
        )


exec_transform = PythonOperator(
        task_id = 'exec_transform',
        python_callable = transform,
        provide_context=True,
        dag = dag
        )



exec_load = PythonOperator(
        task_id = 'exec_load',
        python_callable = load,
        provide_context = True,
        dag = dag
        )

exec_extract >> exec_transform >> exec_load
