##############
#DAG Setting
##############
from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime

dag = DAG(
        dag_id = "sunnytest",
        start_date = datetime(2021,2,18),
        schedule_interval = '@once'
    )




#############
#Python code
#############


import requests
import logging

# csv파일을 str로 저장
def extract(**context):
    url = context["params"]["url"]
    logging.info(url)
    f = requests.get(url)
    return (f.text)



####################
# Dag Task Setting
####################

exec_extract = PythonOperator(
        task_id = 'exec_extract',
        python_callable = extract,       
        params={'url': 'https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv'},
        provide_context=True,
        dag = dag
        )
        
        
