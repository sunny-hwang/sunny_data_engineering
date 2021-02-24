from airflow import DAG
from airflow.macros import *

import os
from glob import glob
import logging
import subprocess

from plugins import redshift_summary


DAG_ID = "Build_Summary_Tables"
dag = DAG(
    DAG_ID,
    schedule_interval="@once",
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    start_date=datetime(2021, 2, 19)
)

# this should be listed in dependency order (all in analytics)
tables_load = [
    'user_summary',
    'cohort_summary'
#    'user_session_summary'
]

dag_root_path = os.path.dirname(os.path.abspath(__file__))

# task 리턴됨
redshift_summary.build_summary_table(dag_root_path, dag, tables_load, "redshift_dev_db")
