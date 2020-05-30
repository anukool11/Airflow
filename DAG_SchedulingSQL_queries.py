# -*- coding: utf-8 -*-
"""
Created on Tue May 19 17:03:20 2020

@author: Dell
"""

from datetime import timedelta
  
import airflow
import os
import csv
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.email import send_email
from airflow.operators import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['anukoolraj12@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
	'retries': 1,
    'retry_delay': timedelta(minutes=5),
	'schedule_interval': '@daily',
}

dag  = DAG('redshifttest2', default_args = default_args, 
           template_searchpath = ['/home/ec2-user/airflow/sqlscripts'])

inserttask = PostgresOperator(
        task_id = 'inserttask',
        postgres_conn_id = 'redshift',
        sql = 'query2.sql',
        dag = dag)

email = EmailOperator(
        task_id='send_email',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        to=['anukoolraj12@gmail.com'],
        subject='Airflow Alert',
        html_content= 'previous task was successful',
        dag=dag
)  



inserttask >> email

