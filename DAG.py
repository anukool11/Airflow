

# -*- coding: utf-8 -*-
"""
Created on Mon May  4 16:59:38 2020

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
 

def getSum():
    num = 10
    sum = 0
    while(num>0):
        sum = sum + num
        num = num - 1 
    return sum

def getProduct():
    num = 10
    product = 1
    while(num>0):
        product = product * num
        num = num - 1 
    return product

def getProductSumRatio():
    product = getProduct()
    sum = getSum()
    ratio = product/sum
    print(ratio)
    return ratio	
	
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
  
dag = DAG(
    'Numbers19', default_args=default_args,
   )
  
t1 = PythonOperator(
    task_id='Sum_of_Numbers',
    python_callable=getSum,
    dag=dag,
)
t2 = PythonOperator(
    task_id='Product_of_Numbers',
    python_callable=getProduct,
    dag=dag,
)
t3 = PythonOperator(
    task_id='ProductSumRatio_of_Numbers',
    python_callable=getProductSumRatio,
    dag=dag,
)

email = EmailOperator(
        task_id='send_email',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        to=['anukoolraj12@gmail.com'],
        subject='Airflow Alert',
        html_content=""" <h3>Success_All</h3>
                         The ratio was  """ + str(getProductSumRatio()),
        dag=dag
)  

email1 = EmailOperator(
        task_id='send_email2',
        trigger_rule=TriggerRule.ONE_SUCCESS,
        to=['anukoolraj12@gmail.com'],
        subject='Airflow Alert',
        html_content= "The product was " + str(getProduct()),
        dag=dag
)  
  
  

[t1,t2] >> email1 >> t3 >> email