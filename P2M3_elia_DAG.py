'''
=================================================
**Target Market Research of Bike Company**

Nama  : Elia Oktaviani
Batch : FTDS-028-RMT

Program ini adalah rangkaian automasi fetch, celaning, dan transfer data sebelum divisualisasikan pada kibana.
Masalah yang saya bawakan pada milestone adalah segmentasi kustomer untuk menentukan launching produk baru pada champaign berikutnya.

=================================================
'''

import datetime as dt
from datetime import datetime, timedelta
import psycopg2
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from elasticsearch import Elasticsearch

import pandas as pd
import re
import time
 
# default arguments
default_args = {
    'owner': 'elia',
    # set start time of the airflow 
    'start_date': datetime(2024, 3, 23, 21, 9) - timedelta(hours = 7),
    'catchup': False,
}

#function to load data
def fetch_data():
    '''
    Fungsi ini bertujuan untuk fetch data dari database 'milestone' yang sudah disambungkan dengan apache airflow dengan koneksi:
    Nama database: milestone
    Host: postgres
    User: airflow
    Password: airflow
    Port: 5432 (airflow)
    Hasilnya data adalah data csv yang diberi nama P2M3_elia_data_raw.csv
    '''
    connection_string = "dbname='milestone' host='postgres' user='airflow' password='airflow' port='5432'"
    connection = psycopg2.connect(connection_string)
    # retrieving data
    df = pd.read_sql("SELECT * FROM table_m3", connection)
    # Close connection
    connection.close()
    df.to_csv('/opt/airflow/dags/P2M3_elia_data_raw.csv')

#function to clean data
def clean():
    '''
    Fungsi ini adalah lanjutan dari proses fetch, yaitu cleaning data data csv bernama P2M3_elia_data_raw.csv
    Pada fungsi ini dilakukan pembersihan dan normalisasi data dan nama kolom agar lebih bersih, rapih, dan representatif.
    Hasilnya data adalah data csv yang diberi nama P2M3_elia_data_clean.csv
    '''
    #read csv
    df=pd.read_csv('/opt/airflow/dags/P2M3_elia_data_raw.csv')
    #handle dulicate data
    df.drop_duplicates(inplace=True)
    #handle missing value
    df=df.dropna(subset=['Date']) #drop null data row (no information)
    df.drop(columns=['Column1'], inplace=True) #drop Column1 (unessesary)
    #normalize columns name
    df.columns = [column.lower().replace(' ', '_') for column in df.columns] #lowercase columns, replace space with '_'
    pattern = r'[^\w\s]'
    df.columns = [re.sub(pattern, '', column) for column in df.columns]
    #change datatype for year and age into integer
    df['year'] = df['year'].astype(int)
    df['customer_age'] = df['customer_age'].astype(int)
    #save into csv data
    df.to_csv('/opt/airflow/dags/P2M3_elia_data_clean.csv')

def post_to_elasticsearch_process():
    '''
    Fungsi ini adalah lanjutan dari proses clean, yaitu transfer data ke kibana untuk divisualisasikan
    Pada fungsi ini dilakukan pembacaan data csv bernama P2M3_elia_data_clean.csv
    Kemudian data dibaca per baris untuk diolah menjadi file json.
    File json ini lah yang akan dijadikan sebagai document pada data no sql di kibana
    Hasilnya data adalah index bernama 'milestone3' yang berisi dokumen-dokumen sesuai dengan data row file csv yang sudah dibaca. 
    '''
    es = Elasticsearch('http://elasticsearch:9200') #define the elasticsearch url
    print(es.ping())
    df=pd.read_csv('/opt/airflow/dags/P2M3_elia_data_clean.csv') 
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="milestone3", doc_type = "doc", body=doc)
        print(res)

# define the DAG, the data is updated every 3 minutes
with DAG("milestone3",
    description='Final Assignment Milestone 3',
    #schedule airflow to run every 6.30AM WIB
    schedule_interval='30 6 * * *',
    default_args=default_args,
    catchup=False
) as dag:

    # Task 1: Fetch data from PostgreSQL
    fetch_data_cutomer = PythonOperator(task_id='fetch_data_from_progresql',
                               python_callable=fetch_data,)

    # Task for data cleaning
    data_cleaning = PythonOperator(task_id='clean_data',
                                python_callable=clean,)

    # Task 3: Transport clean CSV into Elasticsearch
    post_to_elasticsearch = PythonOperator(task_id='transfer_data_to_kibana',
                                python_callable=post_to_elasticsearch_process,)

fetch_data_cutomer >> data_cleaning >> post_to_elasticsearch