import sys
sys.path.append("/home/carlos/Desktop/airflow_demo/airflow")
from codPython.coletaDados_2 import main
from codPython.transformaDadosSpark import process_start_1
from codPython.indicador_final import process_start_2
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import boto3
import toml
import random


def coleta_dados():
    main()

def tranfroma_dados():
    process_start_1()

def indicador_final():
    process_start_2()    

def upload_to_s3(filename:str, format_:str, name_ = None) -> None:
    with open("/home/carlos/Documents/APITw.toml") as config:
		# ler o arquivo e salvar as chaves nas variáveis
        config = toml.loads(config.read())
        AWS_BUCKET_NAME = config['AWS_BUCKET_NAME']
        AWS_ACCESS_KEY = config['AWS_ACCESS_KEY']
        AWS_SECRET_ACCESS_KEY = config['AWS_SECRET_ACCESS_KEY']

    client = boto3.client(
    	service_name = 's3',
    	aws_access_key_id = AWS_ACCESS_KEY,
    	aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
    	region_name = 'sa-east-1'
    )
    format_arq = format_
    name_id = random.randint(1, 999999999999)
    nvl = lambda a, b: b if a is None else a
    name_id = nvl(name_, name_id)
    file_name = '{}.{}'.format(name_id, format_arq)
    full_name = '{}/{}'.format(format_arq, file_name)
    s3_path = AWS_BUCKET_NAME
    source_path = filename

    client.upload_file(
		source_path,
		s3_path,
		full_name
	)


with DAG('dag_trabalho_final',
          start_date= datetime(2022,11,4),
          schedule_interval= '30 * * * *',
          catchup=False) as dag:


    coleta_dados = PythonOperator(
        task_id='coleta_dados',
        python_callable= coleta_dados
    )

    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': "/home/carlos/Desktop/airflow_demo/airflow/arquivos/data_tweet.json",
            'format_': "json"
        }
    )
    
    task_tranforma_dados = PythonOperator(
        task_id='task_tranforma_dados',
        python_callable= tranfroma_dados
    )

    task_upload_table1 = PythonOperator(
        task_id='task_upload_table1',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': "/home/carlos/Desktop/airflow_demo/airflow/arquivos/pasta_tweet/tabela_1.parquet",
            'format_': "parquet",
            'name_': "tabela_1"
        }
    )

    task_upload_table2 = PythonOperator(
        task_id='task_upload_table2',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': "/home/carlos/Desktop/airflow_demo/airflow/arquivos/pasta_tweet/tabela_2.parquet",
            'format_': "parquet",
            'name_': "tabela_2"
        }
    )

    indicador_final = PythonOperator(
        task_id='indicador_final',
        python_callable= indicador_final
    )

    upload_indicador_final = PythonOperator(
        task_id='upload_indicador_final',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': "/home/carlos/Desktop/airflow_demo/airflow/arquivos/upload/tabela_Final.parquet",
            'format_': "parquet",
            'name_': "tabela_final"
        }
    )

    valido = BashOperator(
        task_id='CargaCompleta',
        bash_command="echo 'StatusOK'"
    )


    coleta_dados >>  task_upload_to_s3 >> \
    task_tranforma_dados >> [task_upload_table1, task_upload_table2] >> \
    indicador_final >> upload_indicador_final >> valido
    
