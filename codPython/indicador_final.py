from pyspark.sql import functions as f
from pyspark.sql import SparkSession
import os
import toml
import boto3
spark = SparkSession.builder.appName("Trabalho_Final").getOrCreate()

def process_start_2():
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

    dir = ("/home/carlos/Desktop/airflow_demo/airflow/arquivos/download/")
    for f in os.listdir(dir):
        os.remove(os.path.join(dir, f))
    response = client.list_objects(Bucket="datalake-pucminas-carlos")
    cont = 0
    for object in response['Contents']:
        if 'parquet/' in object['Key']:
            arquivo = (object['Key'])
            if arquivo not in ("parquet/", None):
                name = arquivo.split("/")
                filename = name[1]
                print(filename)
                client.download_file("datalake-pucminas-carlos", arquivo, "/home/carlos/Desktop/airflow_demo/airflow/arquivos/download/{}".format(filename))

    df_indicador_01 = spark.read.option("header", True).parquet("/home/carlos/Desktop/airflow_demo/airflow/arquivos/download/tabela_1.parquet")
    sql = spark.read.option("header", True).parquet("/home/carlos/Desktop/airflow_demo/airflow/arquivos/download/tabela_2.parquet")

    tabela_final = (
        df_indicador_01
        .select("*")
        .join(sql, ["data"], "inner")
        .select("data", "DiaDaSemana", "qtd_tweet", "qtd_like", "qtd_retwwet", "qtd_comentarios", "id", "text")
        .sort("data")
    )

    tabela_final.write.option("header", True).mode("Overwrite").parquet("{}".format("/home/carlos/Desktop/airflow_demo/airflow/arquivos/upload"))

    origem = "/home/carlos/Desktop/airflow_demo/airflow/arquivos/upload"
    lista_dir = os.listdir(origem)

    newname = 'tabela_Final'
    for i in lista_dir:
        x = i.split('-')
        if x[0] == 'part':
            print(i)
            os.rename("{}/{}".format(origem, i), "{}/{}.parquet".format(origem,newname))
            print(newname)

if __name__ == "__main__":
    process_start_2()
