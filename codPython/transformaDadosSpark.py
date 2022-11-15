from pyspark.sql import functions as f
from pyspark.sql import SparkSession
import os

def process_start_1():
    # Create Session
    spark = SparkSession.builder.appName("Trabalho_Final").getOrCreate()

    df = spark.read.json("/home/carlos/Desktop/airflow_demo/airflow/arquivos")

    df_indicador_01 = df.select(f.explode("data").alias("tweets"))\
                .select(f.substring("tweets.created_at",0,10).alias("data"),"tweets.public_metrics.*")\
                .selectExpr("*","""
                            CASE WEEKDAY(data) 
                                    when 0 then 'Segunda-feira'
                                    when 1 then 'Terça-feira'
                                    when 2 then 'Quarta-feira'
                                    when 3 then 'Quinta-feira'
                                    when 4 then 'Sexta-feira'
                                    when 5 then 'Sábado'
                                    when 6 then 'Domingo'
                        END AS DiaDaSemana
                """
                )\
                .groupBy("data", "DiaDaSemana")\
                .agg(
                    f.count(f.lit(1)).alias("qtd_tweet"),
                    f.sum("like_count").alias("qtd_like"),
                    (f.sum("quote_count") + f.sum("quote_count")).alias("qtd_retwwet"),
                    f.sum("reply_count").alias("qtd_comentarios")
        ).sort("data")

    df_indicador_01.write.option("header", True).mode("Overwrite").parquet("{}".format("/home/carlos/Desktop/airflow_demo/airflow/arquivos/pasta_tweet"))


    df_indicador_02 = df.select(f.explode("data").alias("tweets"))\
                        .select("tweets.id","tweets.conversation_id",f.substring("tweets.created_at", 0, 10).alias("data"),"tweets.text", "tweets.public_metrics.*")

    df_indicador_02.createOrReplaceTempView('tweet_TempView')
    sql = spark.sql(
        """
        select * from tweet_TempView where id in (
        select id from (
        select data, max(like_count), max(id) id from tweet_TempView where like_count+quote_count+reply_count+retweet_count in (
        select max(like_count+quote_count+reply_count+retweet_count) qtd_interacao  from tweet_TempView group by data)
        group by data))
        order by 3
        """
    )

    sql.write.option("header", True).mode("Append").parquet("{}".format("/home/carlos/Desktop/airflow_demo/airflow/arquivos/pasta_tweet"))

    origem = "/home/carlos/Desktop/airflow_demo/airflow/arquivos/pasta_tweet"
    lista_dir = os.listdir(origem)

    count = 0
    newname = 'tabela_'
    for i in lista_dir:
        x = i.split('-')
        if x[0] == 'part':
            print(i)
            count += 1
            os.rename("{}/{}".format(origem, i), "{}/{}{}.parquet".format(origem,newname, count))


if __name__ == "__main__":
    process_start_1()
