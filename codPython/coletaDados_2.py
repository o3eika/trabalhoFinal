import sys
sys.path.append("/home/carlos/Desktop/airflow_demo/airflow")
from pathlib import Path
from os.path import join
import toml
import datetime
#import tweepy as tw
import json
import pandas as pd
import requests
import os

# abrir o arquivo com as credenciais de acesso
with open("/home/carlos/Documents/APITw.toml") as config:
    # ler o arquivo e salvar as chaves nas variáveis
   config = toml.loads(config.read())
   APP_NAME = config['APP_NAME']
   API_KEY = config['API_KEY']
   API_KEY_SECRET = config['API_KEY_SECRET']
   ACCESS_TOKEN = config['ACCESS_TOKEN']
   ACCESS_TOKEN_SECRET = config['ACCESS_TOKEN_SECRET']
   BEARER_TOKEN = config['BEARER_TOKEN']

# BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAFZSiwEAAAAA%2FF75VThXwHaegA3yMJ8GWtEQHrI%3DnihIOVIN2mnzN4hq7HmqhC3g6TiV68FcjOFTzuZ9CLJKKr8a6g'
def auth():
    return BEARER_TOKEN

def create_url():
    query = "PUC Minas"
    dateMin = datetime.date.today()- datetime.timedelta(days=5)
    dateAtual = datetime.date.today()
    tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text"
    user_fields = "expansions=author_id&user.fields=id,name,username,created_at,location"
    filters = "start_time={}T00:00:00.00Z&end_time={}T00:00:00.00Z".format(dateMin, dateAtual)
    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}".format(
        query, tweet_fields, user_fields, filters
    )
    return url

def create_parent_folder(file_path):
        Path(Path(file_path).parent).mkdir(parents=True, exist_ok=True)

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def paginate(url, hearders, next_token=""):
    if next_token:
        full_url = f"{url}&next_token={next_token}"
    else:
        full_url = url
    data = connect_to_endpoint(full_url, hearders)
    yield data
    if "next_token" in data.get("meta", {}):
        yield from paginate(url, hearders, data['meta']['next_token'])

def main():
    bearer_token = auth()
    url = create_url()
    headers = create_headers(bearer_token)

    caminho = "/home/carlos/Desktop/airflow_demo/airflow/arquivos/data_tweet.json"
    create_parent_folder(caminho)
    with open(caminho, 'w') as output_file:
                for pg in paginate(url, headers):
                    json.dump(pg, output_file)
                    output_file.write("\n")

if __name__ == "__main__":
    main()
