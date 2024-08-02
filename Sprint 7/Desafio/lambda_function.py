import boto3
import json
from datetime import datetime
from filmes import fetch_romance_drama_movies
from series import fetch_romance_drama_tv_shows

# Inicialização do cliente S3
s3_client = boto3.client('s3')

# converção de objetos para dicionários
def convert_to_dict(obj):
    if hasattr(obj, '__dict__'):
        return obj.__dict__
    else:
        raise TypeError(f"Objeto {obj} não pode ser convertido para dicionário")

# salvamento em JSON no S3
def save_json_to_s3(data, media_type, index):
    current_date = datetime.now().strftime('%Y-%m-%d')
    year, month, day = current_date.split('-')
    
    s3_bucket_name = 'igor-desafio'
    s3_path = f"Raw/tmdb/json/{media_type}/{year}/{month}/{day}/"
    file_name = f"{media_type}data{current_date}{index}{len(data)}.json"
    full_path = f"{s3_path}{file_name}"

    json_data = json.dumps(data, ensure_ascii=False, indent=4, default=convert_to_dict)
    s3_client.put_object(Body=json_data, Bucket=s3_bucket_name, Key=full_path)
    print(f"Dados salvos em {full_path}")

# Função principal para execução no AWS Lambda
def lambda_handler(event, context):
    index = 0
    for data_chunk in fetch_romance_drama_movies():
        if data_chunk:
            save_json_to_s3(data_chunk, 'movies', index)
            index += 1

    index = 0
    for data_chunk in fetch_romance_drama_tv_shows():
        if data_chunk:
            save_json_to_s3(data_chunk, 'tv_shows', index)
            index += 1

    return {
        'statusCode': 200,
        'body': json.dumps('Dados de filmes e séries salvos com sucesso no S3')
    }
