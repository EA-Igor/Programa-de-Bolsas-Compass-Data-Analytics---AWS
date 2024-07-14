import os
import boto3
from datetime import datetime
from dotenv import load_dotenv

# Carrega variáveis de ambiente a partir do arquivo .env
load_dotenv('.env')

# Configurações gerais
bucket_name = 'igor-desafio'  
storage_layer = 'Raw'  
data_origin = 'Local'  
data_format = 'CSV'  

# Caminhos dos arquivos a serem enviados
movies_file_path = '/app/data/movies.csv'
series_file_path = '/app/data/series.csv'

def upload_to_s3(file_path, data_specification):
    # Cria um cliente do S3 usando as credenciais e a região especificadas nas variáveis de ambiente
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        aws_session_token=os.getenv('AWS_SESSION_TOKEN'),
        region_name=os.getenv('AWS_DEFAULT_REGION')                 
    )
    
    # Extrai o nome do arquivo a partir do caminho completo
    file_name = os.path.basename(file_path)
    # Obtém a data atual no formato AAAA/MM/DD
    current_date = datetime.now().strftime('%Y/%m/%d')

    # Define a chave do objeto no S3 no formato especificado
    s3_key = f"{storage_layer}/{data_origin}/{data_format}/{data_specification}/{current_date}/{file_name}"

    try:
        # Tenta fazer o upload do arquivo para o S3
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"Upload bem-sucedido de {file_path} para s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Erro ao fazer upload de {file_path}: {e}")

def main():
    # Chama a função de upload para o arquivo
    upload_to_s3(movies_file_path, 'Movies')
    upload_to_s3(series_file_path, 'Series')

# Verifica se o script está sendo executado diretamente (e não importado como módulo)
if __name__ == "__main__":
    main()
