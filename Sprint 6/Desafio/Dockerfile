# Use a imagem base do Python 3.9 slim
FROM python:3.9-slim

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia todos os arquivos do diretório atual no host para o diretório de trabalho no contêiner
COPY . /app

# Instala a biblioteca boto3, que é necessária para interagir com o AWS S3
RUN pip install boto3

# Instala a biblioteca python-dotenv, que é necessária para carregar variáveis de ambiente a partir de um arquivo .env
RUN pip install python-dotenv

# Define o comando a ser executado quando o contêiner é iniciado
CMD [ "python", "Desafio.py"]
