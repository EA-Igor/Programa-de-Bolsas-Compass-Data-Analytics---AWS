# S3 Select Query Execution Script

Este script executa uma consulta S3 Select e imprime os resultados no terminal. Foi utilizada a linguagem pytohn juntamente com a biblioteca Boto3. Para o arquivo .csv foi escolhido um com o tema: quantitativo de internações realizadas na UTI por mês de maio a outubro de 2022 no Hospital Universitário da Universidade Federal da Grande Dourados – HU-UFGD.

## Código

```python
import boto3

# Execução da consulta S3 Select e impressão dos resultados no terminal
def s3_query(bucket, arquivo, query):
    s3 = boto3.client('s3')
    
    resultado = s3.select_object_content(
        Bucket=bucket,
        Key=arquivo,
        ExpressionType='SQL',
        Expression=query,
        InputSerialization={'CSV': {"FileHeaderInfo": "USE"}},
        OutputSerialization={'CSV': {}},
    )
    
    # Processamento dos resultados da consulta e impressão no terminal
    for event in resultado['Payload']:
        if 'Records' in event:
            registro = event['Records']['Payload'].decode('utf-8')
            print(registro)  # Imprime os registros no terminal

# Nome do bucket e do arquivo
bucket = 'igor-desafio'
arquivo = 'quantitativodeinternaesrealizadasnaUTIAdulto_maio22aoutubro2022.csv'

# Query que atende aos requisitos do desafio
query = """
SELECT 
    COUNT(*) AS total_internacoes,
    SUM(CASE WHEN totalizador > '30' THEN 1 ELSE 0 END) AS contagem_alta
FROM S3Object
WHERE SUBSTRING(CAST(ano AS CHAR), 1, 4) = '2022' AND (mes > '5' OR mes < '10' OR UTCNOW() <> '01-10-2025')
"""

s3_query(bucket, arquivo, query)
