import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

# Configurações do Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_input_path1', 's3_input_path2', 's3_output_path1', 's3_output_path2'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Caminhos de entrada e saída via argumentos
input_path_1 = args['s3_input_path1']
input_path_2 = args['s3_input_path2']
output_path_1 = args['s3_output_path1']
output_path_2 = args['s3_output_path2']

# Leitura dos dados com o delimitador |
df1 = spark.read.format("csv").option("header", "true").option("sep", "|").load(input_path_1)
df2 = spark.read.format("csv").option("header", "true").option("sep", "|").load(input_path_2)

# Aplicando os filtros no DataFrame de filmes (df1) para Drama e Romance entre 1980 e 1990
df1 = df1.filter((col("anoLancamento") >= 1980) & (col("anoLancamento") <= 1990))
df1 = df1.filter(col("genero").rlike(r'\b(Drama|Romance)\b'))

# Aplicando os filtros no DataFrame de séries (df2) para todas as séries de Drama e Romance
df2 = df2.filter(col("genero").rlike(r'\b(Drama|Romance)\b'))

# Gravação dos dados
df1.write.mode("overwrite").format("parquet").save(output_path_1)
df2.write.mode("overwrite").format("parquet").save(output_path_2)

# Finalizando o job
job.commit()
