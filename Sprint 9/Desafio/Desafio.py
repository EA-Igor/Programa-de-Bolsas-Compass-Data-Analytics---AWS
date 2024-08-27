import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, when, lit

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_input_path1', 's3_input_path2', 's3_input_csv_path', 's3_output_dim_show', 's3_output_dim_actor', 's3_output_fact_ratings'])
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

json_path = args['s3_input_path1']
json_path2 = args['s3_input_path2']
csv_path = args['s3_input_csv_path']

movies_df = spark.read.parquet(json_path)
series_df = spark.read.parquet(json_path2)
csv_df = spark.read.parquet(csv_path)

csv_df = csv_df.withColumnRenamed("tituloPincipal", "titulo_csv") \
               .withColumn("tempoMinutos", col("tempoMinutos").cast("int"))

movies_merged_df = movies_df.alias("filmes").join(
    csv_df.alias("csv"), 
    col("filmes.title") == col("csv.titulo_csv"), 
    "left"
)

movies_final_df = movies_merged_df.withColumn(
    "duracao", 
    when(col("csv.titulo_csv").isNotNull(), col("csv.tempoMinutos"))
    .otherwise(col("filmes.duration"))
)

common_df = movies_final_df.select(
    col("filmes.id").alias("id"),
    col("filmes.title").alias("titulo"),
    col("filmes.original_language").alias("idioma_original"),
    col("filmes.overview").alias("descricao"),
    col("filmes.main_actor").alias("ator_principal"),
    col("filmes.popularity").alias("popularidade"),
    col("filmes.release_date").alias("data_lancamento"),
    col("filmes.vote_average").alias("media_votos"),
    col("filmes.vote_count").alias("total_votos"),
    col("duracao"),
    lit("Filme").alias("tipo")
).unionByName(
    series_df.select(
        col("id"),
        col("name").alias("titulo"),  
        col("original_language").alias("idioma_original"),
        col("overview").alias("descricao"),
        col("main_actor").alias("ator_principal"),
        col("popularity").alias("popularidade"),
        col("first_air_date").alias("data_lancamento"),  
        col("vote_average").alias("media_votos"),
        col("vote_count").alias("total_votos"),
        lit(None).cast("int").alias("duracao"),  
        lit("Serie").alias("tipo")
    )
)

dim_show_df = common_df.select(
    col("id"),
    col("titulo"),
    col("idioma_original"),
    col("descricao"),
    col("data_lancamento"),
    col("duracao"),
    col("tipo")
).dropDuplicates()

dim_actor_df = common_df.select(
    col("id"),  
    col("ator_principal").alias("nome_ator")
).dropDuplicates()

fact_ratings_df = common_df.select(
    col("id"),  
    col("popularidade"),
    col("media_votos"),
    col("total_votos")
)

dim_show_output_path = args['s3_output_dim_show']
dim_actor_output_path = args['s3_output_dim_actor']
fact_ratings_output_path = args['s3_output_fact_ratings']

dim_show_df.write.mode("overwrite").parquet(dim_show_output_path)
dim_actor_df.write.mode("overwrite").parquet(dim_actor_output_path)

fact_ratings_df.write.mode("overwrite").parquet(fact_ratings_output_path)

job.commit()
