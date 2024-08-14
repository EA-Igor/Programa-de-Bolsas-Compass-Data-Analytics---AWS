import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_input_path1', 's3_input_path2', 's3_output_path1', 's3_output_path2'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

input_path_1 = args['s3_input_path1']
input_path_2 = args['s3_input_path2']
output_path_1 = args['s3_output_path1']
output_path_2 = args['s3_output_path2']

execution_date = datetime.now().strftime("dt=%Y/%m/%d")

df_series = spark.read.option("multiline", "true").json(input_path_2)

df_series.write.mode("overwrite").format("parquet").save(f"{output_path_2}/{execution_date}")

df_movies = spark.read.option("multiline", "true").json(input_path_1)

df_movies.write.mode("overwrite").format("parquet").save(f"{output_path_1}/{execution_date}")

job.commit()
