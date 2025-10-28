from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

file_path = "/workspaces/pyspark/files/csv/BigMart Sales.csv"
postgres_jar = "/workspaces/pyspark/jars/postgresql-42.7.7.jar"
spark = SparkSession.builder.appName("Reading Spark Data").config("spark.jars",postgres_jar).getOrCreate()
df = spark.read.format("csv").option("header",True).option("inferSchema",True).load(file_path)
df.show()
spark.stop()