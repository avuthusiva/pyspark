from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

file_path = "/workspaces/pyspark/files/csv/BigMart Sales.csv"
postgres_jar = "/workspaces/pyspark/jars/postgresql-42.7.7.jar"
url = "jdbc:postgresql://localhost:5432/siva_db"
properties = {
    "user": "siva",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}
spark = SparkSession.builder.appName("Reading Spark Data").config("spark.jars",postgres_jar).getOrCreate()
df = spark.read.format("csv").option("header",True).option("inferSchema",True).load(file_path)
df.show()
df.write.jdbc(url=url,properties=properties,table="sales",mode="append")
print("Data loaded to DB")
spark.stop()