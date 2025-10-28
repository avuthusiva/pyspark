from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

url = "jdbc:postgresql://localhost:5432/siva_db"
properties = {
    "user": "siva",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}
table_name = "emp"
file_path = "/workspaces/pyspark/files/csv/EMP.csv"
spark = SparkSession.builder.appName("Postgres Data").config("spark.jars","/workspaces/pyspark/jars/postgresql-42.7.7.jar").getOrCreate()
df = spark.read.csv(file_path,header=True,inferSchema=True)
df.show()
df.write.jdbc(url=url,table=table_name,properties=properties,mode="append")
print("EMP data loaded into postgres")
spark.stop()