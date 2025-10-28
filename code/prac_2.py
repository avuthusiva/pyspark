from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Read csv data").config("spark.jars","/workspaces/pyspark/jars/postgresql-42.7.7.jar").getOrCreate()
url = "jdbc:postgresql://localhost:5432/siva_db"
properties = {
    "user": "siva",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}
table_name = "DEPT"
df = spark.read.option("header",True).csv("/workspaces/pyspark/files/csv/DEPT.csv")
df.show()
df.write.jdbc(url=url,properties=properties,table=table_name,mode="append")
print("Data loaded into database")
spark.stop()