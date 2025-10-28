from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

file_path = "/workspaces/pyspark/files/json/drivers.json"
pg_jar = "/workspaces/pyspark/jars/postgresql-42.7.7.jar"
spark = SparkSession.builder.appName("Read json Data").config("spark.jars",pg_jar).getOrCreate()
url = "jdbc:postgresql://localhost:5432/siva_db"
properties = {
    "user": "siva",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}
#spark.sql("select current_timestamp()").show()
df = spark.read.format("json").option("inferSchema",True).load(file_path)
df.show()
df.write.jdbc(url=url,properties=properties,table="drivers",mode="append")
print("Data loaded into DB successfully")
spark.stop()