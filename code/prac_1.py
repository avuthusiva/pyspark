import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Pyspark Application").config("spark.jars","/workspaces/pyspark/jars/postgresql-42.7.7.jar").getOrCreate()
url = "jdbc:postgresql://localhost:5432/siva_db"
properties = {
    "user": "siva",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}
df = spark.read.jdbc(url=url, table="users", properties=properties)
df.show()
spark.stop()