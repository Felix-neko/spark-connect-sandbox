import os
import sys
from pyspark.sql.types import DoubleType, FloatType, LongType, StructType,StructField, StringType
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-1.17.0-openjdk-amd64'

builder = SparkSession.builder.appName("mega-app")

builder = builder.config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "$PWD/warehouse") \
    .config("spark.sql.defaultCatalog", "local")

spark = builder.getOrCreate()

print(spark.sparkContext._gateway.jvm.scala.util.Properties.versionString())

schema = StructType([
    StructField("vendor_id", LongType(), True),
    StructField("trip_id", LongType(), True),
    StructField("trip_distance", FloatType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True)
])

df = spark.createDataFrame([], schema)
print("Создание таблицы demonyc.taxis...")
df.writeTo("demonyc.taxis").createOrReplace()
print("✓ Таблица demonyc.taxis успешно создана!")

spark.stop()
print("✓ Spark-сессия завершена")
