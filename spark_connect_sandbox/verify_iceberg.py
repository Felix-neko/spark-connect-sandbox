import os
import sys
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-1.17.0-openjdk-amd64'

builder = SparkSession.builder.appName("verify-iceberg")

builder = builder.config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "$PWD/warehouse") \
    .config("spark.sql.defaultCatalog", "local")

spark = builder.getOrCreate()

print("Проверка таблицы demonyc.taxis...")
spark.sql("SHOW TABLES IN demonyc").show()

print("\nСхема таблицы:")
spark.sql("DESCRIBE TABLE demonyc.taxis").show()

print("\nСодержимое таблицы:")
spark.sql("SELECT * FROM demonyc.taxis").show()

spark.stop()
print("✓ Проверка завершена")
