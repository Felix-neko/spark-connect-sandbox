import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:15002").enableHiveSupport().getOrCreate()

print(pyspark.__version__)

# Создание тестового DataFrame
data = [
    (1, "Иван Петров", 28, "Москва"),
    (2, "Мария Сидорова", 34, "Санкт-Петербург"),
    (3, "Алексей Иванов", 25, "Новосибирск"),
    (4, "Елена Козлова", 41, "Екатеринбург"),
    (5, "Дмитрий Смирнов", 29, "Казань"),
    (6, "Ольга Морозова", 33, "Ростов-на-Дону")
]

columns = ["id", "name", "age", "city"]
df = spark.createDataFrame(data, columns)

print("Исходный DataFrame:")
df.show()

# === ЗАПИСЬ в Cassandra ===
print("Записываем данные в Cassandra...")
(df.write
    .format("org.apache.spark.sql.cassandra")
    .options(
        table="users",
        keyspace="default",
        spark_cassandra_connection_host="172.19.0.1",
        spark_cassandra_connection_port="9042",
        spark_cassandra_auth_username="cassandra",
        spark_cassandra_auth_password="cassandra"
    )
    .mode("append")
    .save()
)
print("✓ Данные успешно записаны в Cassandra!")

# === ЧТЕНИЕ из Cassandra ===
print("\nЧитаем данные из Cassandra...")
df_read = (spark.read
    .format("org.apache.spark.sql.cassandra")
    .options(
        table="users",
        keyspace="default",
        spark_cassandra_connection_host="172.19.0.1",
        spark_cassandra_connection_port="9042",
        spark_cassandra_auth_username="cassandra",
        spark_cassandra_auth_password="cassandra"
    )
    .load()
)

print("Данные из Cassandra:")
df_read.show()

# Проверка количества записей
print(f"Количество записей в таблице: {df_read.count()}")

spark.stop()
