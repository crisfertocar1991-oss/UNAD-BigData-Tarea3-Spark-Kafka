
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, from_unixtime, window, avg, count
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType,
    DoubleType, LongType
)

# Crear la sesion de Apache Spark
spark = (
    SparkSession.builder
    .appName("Spark_Streaming_Kafka_Sensores")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Estructura de los mensajes JSON
esquema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# Leer mensajes en tiempo real desde Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor_data")
    .option("startingOffsets", "earliest")
    .load()
)

# Convertir los mensajes JSON en columnas
datos = (
    kafka_df
    .select(
        from_json(
            col("value").cast("string"),
            esquema
        ).alias("datos")
    )
    .select("datos.*")
)

# Convertir la fecha del sensor a formato timestamp
datos = datos.withColumn(
    "fecha",
    from_unixtime(col("timestamp")).cast("timestamp")
)

# Calcular promedios por sensor cada minuto
resultados = (
    datos
    .groupBy(
        window(col("fecha"), "1 minute"),
        col("sensor_id")
    )
    .agg(
        avg("temperature").alias("temperatura_promedio"),
        avg("humidity").alias("humedad_promedio"),
        count("*").alias("total_registros")
    )
)

# Mostrar los resultados en la terminal
consulta = (
    resultados.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option("numRows", 30)
    .trigger(processingTime="10 seconds")
    .start()
)

print("\nSPARK STREAMING INICIADO CORRECTAMENTE")
print("Procesando datos del tema sensor_data...")
print("Presiona Ctrl+C para detener.\n")

try:
    consulta.awaitTermination()
except KeyboardInterrupt:
    print("\nDeteniendo Spark Streaming...")
finally:
    consulta.stop()
    spark.stop()
    print("Spark Streaming finalizado.")

