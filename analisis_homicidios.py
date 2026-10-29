from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Crear la sesión de Apache Spark
spark = (
    SparkSession.builder
    .appName("Analisis_Homicidios_Transito_Colombia")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 2. Cargar el conjunto de datos desde Hadoop HDFS
ruta = "hdfs://bigdata:9000/Tarea3_Spark/homicidios_transito.csv"

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(ruta)
)

# 3. Transformar y preparar los datos
df = df.withColumn(
    "FECHA",
    F.to_date(F.col("FECHA HECHO"), "dd/MM/yyyy")
)

df = df.withColumn("ANIO", F.year(F.col("FECHA")))

# 4. Verificar la calidad de los datos
print("\n=== CALIDAD DE LOS DATOS ===")
print("Total de registros:", df.count())
print(
    "Fechas nulas:",
    df.filter(F.col("FECHA").isNull()).count()
)
print(
    "Cantidades nulas:",
    df.filter(F.col("CANTIDAD").isNull()).count()
)
print(
    "Cantidades negativas o iguales a cero:",
    df.filter(F.col("CANTIDAD") <= 0).count()
)

# 5. Conservar registros validos para el analisis
datos = df.filter(
    F.col("FECHA").isNotNull()
    & F.col("CANTIDAD").isNotNull()
    & (F.col("CANTIDAD") > 0)
)

print("\n=== RESUMEN GENERAL ===")
print("Registros validos:", datos.count())

datos.agg(
    F.sum("CANTIDAD").alias("TOTAL_VICTIMAS")
).show()

# 6. Primera pregunta: departamentos con mas victimas
print("\n=== VICTIMAS POR DEPARTAMENTO ===")

por_departamento = (
    datos.groupBy("DEPARTAMENTO")
    .agg(F.sum("CANTIDAD").alias("TOTAL_VICTIMAS"))
    .orderBy(F.desc("TOTAL_VICTIMAS"))
)

por_departamento.show(15, truncate=False)

# 7. Segunda pregunta: evolucion anual
print("\n=== EVOLUCION ANUAL DE VICTIMAS ===")

por_anio = (
    datos.groupBy("ANIO")
    .agg(F.sum("CANTIDAD").alias("TOTAL_VICTIMAS"))
    .orderBy("ANIO")
)

por_anio.show(50, truncate=False)

# 8. Tercera pregunta: municipios con mas victimas
print("\n=== VICTIMAS POR MUNICIPIO ===")

por_municipio = (
    datos.groupBy("DEPARTAMENTO", "MUNICIPIO")
    .agg(F.sum("CANTIDAD").alias("TOTAL_VICTIMAS"))
    .orderBy(F.desc("TOTAL_VICTIMAS"))
)

por_municipio.show(15, truncate=False)

# 9. Finalizar la sesion
spark.stop()
