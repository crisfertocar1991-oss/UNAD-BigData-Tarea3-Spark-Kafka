from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import matplotlib

# Permite guardar graficas sin entorno grafico
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import os

# Carpeta donde se guardaran las graficas
carpeta = "/home/hadoop/tarea3_spark/graficas"
os.makedirs(carpeta, exist_ok=True)

# Iniciar Apache Spark
spark = (
    SparkSession.builder
    .appName("Graficas_Homicidios_Transito")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Cargar datos desde Hadoop HDFS
ruta = "hdfs://bigdata:9000/Tarea3_Spark/homicidios_transito.csv"

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(ruta)
)

# Preparar los datos
df = df.withColumn(
    "FECHA",
    F.to_date(F.col("FECHA HECHO"), "dd/MM/yyyy")
)

df = df.withColumn("ANIO", F.year("FECHA"))

datos = df.filter(
    F.col("FECHA").isNotNull()
    & F.col("CANTIDAD").isNotNull()
    & (F.col("CANTIDAD") > 0)
)

# GRAFICA 1: Departamentos con mas victimas
departamentos = (
    datos.groupBy("DEPARTAMENTO")
    .agg(F.sum("CANTIDAD").alias("TOTAL"))
    .orderBy(F.desc("TOTAL"))
    .limit(10)
    .collect()
)

nombres = [fila["DEPARTAMENTO"] for fila in departamentos]
valores = [fila["TOTAL"] for fila in departamentos]

plt.figure(figsize=(12, 6))
plt.barh(nombres[::-1], valores[::-1], color="steelblue")
plt.title("10 departamentos con mas victimas")
plt.xlabel("Cantidad de victimas")
plt.tight_layout()
plt.savefig(
    os.path.join(carpeta, "01_departamentos.png"),
    dpi=150
)
plt.close()

print("Grafica 1 guardada correctamente")

# GRAFICA 2: Evolucion anual
anios = (
    datos.groupBy("ANIO")
    .agg(F.sum("CANTIDAD").alias("TOTAL"))
    .orderBy("ANIO")
    .collect()
)

eje_anios = [fila["ANIO"] for fila in anios]
eje_valores = [fila["TOTAL"] for fila in anios]

plt.figure(figsize=(12, 6))
plt.plot(eje_anios, eje_valores, marker="o", color="darkgreen")
plt.title("Evolucion anual de victimas en accidentes de transito")
plt.xlabel("Anio")
plt.ylabel("Cantidad de victimas")
plt.xticks(eje_anios, rotation=45)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(
    os.path.join(carpeta, "02_evolucion_anual.png"),
    dpi=150
)
plt.close()

print("Grafica 2 guardada correctamente")

# GRAFICA 3: Municipios con mas victimas
municipios = (
    datos.groupBy("DEPARTAMENTO", "MUNICIPIO")
    .agg(F.sum("CANTIDAD").alias("TOTAL"))
    .orderBy(F.desc("TOTAL"))
    .limit(10)
    .collect()
)

nombres_municipios = [
    fila["MUNICIPIO"] + " (" + fila["DEPARTAMENTO"] + ")"
    for fila in municipios
]
valores_municipios = [fila["TOTAL"] for fila in municipios]

plt.figure(figsize=(12, 6))
plt.barh(
    nombres_municipios[::-1],
    valores_municipios[::-1],
    color="darkorange"
)
plt.title("10 municipios con mas victimas")
plt.xlabel("Cantidad de victimas")
plt.tight_layout()
plt.savefig(
    os.path.join(carpeta, "03_municipios.png"),
    dpi=150
)
plt.close()

print("Grafica 3 guardada correctamente")

# Finalizar Spark
spark.stop()

print("\nProceso finalizado.")
print("Graficas guardadas en:", carpeta)
