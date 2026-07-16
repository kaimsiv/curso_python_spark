import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder \
    .appName("OptimizacionAvanzadaYSaltas") \
    .master("local[*]") \
    .getOrCreate()

# Ajustar nivel a ERROR para mantener la terminal impecable
spark.sparkContext.setLogLevel("ERROR")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Leer desde rutas relativas locales
df_usuarios = spark.read.csv("data/data.csv", header=True, inferSchema=True)
df_zonas = spark.read.csv("data/data2.csv", header=True, inferSchema=True)

print("\n" + "="*60)
print(f"{BOLD}{CYAN} TAREA 5: PIPELINE INTEGRADOR DE OPTIMIZACIÓN Y PERSISTENCIA COLUMNAR{RESET}")
print("="*60)
print(f"   {GREEN}[INFO]{RESET} Ejecutando un Broadcast Join en memoria remota compartida...")

# Forzar optimización enviando la tabla pequeña (df_zonas) a todos los hilos
resultado_final = df_usuarios.join(broadcast(df_zonas), "id")

# USANDO LA CARPETA 'SALIDAS'
ruta_salida = "salidas/resultado_join"
print(f"   {GREEN}[INFO]{RESET} Escribiendo resultados en formato Parquet local en: {YELLOW}{ruta_salida}{RESET}")

# .write.mode("overwrite") evita errores si ejecutas el script más de una vez
resultado_final.write.mode("overwrite").parquet(ruta_salida)

print("-" * 60)
print(f" {BOLD}{GREEN}¡Escritura completada con éxito!{RESET}")
print("-" * 60)
print(f" {BOLD}PASOS DE VALIDACIÓN FINAL EN SPARK UI (http://localhost:4040):{RESET}")
print(f"   1. Pestaña {YELLOW}'SQL / DataFrames'{RESET} -> Clic a la última query para ver el nodo {GREEN}'BroadcastHashJoin'{RESET}.")
print(f"   2. Pestaña {YELLOW}'Jobs'{RESET} -> Verás la tarea dedicada por el Driver al proceso de escritura ({CYAN}'write'{RESET}).")

input("\nPresiona ENTER para dar por concluido el laboratorio completo...")
spark.stop()
print("="*60 + "\n")