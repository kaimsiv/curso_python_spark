import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

# 1. Inicializar la Sesión de Spark Primaria (Contexto de Carga)
spark_principal = SparkSession.builder.appName("SesionPrincipal").getOrCreate()
spark_principal.sparkContext.setLogLevel("ERROR")

CYAN = "\033[96m"
GREEN = "\033[92m"
BOLD = "\033[1m"
RESET = "\033[0m"

df_productos = spark_principal.read.csv("data/Model/Products.csv", inferSchema=True, header=True)

print("\n" + "="*60)
print(f"{BOLD}{CYAN} CONTEXTO: REGISTRO DE VISTA GLOBAL EN EL CATÁLOGO{RESET}")
print("="*60)
# Registrar de forma global en el catálogo distribuido
df_productos.createOrReplaceGlobalTempView("catalogo_global")
print(f"   {GREEN}[INFO]{RESET} Vista global 'catalogo_global' registrada con éxito en la sesión primaria.")
print("-" * 60)

# 2. Inicializar una Nueva Sesión de Spark Independiente (Simulación de consulta aislada)
spark_aislada = SparkSession.builder.appName("SesionAislada").getOrCreate()
spark_aislada.sparkContext.setLogLevel("ERROR")

print(f"{BOLD} ACCEDIENDO A GLOBAL_TEMP DESDE UNA SPARKSESSION SECUNDARIA{RESET}")
print("-" * 60)
resultado = spark_aislada.sql("""
    SELECT ProductKey, Product, Category, Price 
    FROM global_temp.catalogo_global
""")
resultado.show(10, truncate=False)
print("="*60 + "\n")

# Cerrar el ecosistema de sesiones de forma limpia
spark_principal.stop()
spark_aislada.stop()