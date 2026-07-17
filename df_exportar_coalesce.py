import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CrearDFMemoria").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Estilos visuales para la terminal
CYAN = "\033[96m"
GREEN = "\033[92m"
BOLD = "\033[1m"
RESET = "\033[0m"

print("\n" + "="*60)
print(f"{BOLD}{CYAN} 1. DATAFRAME A PARTIR DE UNA LISTA DE TUPLAS (10 MESES){RESET}")
print("="*60)
data_lista = [
    ("Enero", 34667), ("Febrero", 48795), ("Marzo", 87548), 
    ("Abril", 62300), ("Mayo", 54100), ("Junio", 71200),
    ("Julio", 68900), ("Agosto", 73400), ("Septiembre", 61000), 
    ("Octubre", 82300)
]
df_lista = spark.createDataFrame(data_lista, ["Mes", "Ingreso"])
df_lista.show(10, truncate=False)

print("\n" + "="*60)
print(f"{BOLD}{GREEN} 2. DATAFRAME A PARTIR DE UNA LISTA DE DICCIONARIOS (10 USUARIOS){RESET}")
print("="*60)
data_dict = [
    {"Nombre": "Alfonso", "Edad": 25, "Ciudad": "Abejorral"},
    {"Nombre": "Bernardo", "Edad": 30, "Ciudad": "Bogota"},
    {"Nombre": "Celeste", "Edad": 35, "Ciudad": "Cartagena"},
    {"Nombre": "Daniel", "Edad": 28, "Ciudad": "Medellin"},
    {"Nombre": "Elena", "Edad": 42, "Ciudad": "Cali"},
    {"Nombre": "Fernando", "Edad": 19, "Ciudad": "Manizales"},
    {"Nombre": "Gabriela", "Edad": 31, "Ciudad": "Pereira"},
    {"Nombre": "Hugo", "Edad": 50, "Ciudad": "Bucaramanga"},
    {"Nombre": "Isabel", "Edad": 24, "Ciudad": "Santa Marta"},
    {"Nombre": "Jorge", "Edad": 37, "Ciudad": "Ibague"}
]
df_dict = spark.createDataFrame(data_dict)
df_dict.show(10, truncate=False)

print(f"{BOLD} Esquema del Diccionario:{RESET}")
df_dict.printSchema()
print("="*60 + "\n")

spark.stop()