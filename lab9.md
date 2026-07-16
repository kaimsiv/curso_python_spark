# Práctica 9. Monitoreo y optimización de PySpark (Versión Interactiva VS Code)

## Objetivo

Al finalizar la práctica, se espera que el estudiante sea capaz de:

* Identificar y aplicar mejores prácticas de desempeño en entornos de desarrollo de PySpark.
* Interpretar planes de ejecución e interactuar con la **Spark UI** de forma local.
* Utilizar e identificar estrategias de optimización como **Caché** y **Broadcast Joins**.

### Objetivo visual

Se espera que el estudiante observe de forma clara cómo el monitoreo del rendimiento, la caché y los joins broadcast impactan en la eficiencia del procesamiento en Spark.

### Duración aproximada

* 60 minutos.

---


### Instrucciones

Se describen los pasos requeridos para completar la práctica de forma ordenada y coherente.

## Tarea 1. Preparación del Entorno Local y Datos de Prueba

Crearemos de forma segura las carpetas locales y los archivos de datos iniciales con un mínimo de 10 registros por set utilizando la terminal integrada de VS Code.

### Paso 1. Inicialización del directorio de trabajo

Abre tu terminal integrada en Visual Studio Code (`Ctrl + ~`) y ejecuta los siguientes comandos Bash:

```bash
# Crear las carpetas de datos de entrada y de resultados locales
mkdir -p data
mkdir -p salidas

# 1. Generar archivo data.csv con 10 registros consistentes de usuarios
echo -e "nombre,edad,ciudad,id\nMiguel,31,CDMX,1\nAna,25,Guadalajara,2\nPedro,45,Monterrey,3\nDaniela,28,CDMX,4\nElena,42,Guadalajara,5\nFernando,19,Monterrey,6\nGabriela,31,CDMX,7\nHugo,50,Guadalajara,8\nIsabel,24,Monterrey,9\nJorge,37,CDMX,10" > data/data.csv

# 2. Generar archivo data2.csv con 10 registros de zonas vinculados por ID
echo -e "id,zona\n1,Norte\n2,Occidente\n3,Norte\n4,Centro\n5,Occidente\n6,Sur\n7,Centro\n8,Occidente\n9,Sur\n10,Centro" > data/data2.csv

```

---

## Tarea 2. Monitoreo del Código e Interfaz Gráfica (Spark UI)

Para inspeccionar la **Spark UI** de forma local, añadiremos una pausa interactiva en el script. De este modo, la sesión de Spark no se cerrará de inmediato y podrás abrir tu navegador web.

### Paso 2. Crear el script de Monitoreo

Crea un archivo en VS Code llamado `1_monitoreo_ui.py`, añade el siguiente código y ejecútalo con el botón de **Play (▶)** o la tecla **F5**:

```python
import os
# Inyección dinámica obligatoria para forzar Java 17 local
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

# Inicializar SparkSession local configurando explícitamente el puerto web
spark = SparkSession.builder \
    .appName("MonitoreoInteractiva") \
    .master("local[*]") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

# Ajustar nivel de logs para silenciar trazas innecesarias
spark.sparkContext.setLogLevel("ERROR")

CYAN = "\033[96m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

print("\n" + "="*60)
print(f"{BOLD}{CYAN} TAREA 2: MONITOREO DE CARGA DE DATOS E INTERFAZ GRÁFICA{RESET}")
print("="*60)
print(f"{BOLD} Recuperando registros estructurados del DataFrame:{RESET}")
df = spark.read.csv("data/data.csv", header=True, inferSchema=True)
df.show(10, truncate=False)

print("-" * 60)
print(f" {BOLD}{YELLOW}¡LA SPARK UI ESTÁ ACTIVA EN ESTE MOMENTO!{RESET}")
print(f"    Abre tu navegador web e ingresa a: {CYAN}http://localhost:4040{RESET}")
print("-" * 60)

# Pausa interactiva obligatoria para mantener la Spark UI encendida
input("\n--> Revisa la página web. Cuando termines, presiona ENTER aquí para apagar Spark... ")

spark.stop()
print(f"\n {BOLD}Sesión finalizada. Interfaz web apagada correctamente.{RESET}\n")

```

---

## Tarea 3. Application de Mejores Prácticas (Esquemas y Caché)

En esta sección aprenderás a optimizar la memoria declarando esquemas explícitos y guardando DataFrames intermedios en la RAM.

### Paso 3. Crear el script de rendimiento

Crea un archivo llamado `2_buenas_practicas.py` en VS Code y ejecútalo (**Play ▶**):

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("CacheYEsquemas") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# 1. Definición explícita de esquema (Mejor práctica para evitar lecturas ineficientes)
schema = StructType([
    StructField("nombre", StringType(), True),
    StructField("edad", IntegerType(), True),
    StructField("ciudad", StringType(), True),
    StructField("id", IntegerType(), True)
])

df = spark.read.schema(schema).csv("data/data.csv", header=True)

print("\n" + "="*60)
print(f"{BOLD}{GREEN} TAREA 3: ASIGNACIÓN DE ESQUEMA EXPLICITO Y CACHÉ EN RAM{RESET}")
print("="*60)
print(f"   {GREEN}[INFO]{RESET} Cargando registros y guardando DataFrame en Caché...")
df.cache()

# El caché es perezoso (lazy); necesitamos detonar una acción para obligar a cargarlo
total_filas = df.count() 

print("-" * 60)
print(f" {BOLD}Métricas Consolidadas:{RESET}")
print(f"    Acción completada de forma exitosa. Total filas: {YELLOW}{total_filas}{RESET}")
print("-" * 60)
print(f" {BOLD}PASO DE VALIDACIÓN:{RESET} Abre {GREEN}http://localhost:4040{RESET} y ve a la pestaña {YELLOW}'Storage'{RESET}.")
print("   Verás el DataFrame almacenado en memoria de manera visual.")

input("\nPresiona ENTER en esta terminal para continuar y cerrar el script...")
spark.stop()
print("="*60 + "\n")

```

---

## Tarea 4. Análisis con Catalyst Optimizer

**Catalyst Optimizer** planifica y simplifica los filtros automáticamente para que el procesador no trabaje de más.

### Paso 4. Inspeccionar planes lógicos y físicos

Crea el archivo `3_catalyst_optimizer.py` in VS Code y ejecútalo (**F5**):

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CatalystApp") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

df = spark.read.csv("data/data.csv", header=True, inferSchema=True)

# Transformaciones encadenadas
df_filtrado = df.filter(df["edad"] > 30)
df_agrupado = df_filtrado.groupBy("ciudad").count()

print("\n" + "="*60)
print(f"{BOLD}{YELLOW}  TAREA 4: INSPECCIÓN DE PLANES OPTIMIZADOS (CATALYST OPTIMIZER){RESET}")
print("="*60)
print(f"{BOLD} Despliegue del Plan de Ejecución Físico (.explain):{RESET}\n")

# .explain() muestra las optimizaciones (como el Filter Pushdown) aplicadas por Catalyst
df_agrupado.explain()

print("-" * 60)
print(f"{BOLD} Resultados Consolidados Calculados:{RESET}")
df_agrupado.show(10, truncate=False)
print("="*60 + "\n")

spark.stop()

```

---

## Tarea 5. Laboratorio Integrador: Optimización de Cruzados y Escritura Local

En este ejercicio final combinaremos un **Broadcast Join** para evitar transferencias de datos (*Shuffle*) pesadas y utilizaremos la carpeta `salidas/` guardando el resultado final.

### Paso 5. Ejecutar el flujo completo optimizado

Crea el script final llamado `4_optimizacion_avanzada.py` en VS Code:

```python
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

```

---

## Notas Metodológicas

* **Uso Efectivo de Carpetas Locales:** La carpeta `salidas/` cumple un rol crítico de rendimiento. Al escribir con `.write.parquet()`, Spark crea un directorio estructurado que optimiza el almacenamiento en columnas y genera archivos de metadatos independientes, una práctica fundamental en la arquitectura moderna de Big Data.
* **Confirmación visual en SQL/DataFrames:** Al inspeccionar un trabajo de escritura en la Spark UI, la interfaz genera un plano detallado indicando cuántas filas se procesaron y el formato de archivo final, permitiéndote auditar el rendimiento de tus flujos de datos sin salir de tu entorno local.

### Resultado esperado

![resultado](../curso_python_spark/images/lab9_resultado1.png)

---

![resultado](../curso_python_spark/images/lab9_resultado2.png)

