# PYT_SPARK_DEVESS

## Práctica 5. Uso de funciones de transformación

### Objetivo

Al finalizar la práctica, se espera que el estudiante sea capaz de:

* Entender y aplicar funciones de transformación sobre RDDs de manera local y eficiente en Visual Studio Code.
* Configurar esquemas estructurados y procesar formatos CSV y Parquet de forma local.

### Objetivo visual

Se espera que el estudiante observe de forma clara la relación entre la actividad propuesta y el resultado que debe obtener al ejecutar los pasos del laboratorio.
### Duración aproximada

* 60 minutos

### Prerrequisitos

* Haber completado el laboratorio 1.

---

## Instrucciones del Laboratorio

### Tarea 1. Crear un RDD con esquema a partir de archivos locales

#### Paso 1. Preparación de la estructura de datos local (Blindaje contra errores)

Antes de ejecutar los scripts en VS Code, abre la terminal integrada y ejecuta los siguientes comandos Bash. Esto creará la carpeta contenedora y generará tanto el archivo CSV como el archivo Parquet con un mínimo de 10 registros iniciales de prueba para garantizar una ejecución local fluida y sin errores de rutas:

```bash
# 1. Crear el directorio de datos locales
mkdir -p data/Model

# 2. Generar el archivo CSV de clientes (10 registros con estructura completa)
echo "ID,Name,Address,Gender,Status" > data/Model/Customers.csv
echo "1,Alicia,Calle 123,F,Activo" >> data/Model/Customers.csv
echo "2,Bernardo,Avenida 45,M,Inactivo" >> data/Model/Customers.csv
echo "3,Carla,Pasaje Central,F,Activo" >> data/Model/Customers.csv
echo "4,Daniel,Jardin Alto,M,Activo" >> data/Model/Customers.csv
echo "5,Elena,Alameda Norte,F,Inactivo" >> data/Model/Customers.csv
echo "6,Fernando,Camino Viejo,M,Activo" >> data/Model/Customers.csv
echo "7,Gabriela,Plaza Mayor,F,Activo" >> data/Model/Customers.csv
echo "8,Hugo,Esquina Sur,M,Inactivo" >> data/Model/Customers.csv
echo "9,Isabel,Bulevar Este,F,Activo" >> data/Model/Customers.csv
echo "10,Jorge,Valle Verde,M,Activo" >> data/Model/Customers.csv

# 3. Generar el archivo Parquet de prueba con 10 propiedades inmobiliarias
cat << 'EOF' > generar_parquet_inicial.py
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark = SparkSession.builder.master("local[*]").appName("GenerarParquetInicial").getOrCreate()

esquema = StructType([
    StructField("Precio", LongType(), True), StructField("Area", LongType(), True),
    StructField("Recamaras", IntegerType(), True), StructField("Baños", IntegerType(), True),
    StructField("Historias", LongType(), True), StructField("CallePrincipal", StringType(), True),
    StructField("CuartoInvitados", StringType(), True), StructField("Sotano", StringType(), True),
    StructField("AguaCaliente", StringType(), True), StructField("AireAcondicionado", StringType(), True),
    StructField("Estacionamiento", IntegerType(), True), StructField("AreaPreferida", StringType(), True),
    StructField("EstadoMobiliario", StringType(), True)
])

datos = [
    (350000, 120, 3, 2, 2, "Si", "No", "Si", "Si", "Si", 2, "Si", "Amueblado"),
    (120000, 80, 2, 1, 1, "Si", "No", "No", "No", "No", 1, "No", "Sin_Amueblar"),
    (310000, 150, 4, 2, 2, "Si", "Si", "No", "Si", "Si", 2, "Si", "Semi_Amueblado"),
    (95000, 65, 1, 1, 1, "No", "No", "No", "No", "No", 0, "No", "Sin_Amueblar"),
    (550000, 220, 4, 3, 3, "Si", "Si", "Si", "Si", "Si", 3, "Si", "Amueblado"),
    (420000, 180, 3, 2, 1, "Si", "No", "Si", "No", "Si", 2, "No", "Amueblado"),
    (85000, 70, 2, 1, 1, "No", "No", "No", "No", "No", 1, "No", "Sin_Amueblar"),
    (280000, 140, 3, 2, 2, "Si", "No", "No", "Si", "Si", 2, "Si", "Semi_Amueblado"),
    (165000, 95, 2, 1, 1, "Si", "No", "No", "No", "No", 1, "No", "Sin_Amueblar"),
    (210000, 110, 3, 2, 2, "Si", "No", "Si", "No", "Si", 1, "No", "Semi_Amueblado")
]
df = spark.createDataFrame(datos, schema=esquema)
df.write.mode("overwrite").parquet("data/house-price.parquet")
print("Archivo Parquet local generado con éxito en 'data/house-price.parquet'")
EOF

python3 generar_parquet_inicial.py
rm generar_parquet_inicial.py

```

#### Paso 2. RDD con esquema desde CSV

Crea un archivo llamado `01_csv_esquema.py` en tu espacio de trabajo de VS Code e introduce el código adaptado. Nota cómo se omiten los encabezados y se mapean las líneas divididas mediante `.split(",")` para que se ajusten al esquema de conversión de DataFrame:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("SalvarDataFrames").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "data", "Sales.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "salidas", "reporte_completo")

if not os.path.exists(DATA_FILE):
    raise FileNotFoundError(f"El archivo de ventas no existe: {DATA_FILE}. Genera data/Sales.csv primero.")

# Cargar y preparar datos
df = spark.read.csv(DATA_FILE, inferSchema=True, header=True)
df_productos = df.select(
    col("SalesOrderNumber").alias("Order"),
    col("Product").alias("Producto"),
    col("Quantity").alias("Cantidad"),
    col("Sales").alias("Importe")
).withColumns({
    "Total": col("Cantidad") * col("Importe"),
    "Tax": col("Importe") * 0.16
})

print("\n" + "="*60)
print(f"{BOLD}{CYAN} REDUCIENDO PARTICIONES DEL CLÚSTER A 1 ARCHIVO CON coalesce(1){RESET}")
print("="*60)

df_consolidado = df_productos.coalesce(1)

# Guardar en formato CSV local
df_consolidado.write.csv(OUTPUT_DIR, header=True, mode="overwrite")
print(f"   {GREEN}[OK]{RESET} Partición guardada en: {YELLOW}{OUTPUT_DIR}/{RESET}")

spark.stop()

print("-" * 60)
print(f"{BOLD} RENOMBRANDO PARTICIÓN BINARIA A NOMBRE PLANO ESTÁNDAR{RESET}")
print("-" * 60)
if os.path.exists(OUTPUT_DIR):
    archivos = os.listdir(OUTPUT_DIR)
    archivo_csv_origen = [f for f in archivos if f.endswith(".csv") and f.startswith("part-")][0]

    ruta_origen_completa = os.path.join(OUTPUT_DIR, archivo_csv_origen)
    ruta_destino_final = os.path.join(OUTPUT_DIR, "reporte_final.csv")

    os.rename(ruta_origen_completa, ruta_destino_final)
    print(f"   {GREEN}[COMPLETO]{RESET} Archivo final listo en: {YELLOW}{ruta_destino_final}{RESET}")
print("="*60 + "\n")
```

#### Paso 3. Leer desde un archivo Parquet local con esquema

Crea un archivo llamado `02_parquet_rdd.py` en VS Code para procesar el origen Parquet y mapearlo de vuelta a una estructura RDD:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("RDDdesdeParquet") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

GREEN = "\033[92m"
BOLD = "\033[1m"
RESET = "\033[0m"

esquema = StructType([
    StructField("Precio", LongType(), True),
    StructField("Area", LongType(), True),
    StructField("Recamaras", IntegerType(), True),
    StructField("Baños", IntegerType(), True),
    StructField("Historias", LongType(), True),
    StructField("CallePrincipal", StringType(), True),
    StructField("CuartoInvitados", StringType(), True),
    StructField("Sotano", StringType(), True),
    StructField("AguaCaliente", StringType(), True),
    StructField("AireAcondicionado", StringType(), True),
    StructField("Estacionamiento", IntegerType(), True),
    StructField("AreaPreferida", StringType(), True),
    StructField("EstadoMobiliario", StringType(), True)
])

# Carga del archivo utilizando la ruta relativa local
df = spark.read.parquet("data/house-price.parquet")

# Convertir el DataFrame a RDD
rdd = df.rdd

# Convertir el RDD de vuelta a un DataFrame con el esquema explícito y extraer su RDD
rdd_esquema = rdd.toDF(esquema).rdd

# Mostrar resultados ordenados de filas e info
print("\n" + "="*60)
print(f"{BOLD}{GREEN} TAREA 1: PROCESAMIENTO RDD DESDE ARCHIVO PARQUET{RESET}")
print("="*60)
print(f"{BOLD} Registros procesados por renglón de RDD:{RESET}")
for row in rdd_esquema.collect():
    print(f"   {row}")

print("-" * 60)
print(f"{BOLD} Estructura y Esquema en DataFrame Base:{RESET}")
df.show(10, 0)
df.printSchema()
print("="*60 + "\n")

spark.stop()

```

>  **Recomendación:** Utilizar DataFrames cuando sea posible. Si bien es factible crear RDDs con esquemas desde archivos Parquet, se recomienda utilizar DataFrames directamente, ya que ofrecen un mejor rendimiento y funcionalidades optimizadas para el procesamiento de datos estructurados.

---

### Tarea 2. Conteo de palabras utilizando transformaciones perezosas

Crea el archivo `03_conteo_palabras.py`. En este script procesaremos un conjunto de frases reales para contar la frecuencia de cada palabra aplicando el concepto de *Lazy Evaluation*:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ConteoPalabras").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Crear un RDD a partir de 10 líneas de registros y logs simulados
frases = [
    "Error: usuario magarcia2 no existe", 
    "Error: usuario magarcia2 llego al limite de accesos en dragonlair", 
    "Aviso: servidor dragonlair degradado",
    "Aviso: almacenamiento de datos lleno",
    "Error: sesion local expirada en cluster",
    "Info: inicializando carga distribuida de datos",
    "Error: falla de conexion local en puerto 8080",
    "Info: procesamiento diferido activo con exito",
    "Aviso: memoria del sistema al limite de capacidad",
    "Error: usuario admin acceso denegado a base"
]
rdd = sc.parallelize(frases)

# Dividir frases en palabras utilizando flatMap
rdd_palabras = rdd.flatMap(lambda x: x.split(" "))

# Convertir todas las palabras a mayúsculas usando map
rdd_palabras_mayusculas = rdd_palabras.map(lambda x: x.upper())

# Contar frecuencia mapeando a tuplas (K, V) y reduciendo por clave
rdd_frecuencia = rdd_palabras_mayusculas.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)

# Ejecutar la acción final (collect) para materializar y obtener resultados en el driver
resultado = rdd_frecuencia.collect()

print("\n" + "="*60)
print(f"{BOLD}{CYAN} TAREA 2: RESULTADO DE CONTEO DE PALABRAS (LAZY EVALUATION){RESET}")
print("="*60)
for token, count in resultado:
    print(f"   {token:<15} ->  Cantidad: {count}")
print("="*60 + "\n")

spark.stop()

```

---

### Tarea 3. Aplicando transformaciones comunes

#### 1. Función `map`

La función `map` toma una función como argumento y la aplica a cada elemento del RDD, devolviendo un nuevo RDD con los resultados estructurados uno a uno.

Crea el archivo `04_funcion_map.py` extendido a un mínimo de 10 ejemplos por variante:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("FuncionMap").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

print("\n" + "="*60)
print(f"{BOLD}{CYAN} VARIANTE A: CONVERTIR ELEMENTOS A MAYÚSCULAS (10 PALABRAS){RESET}")
print("="*60)
rdd_cadenas = sc.parallelize(["hola", "mundo", "pyspark", "spark", "rdd", "python", "data", "clúster", "memoria", "esquema"])
rdd_mayusculas = rdd_cadenas.map(lambda x: x.upper())
print(f"   {rdd_mayusculas.collect()}")

print("\n" + "="*60)
print(f"{BOLD}{GREEN} VARIANTE B: TRANSFORMAR TUPLAS - INCREMENTAR EDAD EN 2 (10 USUARIOS){RESET}")
print("="*60)
rdd_tuplas = sc.parallelize([
    ("Alicia", 25), ("Bernardo", 30), ("Carolina", 28), ("Daniel", 22), ("Elena", 35),
    ("Fernando", 19), ("Gabriela", 31), ("Hugo", 45), ("Isabel", 24), ("Jorge", 27)
])
rdd_tuplas_transformadas = rdd_tuplas.map(lambda x: (x[0], x[1] + 2))
for item in rdd_tuplas_transformadas.collect():
    print(f"  • {item[0]:<10} -> Nueva Edad: {item[1]}")

print("\n" + "="*60)
print(f"{BOLD}{YELLOW} VARIANTE C: TRANSFORMACIONES COMPLEJAS - PARSEO NOMBRE:EDAD (10 STRINGS){RESET}")
print("="*60)
rdd_formato = sc.parallelize([
    "Alice:25", "Bob:30", "Cathy:28", "David:22", "Eva:35",
    "Frank:19", "Grace:31", "Harry:45", "Iris:24", "Jack:27"
])
rdd_parseado = rdd_formato.map(lambda x: (x.split(":")[0], int(x.split(":")[1])))
for item in rdd_parseado.collect():
    print(f"   Registro Parseado: Clave={item[0]:<8} | Valor (Int)={item[1]}")
print("="*60 + "\n")

spark.stop()
```

#### 2. Función `flatMap`

A diferencia de `map`, `flatMap` aplica la función sobre cada elemento pero "aplana" cualquier colección o lista resultante en elementos individuales independientes en el RDD de salida.

Crea el archivo `05_funcion_flatmap.py` con 10 registros de log simulados para contrastar estructuralmente ambos métodos:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("FuncionFlatMap").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

CYAN = "\033[96m"
GREEN = "\033[92m"
BOLD = "\033[1m"
RESET = "\033[0m"

frases = [
    "Error de aplicacion web", "Aviso de recursos limite", "Error de seguridad local",
    "Info de carga completa", "Falla de puerto cerrado", "Aviso de red inestable",
    "Error de usuario invalido", "Info de sesion iniciada", "Falla de lectura disco",
    "Aviso de base demorada"
]
rdd = sc.parallelize(frases)

palabras_map = rdd.map(lambda frase: frase.split())
palabras_flat = rdd.flatMap(lambda frase: frase.split())

print("\n" + "="*60)
print(f"{BOLD}{CYAN} RESULTADO CON MAP (MANTIENE LISTA DE LISTAS ANIDADAS){RESET}")
print("="*60)
for lista in palabras_map.collect():
    print(f"    {lista}")

print("\n" + "="*60)
print(f"{BOLD}{GREEN} RESULTADO CON FLATMAP (ELEMENTOS APLANADOS EN UNA SOLA DIMENSIÓN){RESET}")
print("="*60)
coleccion_plana = palabras_flat.collect()
print(f"   Total tokens: {len(coleccion_plana)}")
print(f"   Lista Completa: {coleccion_plana}")
print("="*60 + "\n")

spark.stop()
```

#### 3. Función `filter`

Se utiliza para seleccionar únicamente los elementos que cumplen con una condición específica (donde la evaluación resulta en `True`). Sintaxis: `nuevo_rdd = rdd.filter(función)`.

Crea el archivo `06_funcion_filter.py` para probar todas las variantes funcionales ampliadas a 10 ejemplos de datos:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("FuncionFilter").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

print("\n" + "="*60)
print(f"{BOLD}{CYAN} VARIANTE A: FILTRAR NÚMEROS PARES (10 ELEMENTOS){RESET}")
print("="*60)
rdd_num = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
rdd_pares = rdd_num.filter(lambda x: x % 2 == 0)
print(f"   Pares detectados: {rdd_pares.collect()}")

rdd_personas = sc.parallelize([
    ("Alice", 25), ("Bob", 30), ("Cathy", 28), ("David", 22), ("Eva", 35),
    ("Frank", 19), ("Grace", 31), ("Harry", 45), ("Iris", 24), ("Jack", 27)
])

print("\n" + "="*60)
print(f"{BOLD}{GREEN} VARIANTE B Y C: FILTRAR TUPLAS CON LAMBDA Y DEFS (EDAD > 25){RESET}")
print("="*60)
def es_mayor_de_25(persona):
    nombre, edad = persona
    return edad > 25

rdd_mayores = rdd_personas.filter(es_mayor_de_25)
for p in rdd_mayores.collect():
    print(f"   Mayor de 25: {p[0]:<10} | Edad: {p[1]}")

print("\n" + "="*60)
print(f"{BOLD}{YELLOW} VARIANTE D: FILTRADO COMPLEJO CON MÚLTIPLES CRITERIOS (BOGOTÁ Y > 25){RESET}")
print("="*60)
rdd_ciudades = sc.parallelize([
    ("Alice", 25, "Medellin"), ("Bob", 30, "Bogotá"), ("Cathy", 28, "Bogotá"),
    ("David", 22, "Cali"), ("Ernesto", 21, "Bogotá"), ("Fernanda", 20, "Medellin"),
    ("Guillermo", 32, "Bogotá"), ("Hilda", 29, "Cali"), ("Ivan", 26, "Bogotá"), ("Julia", 34, "Medellin")
])
rdd_filtrado_complejo = rdd_ciudades.filter(lambda x: x[2] == "Bogotá" and x[1] > 25)
for item in rdd_filtrado_complejo.collect():
    print(f"   Match -> Nombre: {item[0]:<10} | Edad: {item[1]} | Ciudad: {item[2]}")
print("="*60 + "\n")

spark.stop()
```

#### 4. Función `distinct`

Devuelve un nuevo RDD eliminando los registros duplicados a través de una reorganización interna de particiones (*shuffle*).

Crea el archivo `07_funcion_distinct.py`. Este archivo incluye el flujo avanzado para remover duplicados basándose en una sola columna específica utilizando transformaciones de unión (`join`), con listas de 10 elementos:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("FuncionDistinct").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

CYAN = "\033[96m"
GREEN = "\033[92m"
BOLD = "\033[1m"
RESET = "\033[0m"

print("\n" + "="*60)
print(f"{BOLD}{CYAN} VARIANTE A Y B: ELEMENTOS Y TUPLAS ÚNICAS DIRECTAS{RESET}")
print("="*60)
rdd_duplicado = sc.parallelize([1, 2, 3, 4, 2, 3, 5, 6, 1, 7, 8, 5, 2, 9, 10])
print(f"   Colección Única Simple: {rdd_duplicado.distinct().collect()}")

rdd_tuplas_dup = sc.parallelize([
    ("Alice", 25), ("Bob", 30), ("Alice", 25), ("Cathy", 28), ("Bob", 30),
    ("David", 22), ("Eva", 35), ("David", 22), ("Frank", 19), ("Alice", 25)
])
print(f"   Tuplas Completas Únicas: {rdd_tuplas_dup.distinct().collect()}")

print("\n" + "="*60)
print(f"{BOLD}{GREEN} VARIANTE C: FLUJO AVANZADO - ELIMINAR DUPLICADOS POR UNA COLUMNA{RESET}")
print("="*60)
rdd_base = sc.parallelize([
    ("Alice", 25), ("Bob", 30), ("Alice", 35), ("Cathy", 28), ("Bob", 40),
    ("David", 22), ("Eva", 35), ("David", 48), ("Frank", 19), ("Alice", 50)
])

# 1. Seleccionar la columna "nombre" (índice 0) y aplicar distinct
nombres_unicos = rdd_base.map(lambda x: x[0]).distinct()
print(f"   Nombres únicos extraídos: {nombres_unicos.collect()}")

# 2. Utilizar un join para cruzar y mantener solo un registro por nombre único
rdd_filtrado = rdd_base.map(lambda x: (x[0], x)).join(nombres_unicos.map(lambda x: (x, 1))).map(lambda x: x[1][0])
print(f"   RDD filtrado final (Primer cruce):")
for res in rdd_filtrado.collect():
    print(f"      Manteniendo registro -> {res}")
print("="*60 + "\n")

spark.stop()
```

#### 5. Función `union`, `intersection`, `subtract` y `cartesian`

Operaciones aplicadas entre dos RDDs cuyos elementos deben ser preferentemente del mismo tipo. Crea el archivo `08_operaciones_multi_rdd.py` cargado con las colecciones completas de los 10 elementos creados inicialmente.

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("OperacionesMultiRDD").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Dos RDDs bases con 10 elementos cada uno intercalando intersecciones
rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
rdd2 = sc.parallelize([7, 8, 9, 10, 11, 12, 13, 14, 15, 16])

print("\n" + "="*60)
print(f"{BOLD}{CYAN} TAREA 3: OPERACIONES ÁLGEBRICAS ENTRE MULTI-RDDS{RESET}")
print("="*60)
print(f"   Union (Agregados totales con duplicados): \n     -> {rdd1.union(rdd2).collect()}\n")
print(f"   Intersection (Elementos compartidos en red): \n     -> {rdd1.intersection(rdd2).collect()}\n")
print(f"   Subtract (Elementos exclusivos del RDD1): \n     -> {rdd1.subtract(rdd2).collect()}\n")

# Producto Cartesiano
rdd_letras = sc.parallelize(["A", "B"])
print(f"   Cartesian (Muestra de combinaciones RDD1 con Letras): \n     -> {rdd1.cartesian(rdd_letras).take(10)}... [Muestra]")
print("="*60 + "\n")

spark.stop()
```

#### 6. Agregaciones Clave-Valor (`groupByKey`, `reduceByKey`, `sortByKey`)

Transformaciones específicas para RDDs estructurados en pares `(Clave, Valor)`. Crea el archivo `09_agregaciones_clave_valor.py` alimentado con las 10 transacciones creadas incialmente:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("AgregacionesClaveValor").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

print("\n" + "="*60)
print(f"{BOLD}{CYAN} 1. APLICACIÓN DE groupByKey (10 MATRÍCULAS DE ALUMNOS){RESET}")
print("="*60)
data_estudiantes = [
    ("Juan", "Matemáticas", 8), ("María", "Ciencias", 9), ("Juan", "Física", 7), 
    ("Pedro", "Matemáticas", 6), ("María", "Historia", 10), ("Pedro", "Física", 8),
    ("Ana", "Ciencias", 9), ("Ana", "Física", 10), ("Juan", "Historia", 9), ("Pedro", "Ciencias", 7)
]
rdd_estudiantes = sc.parallelize(data_estudiantes)
grouped_rdd = rdd_estudiantes.map(lambda x: (x[0], (x[1], x[2]))).groupByKey()

for student, grades in grouped_rdd.collect():
    print(f"   Estudiante: {student:<8} -> Cursos tomados: {list(grades)}")

print("\n" + "="*60)
print(f"{BOLD}{GREEN} 2. APLICACIÓN DE reduceByKey (CONSOLIDACIÓN DE 10 VENTAS AGREGADAS){RESET}")
print("="*60)
data_ventas = [
    ("ProductoA", 10), ("ProductoB", 5), ("ProductoA", 15), ("ProductoC", 8), ("ProductoB", 12),
    ("ProductoC", 22), ("ProductoA", 5), ("ProductoD", 30), ("ProductoD", 15), ("ProductoB", 7)
]
rdd_ventas = sc.parallelize(data_ventas)
total_ventas = rdd_ventas.reduceByKey(lambda a, b: a + b)
for item in total_ventas.collect():
    print(f"   {item[0]:<12} -> Total Acumulado: {item[1]}")

print("\n" + "="*60)
print(f"{BOLD}{YELLOW} 3. APLICACIÓN DE sortByKey (ORDEN ASCENDENTE VS DESCENDENTE){RESET}")
print("="*60)
print(f"   Orden Ascendente:  {total_ventas.sortByKey(ascending=True).collect()}")
print(f"   Orden Descendente: {total_ventas.sortByKey(ascending=False).collect()}")
print("="*60 + "\n")

spark.stop()
```

---

### Tarea 4. Filtrado de columnas y conversión de tipos

#### Paso 4. Preparación del Archivo Local de Ventas

Ejecuta la siguiente instrucción en tu terminal integrada para generar el archivo `Sales.csv` enriquecido con exactamente 10 registros válidos antes de procesar las selecciones de campos:

```bash
mkdir -p data
echo "SalesOrderNumber,OrderDate,Customer,Country,TotalCost" > data/Sales.csv
echo "SO43659,2026-07-01,Gustavo,United States,1000" >> data/Sales.csv
echo "SO43660,2026-07-02,Catherine,Australia,2500" >> data/Sales.csv
echo "SO43661,2026-07-03,Fernando,Canada,1200" >> data/Sales.csv
echo "SO43662,2026-07-04,Alicia,France,3100" >> data/Sales.csv
echo "SO43663,2026-07-05,Bernardo,Germany,1500" >> data/Sales.csv
echo "SO43664,2026-07-06,Gabriela,Canada,1800" >> data/Sales.csv
echo "SO43665,2026-07-07,Hugo,France,900" >> data/Sales.csv
echo "SO43666,2026-07-08,Isabel,Germany,2200" >> data/Sales.csv
echo "SO43667,2026-07-09,Jorge,Australia,1300" >> data/Sales.csv
echo "SO43668,2026-07-10,Elena,United States,4500" >> data/Sales.csv
```

#### Paso 5. Ejecución del script de Proyección y Conversión

Crea el archivo `10_filtrado_y_tipos.py`. Este script recopila tanto el enfoque de DataFrames usando `.select()` como el método puro de RDD usando `.map()` con índices para aislar las columnas deseadas y realizar los casteos a tipos numéricos con 10 registros para cada flujo de evaluación:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("FiltradoYTipos").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "data", "Sales.csv")

if not os.path.exists(DATA_FILE):
    raise FileNotFoundError(f"No existe el archivo: {DATA_FILE}")

print("\n" + "="*60)
print(f"{BOLD}{CYAN} ENFOQUE A: SELECCIÓN DE CAMPOS EXPLICITOS VÍA DATAFRAME SELECT{RESET}")
print("="*60)
df = spark.read.csv(DATA_FILE, header=True, inferSchema=True)
campos_deseados = ["SalesOrderNumber", "OrderDate", "Customer", "Country", "TotalCost"]
rdd_desde_select = df.select(*campos_deseados).rdd

for row in rdd_desde_select.collect():
    print(f"   Row Proyectada: {row}")

print("\n" + "="*60)
print(f"{BOLD}{GREEN} ENFOQUE B: SELECCIÓN DE ÍNDICES EN RDD PURO USANDO MAP (10 FILAS){RESET}")
print("="*60)
data_rdd_puro = [
    ("A", 1, "X", True), ("B", 2, "Y", False), ("C", 3, "Z", True), ("D", 4, "W", False),
    ("E", 5, "V", True), ("F", 6, "U", False), ("G", 7, "T", True), ("H", 8, "S", False),
    ("I", 9, "R", True), ("J", 10, "Q", False)
]
rdd_original = sc.parallelize(data_rdd_puro)

indices_deseados = [0, 2]
rdd_mapeado = rdd_original.map(lambda row: tuple(row[i] for i in indices_deseados))
print(f"   Matrices indexadas resultantes:\n     {rdd_mapeado.collect()}")

print("\n" + "="*60)
print(f"{BOLD}{YELLOW} ENFOQUE C: CONVERSIÓN EXPLÍCITA DE TIPOS DE DATOS (STRING A INT){RESET}")
print("="*60)
rdd_datos_string = sc.parallelize([
    ("Alejandro", "25"), ("Beatriz", "30"), ("Carmen", "28"), ("Daniel", "22"), ("Elena", "35"),
    ("Fernando", "19"), ("Gabriela", "31"), ("Hugo", "45"), ("Isabel", "24"), ("Jorge", "27")
])
rdd_convertido = rdd_datos_string.map(lambda x: (x[0], int(x[1])))
for registro in rdd_convertido.collect():
    print(f"   Registro Convertido Tipo -> Nombre: {registro[0]:<10} | Edad (Int Value): {registro[1]}")
print("="*60 + "\n")

spark.stop()
```

### Resultado esperado


![resultado](../curso_python_spark/images/lab5_resultado.png)
