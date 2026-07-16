# PYT_SPARK_DEVESS

## Práctica 3. DataFrames en PySpark

### Objetivo

Al finalizar la práctica, se espera que el estudiante sea capaz de crear, manipular, transformar y exportar DataFrames en PySpark a partir de colecciones en memoria y archivos estructurados (CSV, JSON, Parquet) usando Visual Studio Code.


### Objetivo visual

Se espera que el estudiante observe de forma clara la relación entre la actividad propuesta y el resultado que debe obtener al ejecutar los pasos del laboratorio.
### Duración aproximada:

* 45 minutos.

### Prerrequisitos

* Haber completado las prácticas 1 y 2.
* Contar con las librerías `pyspark` y `findspark` instaladas.

---


### Instrucciones

Se describen los pasos requeridos para completar la práctica de forma ordenada y coherente.

## Tarea 1. Creando DataFrames

Los DataFrames son estructuras de datos distribuidas y optimizadas con tipos de datos asignados por columna, ofreciendo un rendimiento muy superior al de los RDDs tradicionales.

#### Paso 1. Preparación de los archivos de datos de prueba

Antes de ejecutar los códigos de lectura, ejecuta los siguientes comandos en tu terminal integrada de VS Code (`~/1python/netec`) para poblar la carpeta local `data/` con archivos CSV, JSON y Parquet de prueba con los siguientes 10 registros cada uno:

```bash
mkdir -p data/Model/

# 1. Crear Sales.csv (10 registros)
echo -e "SalesOrderNumber,Product,Quantity,Sales\nSO43659,Mountain Bike,1,3399.99\nSO43660,Helmet,3,34.99\nSO43661,Socks,2,9.50\nSO43662,Laptop,1,1200.00\nSO43663,Teclado,5,45.00\nSO43664,Monitor,2,250.00\nSO43665,Mouse,10,15.50\nSO43666,Impresora,1,180.00\nSO43667,Audifonos,4,60.00\nSO43668,Disco_Duro,3,85.00" > data/Sales.csv

# 2. Crear Products.csv (10 registros)
echo -e "Product,Cost,Price\nMountain Bike,1800.00,3399.99\nHelmet,12.00,34.99\nSocks,3.00,9.50\nLaptop,750.00,1200.00\nTeclado,15.00,45.00\nMonitor,110.00,250.00\nMouse,5.00,15.50\nImpresora,90.00,180.00\nAudifonos,20.00,60.00\nDisco_Duro,40.00,85.00" > data/Model/Products.csv

# 3. Crear users.json (10 registros - Arreglo Multilínea)
echo -e '[\n  {"Nombre": "Alfonso", "Edad": 25, "Ciudad": "Abejorral"},\n  {"Nombre": "Bernardo", "Edad": 30, "Ciudad": "Bogota"},\n  {"Nombre": "Celeste", "Edad": 35, "Ciudad": "Cartagena"},\n  {"Nombre": "Daniel", "Edad": 28, "Ciudad": "Medellin"},\n  {"Nombre": "Elena", "Edad": 42, "Ciudad": "Cali"},\n  {"Nombre": "Fernando", "Edad": 19, "Ciudad": "Manizales"},\n  {"Nombre": "Gabriela", "Edad": 31, "Ciudad": "Pereira"},\n  {"Nombre": "Hugo", "Edad": 50, "Ciudad": "Bucaramanga"},\n  {"Nombre": "Isabel", "Edad": 24, "Ciudad": "Santa Marta"},\n  {"Nombre": "Jorge", "Edad": 37, "Ciudad": "Ibague"}\n]' > data/users.json

# 4. Crear un parquet base usando 10 registros inmobiliarios
python3 -c "import os; os.environ['JAVA_HOME']='/usr/lib/jvm/java-17-openjdk-amd64'; import findspark; findspark.init(); from pyspark.sql import SparkSession; s=SparkSession.builder.getOrCreate(); datos=[('Casa Centro', 250000), ('Dpto Sur', 120000), ('Casa Norte', 310000), ('Dpto Este', 95000), ('Penthouse', 550000), ('Local Comercial', 420000), ('Terreno Campo', 85000), ('Duplex Playa', 280000), ('Oficina Inversion', 165000), ('Chalet Bosque', 210000)]; s.createDataFrame(datos, ['Propiedad', 'Precio']).write.mode('overwrite').parquet('data/house-price.parquet'); s.stop()"
```

#### Paso 2. Crear DataFrame desde una Lista y un Diccionario

Crea el archivo `df_creacion_memoria.py`:

```python
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
```

#### Paso 3. Crear DataFrame desde fuentes de archivos externos (CSV, Parquet, JSON)

Crea el archivo `df_creacion_archivos.py`:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CrearDFArchivos").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

print("\n" + "="*60)
print(f"{BOLD}{CYAN} 1. LECTURA DE CSV CON INFERENCIA DE ESQUEMA Y CABECERA{RESET}")
print("="*60)
df_csv = spark.read.csv("data/Sales.csv", header=True, inferSchema=True)
df_csv.show(10, truncate=False)
df_csv.printSchema()

print("\n" + "="*60)
print(f"{BOLD}{GREEN} 2. LECTURA DE ARCHIVO OPTIMIZADO PARQUET{RESET}")
print("="*60)
df_parquet = spark.read.parquet("data/house-price.parquet")
df_parquet.show(10, truncate=False)
df_parquet.printSchema()

print("\n" + "="*60)
print(f"{BOLD}{YELLOW} 3. LECTURA DE ARCHIVO SEMIESTRUCTURADO JSON{RESET}")
print("="*60)
df_json = spark.read.json("data/users.json", multiLine=True)
df_json.show(10, truncate=False)
df_json.printSchema()
print("="*60 + "\n")

spark.stop()
```

---

## Tarea 2. Trabajando con DataFrames (Transformaciones)

#### Paso 4. Selección y renombrado de columnas utilizando `col()` y `alias()`

Crea el archivo `df_transformacion_columnas.py`:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("SeleccionColumnas").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

CYAN = "\033[96m"
GREEN = "\033[92m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Cargar el CSV de productos
df_productos = spark.read.csv("data/Model/Products.csv", inferSchema=True, header=True)

print("\n" + "="*60)
print(f"{BOLD}{CYAN} SELECCIÓN BÁSICA USANDO STRINGS (PRODUCTOS){RESET}")
print("="*60)
df_productos.select("Product", "Cost", "Price").show(10, truncate=False)

print("\n" + "="*60)
print(f"{BOLD}{GREEN} SELECCIÓN Y MAPEO AVANZADO USANDO OBJETOS col() Y alias(){RESET}")
print("="*60)
df_sales = spark.read.csv("data/Sales.csv", inferSchema=True, header=True)

df_mapeado = df_sales.select(
    col("SalesOrderNumber").alias("Order"),
    col("Product").alias("Producto"),
    col("Quantity").alias("Cantidad"),
    col("Sales").alias("Importe")
)
df_mapeado.show(10, truncate=False)
print("="*60 + "\n")

spark.stop()

```

#### Paso 5. Agregar columnas constantes (`lit`), calculadas (`expr`) y múltiples (`withColumns`)

Crea el archivo `df_columnas_calculadas.py`:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr

spark = SparkSession.builder.appName("CalculoColumnas").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Cargar JSON y agregar columna fija literal
df_json = spark.read.json("data/users.json", multiLine=True)
df_empleados = df_json.withColumn("Status", lit("Active"))

print("\n" + "="*60)
print(f"{BOLD}{CYAN} COLUMNA ESTÁTICA AGREGADA CON lit(){RESET}")
print("="*60)
df_empleados.show(10, truncate=False)

# Cargar Sales y generar cálculos
df_sales = spark.read.csv("data/Sales.csv", inferSchema=True, header=True)

print("\n" + "="*60)
print(f"{BOLD}{GREEN} COLUMNA CALCULADA EN LÍNEA USANDO expr() (SUBTOTAL){RESET}")
print("="*60)
df_sales.select(
    col("SalesOrderNumber").alias("Order"),
    col("Product").alias("Producto"),
    expr("Sales * Quantity").alias("Subtotal")
).show(10, truncate=False)

print("\n" + "="*60)
print(f"{BOLD}{YELLOW} AGREGAR MÚLTIPLES COLUMNAS USANDO UN DICCIONARIO EN withColumns(){RESET}")
print("="*60)
df_resumen = df_sales.select(
    col("SalesOrderNumber").alias("Order"),
    col("Product").alias("Producto"),
    col("Quantity").alias("Cantidad"),
    col("Sales").alias("Importe")
)

df_final = df_resumen.withColumns({
    "Total": df_resumen.Cantidad * df_resumen.Importe,
    "Impuesto": df_resumen.Importe * 0.16
})
df_final.show(10, truncate=False)
print("="*60 + "\n")

spark.stop()
```

---

## Tarea 3. Salvar DataFrames y Coalesce (Reducción de particiones)

Cuando Spark guarda un archivo, escribe una partición por cada núcleo activo del clúster local. Para consolidar el resultado en un solo archivo físico en disco y renombrarlo utilizando Python, aplicamos `.coalesce(1)`.

#### Paso 6. Exportar y reestructurar salidas locales

Crea el archivo `df_exportar_coalesce.py`:

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

# Cargar y preparar datos
df = spark.read.csv("data/Sales.csv", inferSchema=True, header=True)
df_productos = df.select(
    col("SalesOrderNumber").alias("Order"),
    col("Product").alias("Producto"),
    col("Quantity").alias("Cantidad"),
    col("Sales").alias("Importe")
).withColumns({
    "Total": col("Quantity") * col("Sales"),
    "Tax": col("Sales") * 0.16
})

ruta_salida = "salidas/reporte_completo"

print("\n" + "="*60)
print(f"{BOLD}{CYAN} REDUCIENDO PARTICIONES DEL CLÚSTER A 1 ARCHIVO CON coalesce(1){RESET}")
print("="*60)

df_consolidado = df_productos.coalesce(1)

# Guardar en formato CSV local
df_consolidado.write.csv(ruta_salida, header=True, mode="overwrite")
print(f"   {GREEN}[OK]{RESET} Partición guardada en: {YELLOW}{ruta_salida}/{RESET}")

spark.stop()

# --- Bloque del Sistema Operativo para renombrar el archivo resultante ---
print("-" * 60)
print(f"{BOLD} RENOMBRANDO PARTICIÓN BINARIA A NOMBRE PLANO ESTÁNDAR{RESET}")
print("-" * 60)
if os.path.exists(ruta_salida):
    archivos = os.listdir(ruta_salida)
    # Filtrar el archivo generado por la partición de Spark
    archivo_csv_origen = [f for f in archivos if f.endswith(".csv") and f.startswith("part-")][0]
    
    ruta_origen_completa = os.path.join(ruta_salida, archivo_csv_origen)
    ruta_destino_final = os.path.join(ruta_salida, "reporte_final.csv")
    
    os.rename(ruta_origen_completa, ruta_destino_final)
    print(f"   {GREEN}[COMPLETO]{RESET} Archivo final listo en: {YELLOW}{ruta_destino_final}{RESET}")
print("="*60 + "\n")
```

### Resultado esperado

Se espera que el estudiante complete los pasos de la práctica y obtenga una salida coherente en la terminal o en los archivos generados, según corresponda al laboratorio.
