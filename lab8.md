Aquí tienes el manual de la **Práctica 8 completamente actualizado**. Se ha integrado un set de datos de **10 registros mínimos** reales y consistentes para cada archivo de origen, y se han reestructurado todos los scripts en Python añadiendo el silenciado de logs (`sc.setLogLevel("ERROR")`), la paleta de colores ANSI uniforme y los separadores de bloques para garantizar impresiones impecables y ordenadas en la terminal de VS Code.

---

## PYT_SPARK_DEVESS

### Práctica 8. Uso de agregaciones, agrupaciones y relaciones

#### Objetivos

Al finalizar la práctica, serás capaz de:

* Aplicar agregaciones, agrupaciones y relaciones de tablas mediante Spark SQL en un entorno de desarrollo local.

#### Duración aproximada

* 60 minutos.

#### Prerrequisitos

* **Entorno de Trabajo Local:** Tener instalado Visual Studio Code con la extensión de Python y Java 17 configurado en el sistema.

#### Contexto

Como parte de las consultas y análisis, vincular tablas de diferentes fuentes es una necesidad regular, así como obtener agregaciones para analizar la información. En PySpark, se pueden realizar agregaciones y agrupaciones utilizando tanto DataFrames como consultas SQL tradicionales sobre vistas temporales.

---

## Tarea 0. Preparación del Entorno y Creación de Datos Locales

Para que PySpark pueda procesar la información en tu entorno local, primero debemos crear físicamente la estructura de directorios y los archivos `.csv` con datos simulados.

1. Abre la terminal integrada de **Visual Studio Code** (asegúrate de estar situado en la ruta de tu proyecto, por ejemplo: `/home/kaims/1python/netec/lab8`).
2. Ejecuta los siguientes comandos en tu consola Bash para generar las carpetas y los archivos de datos automáticamente con un mínimo de 10 registros por set:

```bash
# 1. Crear la estructura de directorios necesaria
mkdir -p data/Model
mkdir -p data/TotalSales

# 2. Crear el archivo de ventas general (data/Sales.csv - 10 registros)
echo -e "Country,Territory,Sales,ProductKey,CustomerKey,SalesOrderNumber,OrderDate,UnitPrice,OrderQuantity\nMexico,Norte,1500,10,100,SO43659,2026-01-01,1500,1\nMexico,Sur,2500,20,200,SO43660,2026-01-02,1250,2\nCanada,Norte,3000,10,100,SO43661,2026-01-03,1500,2\nCanada,Este,1200,30,300,SO43662,2026-01-04,400,3\nUSA,Oeste,5000,20,400,SO43663,2026-01-05,2500,2\nMexico,Centro,1800,40,500,SO43664,2026-01-06,600,3\nUSA,Este,2400,10,600,SO43665,2026-01-07,1500,2\nCanada,Oeste,3500,20,700,SO43666,2026-01-08,1250,3\nMexico,Norte,1200,30,100,SO43667,2026-01-09,400,3\nUSA,Sur,4800,40,800,SO43668,2026-01-10,600,8" > data/Sales.csv

# 3. Crear el archivo de productos (data/Model/Products.csv - 10 registros)
echo -e "ProductKey,Category,Product,Price\n10,Electronica,Laptop,1500\n20,Electronica,Smartphone,1250\n30,Ropa,Camisa,400\n40,Ropa,Pantalon,600\n50,Hogar,Lampara,150\n60,Hogar,Silla,350\n70,Electronica,Tablet,800\n80,Ropa,Casaca,1200\n90,Hogar,Escritorio,950\n100,Electronica,Teclado,120" > data/Model/Products.csv

# 4. Crear el archivo de clientes (data/Model/Customers.csv - 10 registros)
echo -e "CustomerKey,Customer\n100,Alejandro Gomez\n200,Beatriz Ruiz\n300,Carlos Marin\n400,Diana Perez\n500,Eduardo Ponce\n600,Fernando Solis\n700,Gabriela Luna\n800,Hugo Chavez\n900,Isabel Diaz\n1000,Jorge Vega" > data/Model/Customers.csv

# 5. Crear los archivos históricos anuales en la subcarpeta TotalSales (10 archivos/registros acumulados)
echo -e "Year,TotalSales\n2016,390000" > data/TotalSales/Sales2016.csv
echo -e "Year,TotalSales\n2017,410000" > data/TotalSales/Sales2017.csv
echo -e "Year,TotalSales\n2018,450000" > data/TotalSales/Sales2018.csv
echo -e "Year,TotalSales\n2019,520000" > data/TotalSales/Sales2019.csv
echo -e "Year,TotalSales\n2020,610000" > data/TotalSales/Sales2020.csv
echo -e "Year,TotalSales\n2021,580000" > data/TotalSales/Sales2021.csv
echo -e "Year,TotalSales\n2022,640000" > data/TotalSales/Sales2022.csv
echo -e "Year,TotalSales\n2023,720000" > data/TotalSales/Sales2023.csv
echo -e "Year,TotalSales\n2024,810000" > data/TotalSales/Sales2024.csv
echo -e "Year,TotalSales\n2025,890000" > data/TotalSales/Sales2025.csv

```

---

## Tarea 1. Agregaciones y agrupaciones con SQL

### Paso 1. Agrupar DataFrames (Básico)

1. En VS Code, crea un archivo nuevo con el nombre `agrupacion_basica.py`.
2. Introduce el siguiente código estructurado con la paleta de estilos del laboratorio:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

# Inicialización unificada de SparkSession
spark = SparkSession.builder.appName("Usar SQL y DataFrames").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Resolución dinámica de la ruta absoluta local
ruta_sales = os.path.abspath("data/Sales.csv")

# Crear DataFrame utilizando el protocolo explicito file://
dfSales = spark.read.csv(f"file://{ruta_sales}", inferSchema=True, header=True)

# Registrar el DataFrame como una tabla temporal
dfSales.createOrReplaceTempView("ventas")

print("\n" + "="*60)
print(f"{BOLD}{CYAN}📊 PASO 1: METRICAS AGRUPADAS POR PAÍS DE ORIGEN (SUM){RESET}")
print("="*60)

query = (
    "SELECT Country, SUM(Sales) AS TotalSales "
    " FROM ventas "
    " GROUP BY Country "
)

spark.sql(query).show(10, truncate=False)
print("="*60 + "\n")

spark.stop()

```

### Paso 2. Agrupar por más de un campo

1. Crea un archivo nuevo en VS Code llamado `agrupacion_multiple.py`.
2. Introduce el código adaptado y ejecútalo.

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Usar SQL y DataFrames").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

GREEN = "\033[92m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Ruta absoluta dinámica
ruta_sales = os.path.abspath("data/Sales.csv")
dfSales = spark.read.csv(f"file://{ruta_sales}", inferSchema=True, header=True)

# Registrar la vista temporal
dfSales.createOrReplaceTempView("ventas")

print("\n" + "="*60)
print(f"{BOLD}{GREEN}🗺️  PASO 2: AGRUPACIÓN MÚLTIPLE (TERRITORIO Y PAÍS){RESET}")
print("="*60)

query = (
    "SELECT Territory, Country, SUM(Sales) AS TotalSales "
    " FROM ventas "
    " GROUP BY Territory, Country "
)

spark.sql(query).show(10, truncate=False)
print("="*60 + "\n")

spark.stop()

```

### Paso 3. Aplicar múltiples funciones de agregación en simultáneo

1. Crea un archivo nuevo en VS Code llamado `multiples_agregaciones.py`.
2. Pega el siguiente código que apunta al subdirectorio de productos y ejecútalo.

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Usar SQL y DataFrames").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Ruta absoluta dinámica apuntando a la subcarpeta de modelo
ruta_products = os.path.abspath("data/Model/Products.csv")
dfProducts = spark.read.csv(f"file://{ruta_products}", inferSchema=True, header=True)

# Registrar el DataFrame como una tabla temporal
dfProducts.createOrReplaceTempView("productos")

print("\n" + "="*60)
print(f"{BOLD}{YELLOW}🧮 PASO 3: FUNCIONES SIMULTÁNEAS POR CATEGORÍA DE PRODUCTO{RESET}")
print("="*60)

query = (
    "SELECT Category, SUM(Price) As TotalPrice, AVG(Price) as AveragePrice, "
    "MIN(Price) as MinPrice, MAX(Price) as MaxPrice"
    " FROM productos "
    " GROUP BY Category "
)

dfVentas = spark.sql(query)
dfVentas.show(10, truncate=False)
print("="*60 + "\n")

spark.stop()

```

### Paso 4. Filtrado de agrupaciones usando HAVING

1. Crea un archivo nuevo en VS Code llamado `filtrado_having.py`.
2. Inserta el fragmento de código corregido y ejecútalo.

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Usar SQL y DataFrames").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

ruta_products = os.path.abspath("data/Model/Products.csv")
dfProducts = spark.read.csv(f"file://{ruta_products}", inferSchema=True, header=True)
dfProducts.createOrReplaceTempView("productos")

print("\n" + "="*60)
print(f"{BOLD}{CYAN}🔍 PASO 4: FILTRADO POST-AGRUPACIÓN MEDIANTE CLÁUSULA HAVING{RESET}")
print("="*60)

# Consultar las categorías cuyo conteo grupal sea mayor o igual a 2 registros
query = (
    "SELECT Category, COUNT(Product) as NoProducts"
    " FROM productos "
    " GROUP BY Category "
    " HAVING COUNT(Product) >= 2"
)

dfProductsResult = spark.sql(query)
dfProductsResult.show(10, truncate=False)
print("="*60 + "\n")

spark.stop()

```

### Paso 5. Agregación global sin agrupación

1. Crea un archivo nuevo en VS Code llamado `agregacion_global.py`.
2. Ejecútalo para validar las métricas globales de tus productos.

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Usar SQL y DataFrames").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

GREEN = "\033[92m"
BOLD = "\033[1m"
RESET = "\033[0m"

ruta_products = os.path.abspath("data/Model/Products.csv")
dfProducts = spark.read.csv(f"file://{ruta_products}", inferSchema=True, header=True)
dfProducts.createOrReplaceTempView("productos")

print("\n" + "="*60)
print(f"{BOLD}{GREEN}📈 PASO 5: EVALUACIÓN Y AGREGACIÓN GLOBAL DE MÉTRICAS (SIN GROUP BY){RESET}")
print("="*60)

query = (
    "SELECT SUM(Price) As TotalSales, AVG(Price) as Average, MAX(Price) as MaxPrice,"
    " MIN(Price) as MinPrice, COUNT(Price) as NoProducts"
    " FROM productos "
)

dfProductsResult = spark.sql(query)
dfProductsResult.show(10, truncate=False)
print("="*60 + "\n")

spark.stop()

```

---

## Tarea 2. Manejando relaciones

Se puede utilizar SQL para trabajar con relaciones entre DataFrames utilizando operaciones como `JOIN` (cruces horizontales) y `UNION` (cruces verticales).

### Paso 1. Combinar múltiples tablas con JOIN masivo

1. Crea un archivo nuevo en VS Code llamado `relacion_tablas.py`.
2. Introduce el código y ejecútalo.

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Usar SQL y DataFrames").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Carga independiente resolviendo de forma absoluta y dinámica cada ruta local
ruta_p = os.path.abspath("data/Model/Products.csv")
dfProducts = spark.read.csv(f"file://{ruta_p}", inferSchema=True, header=True)
dfProducts.createOrReplaceTempView("productos")

ruta_c = os.path.abspath("data/Model/Customers.csv")
dfCustomers = spark.read.csv(f"file://{ruta_c}", inferSchema=True, header=True)
dfCustomers.createOrReplaceTempView("clientes")

ruta_s = os.path.abspath("data/Sales.csv")
dfSales = spark.read.csv(f"file://{ruta_s}", inferSchema=True, header=True)
dfSales.createOrReplaceTempView("ventas")

print("\n" + "="*60)
print(f"{BOLD}{CYAN}🛰️  PASO 1: RELACIÓN MULTI-TABLA (VENTAS, PRODUCTOS Y CLIENTES){RESET}")
print("="*60)

# Consulta relacional de las tres vistas temporales locales
query = (
    "SELECT v.SalesOrderNumber, c.Customer, v.OrderDate, p.Product, p.Category,"
    " v.UnitPrice, v.OrderQuantity "
    " FROM ventas v JOIN productos p on v.ProductKey = p.ProductKey"
    " JOIN clientes c on v.CustomerKey = c.CustomerKey"
)

dfResultado = spark.sql(query)
dfResultado.show(10, truncate=False)
print("="*60 + "\n")

spark.stop()

```

### Paso 2. Evaluación de comportamientos de cruce (Datos Inline)

Para enriquecer esta comparación a 10 ejemplos robustos por cada tipo de join, utilizaremos colecciones completas generadas directamente en memoria.

1. Crea un archivo en VS Code llamado `tipos_de_join.py`.
2. Ejecútalo con **Play (▶)** para comparar las salidas por consola.

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQL relaciones").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Colecciones inline de 10 elementos para simular desajustes y coincidencias
data_empleados = [
    (1, "Alejandra", 101), (2, "Berenice", 102), (3, "Carlos", 101), (4, "Daniela", 104), (5, "Ernesto", 110),
    (6, "Fabian", 102), (7, "Gabriela", 106), (8, "Hector", 115), (9, "Ines", 101), (10, "Juan", 120)
]

data_departamentos = [
    (101, "Ventas"), (102, "Marketing"), (103, "IT"), (104, "Finanzas"), (105, "RH"),
    (106, "Operaciones"), (107, "Legal"), (108, "Logistica"), (109, "Calidad"), (110, "Compras")
]

df_empleados = spark.createDataFrame(data_empleados, ["id_empleado", "nombre", "id_departamento"])
df_departamentos = spark.createDataFrame(data_departamentos, ["id_departamento", "nombre_departamento"])

df_empleados.createOrReplaceTempView("empleados")
df_departamentos.createOrReplaceTempView("departamentos")

print("\n" + "="*60)
print(f"{BOLD}{YELLOW}⚡ PASO 2: ANÁLISIS COMPARATIVO DE COMPORTAMIENTOS DE CRUCE (JOIN){RESET}")
print("="*60)

# --- 1. INNER JOIN ---
print("\n📋 [INNER JOIN] - Solo registros con coincidencia exacta:")
query_inner = (
    "SELECT e.id_empleado, e.nombre, d.nombre_departamento "
    "FROM empleados e INNER JOIN departamentos d ON e.id_departamento = d.id_departamento"
)
spark.sql(query_inner).show(10, truncate=False)

# --- 2. LEFT JOIN ---
print("\n📋 [LEFT JOIN] - Todos los empleados con o sin departamento:")
query_left = (
    "SELECT e.id_empleado, e.nombre, d.nombre_departamento "
    "FROM empleados e LEFT JOIN departamentos d ON e.id_departamento = d.id_departamento"
)
spark.sql(query_left).show(10, truncate=False)

# --- 3. RIGHT JOIN ---
print("\n📋 [RIGHT JOIN] - Todos los departamentos con o sin empleados:")
query_right = (
    "SELECT e.id_empleado, e.nombre, d.nombre_departamento "
    "FROM empleados e RIGHT JOIN departamentos d ON e.id_departamento = d.id_departamento"
)
spark.sql(query_right).show(10, truncate=False)

# --- 4. FULL JOIN ---
print("\n📋 [FULL JOIN] - Consolidación absoluta de ambos extremos:")
query_full = (
    "SELECT e.id_empleado, e.nombre, d.nombre_departamento "
    "FROM empleados e FULL JOIN departamentos d ON e.id_departamento = d.id_departamento"
)
spark.sql(query_full).show(15, truncate=False)
print("="*60 + "\n")

spark.stop()

```

### Paso 3. Unión Vertical con UNION

1. Crea un archivo final en VS Code llamado `union_historicos.py`.
2. Escribe y ejecuta el siguiente script que procesa el catálogo de 10 fuentes anuales:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Usar UNION").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

GREEN = "\033[92m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Carga e inicialización de los 10 archivos históricos configurados en la Tarea 0
anios = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
for anio in anios:
    ruta = os.path.abspath(f"data/TotalSales/Sales{anio}.csv")
    df = spark.read.csv(f"file://{ruta}", inferSchema=True, header=True)
    df.createOrReplaceTempView(f"y{anio}")

print("\n" + "="*60)
print(f"{BOLD}{GREEN}🥞 PASO 3: FUSIÓN VERTICAL DE HISTÓRICOS DE VENTAS (UNION){RESET}")
print("="*60)

# Fusión e impresión de los 10 sets mediante sentencias secuenciales UNION
query = " SELECT * FROM y2016 "
for anio in anios[1:]:
    query += f"UNION SELECT * FROM y{anio} "

spark.sql(query).show(12, truncate=False)
print("="*60 + "\n")

spark.stop()

```

---

### Notas Metodológicas

* **Evaluación Perezosa (Lazy Evaluation):** Cuando utilizas sentencias SQL complejas como un `JOIN` o un `UNION`, PySpark no procesa los registros de inmediato en tu almacenamiento. Solo añade estas instrucciones a un grafo de ejecución interno (Plan Lógico). La lectura física y el cruce relacional ocurren únicamente en el momento en que se invoca la acción `.show()`.
* **Seguridad de Rutas Locales:** Al usar la combinación de `os.path.abspath` junto con el prefijo `file://`, blindamos a Spark contra errores de interpretación de rutas relativas propios del subsistema local de Hadoop, garantizando portabilidad sin importar en qué subcarpeta del proyecto esté parada la consola de Visual Studio Code.