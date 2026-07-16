# PYT_SPARK_DEVESS

## Práctica 4. Uso de DataFrames, cálculos y operaciones con columnas y transformaciones

### Objetivo

Al finalizar la práctica, se espera que el estudiante sea capaz de implementar lecturas, consultas estructuradas, operaciones de agregación y transformaciones simulando operaciones CRUD con SQL directo en PySpark utilizando Visual Studio Code.


### Objetivo visual

Se espera que el estudiante observe de forma clara la relación entre la actividad propuesta y el resultado que debe obtener al ejecutar los pasos del laboratorio.
### Duración aproximada

* 60 minutos.

### Prerrequisitos

* Haber completado las prácticas 1 y 2.

---


### Instrucciones

Se describen los pasos requeridos para completar la práctica de forma ordenada y coherente.

## Tarea 1. Crear DataFrame desde fuentes de datos y crear vistas temporales

Registrar un DataFrame como una vista temporal te permite ejecutar consultas SQL estándar (`SELECT`, `WHERE`, `GROUP BY`) directamente sobre los conjuntos de datos distribuidos en Spark sin tener que migrarlos a una base de datos relacional.

* **Vista temporal local:** Solo está disponible en la sesión de Spark que la creó (`spark`).
* **Vista temporal global:** Se almacena en el catálogo del sistema (`global_temp`) y está disponible para múltiples sesiones de Spark en paralelo.

#### Paso 1. Preparación de datos enriquecidos para pruebas de SQL

Para asegurar que las consultas no devuelvan tablas vacías, ejecuta estos comandos en tu terminal de VS Code para actualizar tus archivos de prueba con 10 registros por archivo:

```bash
# 1. Crear la carpeta "data" y su subcarpeta "Model" al mismo tiempo
mkdir -p data/Model/

# 2. Regenerar el archivo Sales.csv (10 registros con fechas, países variados y montos consistentes)
echo -e "SalesOrderNumber,OrderDate,Country,Product,Quantity,Sales\nSO43659,2020-01-15,Canada,Mountain Bike,1,3500.00\nSO43660,2021-06-20,Germany,Helmet,3,3400.00\nSO43661,2021-07-11,Canada,Socks,2,3200.00\nSO43662,2022-03-05,France,Jersey,1,150.00\nSO43663,2022-04-12,Canada,Laptop,1,1200.00\nSO43664,2023-05-18,Germany,Teclado,5,45.00\nSO43665,2023-08-22,France,Monitor,2,250.00\nSO43666,2024-01-10,Canada,Mouse,10,15.50\nSO43667,2024-03-15,Germany,Impresora,1,180.00\nSO43668,2025-02-28,France,Audifonos,4,60.00" > data/Sales.csv

# 3. Regenerar el archivo Products.csv (10 registros enlazados por nombre de producto)
echo -e "ProductKey,Product,Category,Price\n1,Mountain Bike,Bikes,3399.99\n2,Helmet,Accessories,34.99\n3,Socks,Clothing,9.50\n4,Laptop,Components,1200.00\n5,Teclado,Accessories,45.00\n6,Monitor,Components,250.00\n7,Mouse,Accessories,15.50\n8,Impresora,Components,180.00\n9,Audifonos,Accessories,60.00\n10,Disco_Duro,Components,85.00" > data/Model/Products.csv
```

#### Paso 2. Creación de vista temporal local a partir de una colección

Crea el archivo `sql_vista_local.py`:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQLEjemploColeccion").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Estilos visuales
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Datos de prueba locales (Incrementado a 10 registros para consistencia)
data = [
    ("Alicia", 38, 50000, "HR"),
    ("Bertha", 30, 60000, "IT"),
    ("Carlos", 48, 70000, "Finance"),
    ("David", 36, 80000, "HR"),
    ("Eva", 42, 90000, "IT"),
    ("Fernando", 29, 45000, "HR"),
    ("Gabriela", 31, 85000, "Finance"),
    ("Hugo", 52, 95000, "IT"),
    ("Isabel", 26, 62000, "HR"),
    ("Jorge", 40, 75000, "Finance")
]
columnas = ["name", "age", "salary", department"]

df = spark.createDataFrame(data, columnas)

# Registrar el DataFrame como una vista temporal local
df.createOrReplaceTempView("employees")

print("\n" + "="*60)
print(f"{BOLD}{CYAN} CONSULTA SQL ESTÁNDAR SOBRE VISTA TEMPORAL (SALARIO >= 70000){RESET}")
print("="*60)
result = spark.sql("SELECT name, age, salary, department FROM employees WHERE salary >= 70000")
result.show(10, truncate=False)
print("="*60 + "\n")

spark.stop()
```

#### Paso 3. Consultas SQL avanzadas, filtros y agregaciones desde archivos CSV

Crea el archivo `sql_consultas_ventas.py`:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQLConsultasVentas").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Leer el CSV relacional usando rutas locales relativas
df = spark.read.csv("data/Sales.csv", inferSchema=True, header=True)
df.createOrReplaceTempView("ventas")

print("\n" + "="*60)
print(f"{BOLD}{CYAN}🇨🇦 1. FILTRO BÁSICO CONDICIONAL (Country = 'Canada'){RESET}")
print("="*60)
res_filtro = spark.sql("SELECT SalesOrderNumber, OrderDate, Country, Product, Sales FROM ventas WHERE Country = 'Canada'")
res_filtro.show(10, truncate=False)

print("\n" + "="*60)
print(f"{BOLD}{GREEN} 2. FILTRO COMPLEJO CON OPERADORES LÓGICOS (OR, AND, BETWEEN){RESET}")
print("="*60)
res_complejo = spark.sql("""
    SELECT SalesOrderNumber, OrderDate, Country, Product, Sales 
    FROM ventas 
    WHERE (Country = 'Canada' OR Country = 'Germany') 
    AND Sales BETWEEN 3000 AND 4000
""")
res_complejo.show(10, truncate=False)

print("\n" + "="*60)
print(f"{BOLD}{YELLOW} 3. MÉTRICAS DE AGREGACIÓN DISTRIBUIDAS (GROUP BY y ORDER BY){RESET}")
print("="*60)
res_agrupado = spark.sql("""
    SELECT Country, COUNT(Sales) as NoOperaciones, SUM(Sales) as Total
    FROM ventas 
    GROUP BY Country 
    ORDER BY Total DESC
""")
res_agrupado.show(10, truncate=False)
print("="*60 + "\n")

spark.stop()
```

---

## Tarea 2. Operaciones CRUD con DataFrames (Simulación y Limitations)

>  **Nota Teórica Fundamental:** Los DataFrames en PySpark se basan en RDDs, los cuales son **estructuras inmutables y tolerantes a fallos**. Por ende, **las operaciones nativas de SQL como `UPDATE` o `DELETE` están prohibidas** y arrojarán una excepción (`AnalysisException`). Para modificar datos en Spark, se debe simular la operación proyectando nuevas columnas condicionales o filtrando registros no deseados mediante sentencias `SELECT`.

#### Paso 4. Simulación de UPDATE (Estructuras CASE WHEN)

Crea el archivo `crud_update.py`:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CRUDUpdate").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

CYAN = "\033[96m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

df = spark.read.csv("data/Model/Products.csv", inferSchema=True, header=True)
df.createOrReplaceTempView("catalogo")

print("\n" + "="*60)
print(f"{BOLD}{CYAN} SIMULACIÓN CORRECTA DE UPDATE USANDO LÓGICA CONDICIONAL CASE WHEN{RESET}")
print("="*60)
df_actualizado = spark.sql("""
    SELECT ProductKey, Product, Category, Price, 
    CASE WHEN Category = 'Accessories' THEN Price * 1.10 ELSE Price END AS NewPrice 
    FROM catalogo
""")
df_actualizado.show(10, truncate=False)

print("-" * 60)
print(f"{BOLD} INTENTO DE UPDATE TRADICIONAL (DEMOSTRACIÓN DE RESTRICCIÓN){RESET}")
print("-" * 60)
try:
    spark.sql("UPDATE catalogo SET Price = Price * 1.10 WHERE Category = 'Accessories'")
except Exception as e:
    print(f"   {BOLD}Spark rechazó el comando correctamente:{RESET}\n  {YELLOW}{str(e)[:105]}...{RESET}")
print("="*60 + "\n")

spark.stop()

```

#### Paso 5. Simulación de DELETE (Exclusión mediante Cláusula WHERE)

Crea el archivo `crud_delete.py`:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CRUDDelete").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

GREEN = "\033[92m"
BOLD = "\033[1m"
RESET = "\033[0m"

df = spark.read.csv("data/Sales.csv", inferSchema=True, header=True)
df.createOrReplaceTempView("ventas")

print("\n" + "="*60)
print(f"{BOLD}{GREEN} SIMULACIÓN DE DELETE EXCLUYENDO EL AÑO 2020 CON FILTROS DE FECHA{RESET}")
print("="*60)
df_filtrado = spark.sql("""
    SELECT YEAR(CAST(OrderDate AS DATE)) AS Year, COUNT(Sales) as NoVentas, SUM(Sales) as TotalSales
    FROM ventas 
    WHERE YEAR(CAST(OrderDate AS DATE)) != 2020
    GROUP BY YEAR(CAST(OrderDate AS DATE))
    ORDER BY Year ASC
""")
df_filtrado.show(10, truncate=False)
print("="*60 + "\n")

spark.stop()

```

---

## Tarea 3. Uso de vistas temporales globales (Multi-Sesión)

Para compartir datos procesados entre diferentes flujos de analítica dentro de una misma aplicación de clúster, creamos una vista global accesible mediante el prefijo reservado de catálogo `global_temp.`.

#### Paso 6. Creación e invocación inter-sesión

Crea el archivo `sql_vista_global.py`:

```python
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
```

### Resultado esperado

![resultado](../curso_python_spark/images/lab4_resultado1.png)
---
![resultado](../curso_python_spark/images/lab4_resultado2.png)
