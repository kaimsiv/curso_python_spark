# PYT_SPARK_DEVESS

## Práctica 6. Acciones

### Objetivo

Al finalizar la práctica, se espera que el estudiante sea capaz de entender la función de las acciones y aplicarlas sobre RDD utilizando un entorno local optimizado.


### Objetivo visual

Se espera que el estudiante observe de forma clara la relación entre la actividad propuesta y el resultado que debe obtener al ejecutar los pasos del laboratorio.
### Duración aproximada

* 45 minutos

---


### Instrucciones

Se describen los pasos requeridos para completar la práctica de forma ordenada y coherente.

## Tarea 1. Preparación del Entorno Local y Datos de Prueba

### Paso 1. Configurar carpetas relativas desde la Terminal de VS Code

1. Abre tu terminal integrada en Visual Studio Code.
2. Ejecuta el siguiente comando Bash para crear las subcarpetas necesarias usando la bandera `-p`:

```bash
mkdir -p data/TotalSalesRed/
```
![alt text](image.png)

### Paso 2. Generar los archivos CSV locales de prueba

Para simular el volumen y estructura que requiere el laboratorio, ejecuta consecutivamente estos comandos en tu terminal local para poblar los archivos con un mínimo de 10 registros por set de datos sin depender de rutas absolutas del sistema:

```bash
# 1. Crear Sales2018.csv (10 registros completos con 12 columnas estructuradas)
echo -e "ID,Fecha,Cliente,Region,Pais,Cod1,Cod2,Prod,Cat,Tienda,Cantidad,Precio\n1001,2018-05-12,C01,North,USA,X,Y,Laptop,Tech,S1,2,1200.00\n1002,2018-05-12,C02,South,MEX,X,Y,Mouse,Tech,S2,10,25.50\n1003,2018-05-13,C03,East,CAN,X,Y,Desk,Home,S1,1,350.00\n1004,2018-05-13,C04,North,USA,X,Y,Teclado,Tech,S3,4,45.00\n1005,2018-05-14,C05,West,USA,X,Y,Monitor,Tech,S1,2,250.00\n1006,2018-05-14,C06,East,CAN,X,Y,Chair,Home,S2,5,120.00\n1007,2018-05-15,C07,South,MEX,X,Y,Audifonos,Tech,S3,3,60.00\n1008,2018-05-15,C08,North,USA,X,Y,Printer,Tech,S1,1,180.00\n1009,2018-05-16,C09,West,USA,X,Y,Cable,Tech,S2,15,10.50\n1010,2018-05-16,C10,East,CAN,X,Y,Tablet,Tech,S3,2,400.00" > data/TotalSalesRed/Sales2018.csv

# 2. Generar Sales2018f.csv (10 registros simulando filas con anomalías estructurales o menos columnas para la validación)
echo -e "ID,Fecha,Cliente,Region,Pais,Cod1,Cod2\n1001,2018-05-12,C01,North,USA,X,Y\n1002,2018-05-12\n1003,2018-05-13,C03,East,CAN,X,Y\n1004,2018-05-13,C04,North,USA,X,Y\n1005,2018-05-14\n1006,2018-05-14,C06,East,CAN,X,Y\n1007,2018-05-15,C07,South,MEX,X,Y\n1008,2018-05-15\n1009,2018-05-16,C09,West,USA,X,Y\n1010,2018-05-16,C10,East,CAN,X,Y" > data/TotalSalesRed/Sales2018f.csv

# 3. Generar Sales2020.csv (10 registros con cantidades específicas indexadas en la columna 10)
echo -e "ID,Fecha,Cliente,Region,Pais,Cod1,Cod2,Prod,Cat,Tienda,Cantidad,Precio\n2001,2020-01-10,C01,West,USA,A,B,Phone,Tech,S3,5,800.0\n2002,2020-01-11,C02,East,MEX,A,B,Case,Tech,S1,20,15.0\n2003,2020-01-12,C03,North,CAN,A,B,Charger,Tech,S2,8,25.0\n2004,2020-01-12,C04,South,MEX,A,B,Tablet,Tech,S1,12,350.0\n2005,2020-01-13,C05,West,USA,A,B,Watch,Tech,S3,3,250.0\n2006,2020-01-14,C06,North,USA,A,B,Laptop,Tech,S2,4,1200.0\n2007,2020-01-14,C07,East,CAN,A,B,Mouse,Tech,S1,15,30.0\n2008,2020-01-15,C08,South,MEX,A,B,Screen,Tech,S3,2,180.0\n2009,2020-01-15,C09,West,USA,A,B,Cable,Tech,S1,25,8.5\n2010,2020-01-16,C10,North,CAN,A,B,Keypad,Tech,S2,6,40.0" > data/TotalSalesRed/Sales2020.csv

# 4. Generar el archivo general Sales.csv en la raíz de data/ (10 registros para agregación por fechas)
echo -e "ID,Fecha,Cliente,Region,Pais,Cod1,Cod2,Prod,Cat,Tienda,Cantidad,Precio\n3001,2026-07-01,C10,North,USA,M,N,Tablet,Tech,S1,3,400.00\n3002,2026-07-01,C11,North,USA,M,N,Cable,Tech,S1,5,10.00\n3003,2026-07-02,C12,South,MEX,M,N,Screen,Tech,S2,1,250.00\n3004,2026-07-02,C13,East,CAN,M,N,Router,Tech,S1,2,85.00\n3005,2026-07-02,C14,West,USA,M,N,Hub_USB,Tech,S3,6,30.00\n3006,2026-07-03,C15,North,USA,M,N,Battery,Tech,S2,4,45.00\n3007,2026-07-03,C16,South,MEX,M,N,Webcam,Tech,S1,2,75.00\n3008,2026-07-04,C17,East,CAN,M,N,Stand,Tech,S3,8,20.00\n3009,2026-07-04,C18,West,USA,M,N,Adapter,Tech,S2,10,15.00\n3010,2026-07-04,C19,North,USA,M,N,SSD_Ext,Tech,S1,1,110.00" > data/Sales.csv
```
![alt text](image-2.png)
---

## Tarea 2. Aplicar Acciones de Conteo y Muestreo en un Archivo

### Paso 3. Crear el archivo de script en VS Code

1. En el panel explorador de VS Code, crea un nuevo archivo llamado `rdd_acciones_conteo.py`.
2. Pega el siguiente código estructurado:

```python
# ==============================================================================
# 1. PREVENCIÓN DE ERRORES DE JAVA (INYECCIÓN DINÁMICA)
# ==============================================================================
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

# ==============================================================================
# 2. UNIFICACIÓN DE CÓDIGO CON SPARKSESSION
# ==============================================================================
from pyspark.sql import SparkSession

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("Ejemplo RDD desde archivo") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Estilos de formato terminal
CYAN = "\033[96m"
GREEN = "\033[92m"
BOLD = "\033[1m"
RESET = "\033[0m"

# ==============================================================================
# 3. PROCESAMIENTO CON RUTAS RELATIVAS LOCALES
# ==============================================================================
# Cargar un RDD desde un archivo de texto usando ruta relativa
rdd = sc.textFile("data/TotalSalesRed/Sales2018.csv")

# Transformación: Dividir cada línea en palabras (campos)
rdd_palabras = rdd.flatMap(lambda linea: linea.split(","))

# Transformación: Convertir palabras a minúsculas y mayúsculas
rdd_minusculas = rdd_palabras.map(lambda palabra: palabra.lower())
rdd_mayusculas = rdd_palabras.map(lambda palabra: palabra.upper())

# Acción: Contar la cantidad de palabras
cantidad_palabras = rdd_minusculas.count()

# Acción: Recopilar las primeras 10 palabras usando take()
primerasMin_palabras = rdd_minusculas.take(10)
primerasMay_palabras = rdd_mayusculas.take(10)

# Mostrar resultados formateados
print("\n" + "="*60)
print(f"{BOLD}{CYAN} TAREA 2: ACCIONES DE CONTEO Y MUESTREO EN RDD{RESET}")
print("="*60)
print(f"{BOLD} Cantidad total de palabras recuperadas:{RESET} {GREEN}{cantidad_palabras}{RESET}")
print("-" * 60)
print(f"{BOLD} Muestra de primeras 10 palabras minúsculas (take):{RESET}\n  {primerasMin_palabras}")
print(f"{BOLD} Muestra de primeras 10 palabras mayúsculas (take):{RESET}\n  {primerasMay_palabras}")
print("="*60 + "\n")

# Cerrar la sesión de Spark de forma limpia
spark.stop()
```
![alt text](image-3.png)
---
### Paso 4. Ejecución

* Guarda el archivo (`Ctrl + S`).
* Haz clic en el botón **Play (▶)** de VS Code en la esquina superior derecha.

---

## Tarea 3. Validación de Archivos por Columnas

### Paso 5. Crear el archivo de script en VS Code

1. Crea un nuevo archivo llamado `rdd_validacion_columnas.py`.
2. Añade el código optimizado para examinar la integridad del archivo modificado estructuralmente:

```python
# ==============================================================================
# 1. PREVENCIÓN DE ERRORES DE JAVA (INYECCIÓN DINÁMICA)
# ==============================================================================
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

# ==============================================================================
# 2. UNIFICACIÓN DE CÓDIGO CON SPARKSESSION
# ==============================================================================
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ejemplo RDD desde CSV") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# ==============================================================================
# 3. PROCESAMIENTO CON RUTAS RELATIVAS LOCALES
# ==============================================================================
# Cargar el archivo modificado
rdd = sc.textFile("data/TotalSalesRed/Sales2018f.csv")

# Transformación: Dividir cada línea en columnas
rdd_columnas = rdd.map(lambda linea: linea.split(","))

# Transformación: Filtrar filas que tienen más de 6 columnas
rdd_filtrado = rdd_columnas.filter(lambda columnas: len(columnas) > 6)

# Acción: Contar el número de filas válidas
cantidad_filas = rdd_filtrado.count()

# Acción: Recopilar las primeras 3 filas
primeras_filas = rdd_filtrado.take(3)

# Mostrar resultados
print("\n" + "="*60)
print(f"{BOLD}{GREEN} TAREA 3: AUDITORÍA Y VALIDACIÓN ESTRUCTURAL DE COLUMNAS{RESET}")
print("="*60)
print(f"{BOLD} Cantidad de registros válidos encontrados (> 6 columnas):{RESET} {YELLOW}{cantidad_filas}{RESET}")
print("-" * 60)
print(f"{BOLD} Muestra de las primeras 3 filas válidas procesadas:{RESET}")
for fila in primeras_filas:
    print(f"   {fila}")
print("="*60 + "\n")

spark.stop()
```
![alt text](image-4.png)
---
### Paso 6. Ejecución

* Ejecuta este script pulsando el botón **Play (▶)** y observa cómo se discriminan los registros truncados gracias al cálculo perezoso evaluado por las acciones de conteo.

---

## Tarea 4. Extraer y Calcular Valores desde un CSV

### Paso 7. Crear el archivo de script en VS Code

1. Crea un archivo llamado `rdd_calculo_valores.py`.
2. Agrega la lógica para calcular estadísticas sobre los campos posicionales numéricos:

```python
# ==============================================================================
# 1. PREVENCIÓN DE ERRORES DE JAVA (INYECCIÓN DINÁMICA)
# ==============================================================================
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

# ==============================================================================
# 2. UNIFICACIÓN DE CÓDIGO CON SPARKSESSION
# ==============================================================================
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Carga desde CSV") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# ==============================================================================
# 3. PROCESAMIENTO CON RUTAS RELATIVAS LOCALES
# ==============================================================================
rdd = sc.textFile("data/TotalSalesRed/Sales2020.csv")

# Dividir cada línea en columnas
rdd_columnas = rdd.map(lambda linea: linea.split(","))

# Filtrar la cabecera usando la acción first()
cabecera = rdd_columnas.first()
rdd_datos = rdd_columnas.filter(lambda linea: linea != cabecera)

# Mapear a la columna numérica correspondiente a Cantidad (Índice 10)
rdd_numeros = rdd_datos.map(lambda linea: float(linea[10]))

print("\n" + "="*60)
print(f"{BOLD}{CYAN} TAREA 4: EXTRACCIÓN Y ESTADÍSTICAS NUMÉRICAS DESDE CSV{RESET}")
print("="*60)
print(f"{BOLD} Muestra completa del RDD numérico recuperado (collect):{RESET}")

# Control simple para mostrar el contenido del rdd de manera local
for row in rdd_numeros.collect():
    print(f"   Valor extraído: {row}")

# Acción: Contar el número de elementos
total_elementos = rdd_numeros.count()

# Acción: Sumar todos los valores usando reduce()
suma_total = rdd_numeros.reduce(lambda x, y: x + y)

# Calcular el promedio numérico en el driver
promedio = suma_total / total_elementos

print("-" * 60)
print(f" {BOLD}Métricas Consolidadas (Driver Local):{RESET}")
print(f"    Total Elementos : {YELLOW}{total_elementos}{RESET}")
print(f"    Suma Acumulada  : {GREEN}{suma_total}{RESET}")
print(f"    Promedio Calculado: {CYAN}{promedio:.2f}{RESET}")
print("="*60 + "\n")

spark.stop()
```
![alt text](image-5.png)
---

## Tarea 5. Agregación Avanzada de Importes y Ventas Diarias

### Paso 8. Crear el archivo de script en VS Code

1. Diseña un archivo llamado `rdd_agregacion_ventas.py`.
2. Pega el script que analiza e itera los pedidos de manera detallada:

```python
# ==============================================================================
# 1. PREVENCIÓN DE ERRORES DE JAVA (INYECCIÓN DINÁMICA)
# ==============================================================================
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

# ==============================================================================
# 2. UNIFICACIÓN DE CÓDIGO CON SPARKSESSION
# ==============================================================================
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CargaCSV") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# ==============================================================================
# 3. PROCESAMIENTO CON RUTAS RELATIVAS LOCALES
# ==============================================================================
rdd = sc.textFile("data/Sales.csv")

# Obtener la primera línea (encabezado) mediante una acción
header = rdd.first()

# Filtrar el encabezado
rdd_data = rdd.filter(lambda line: line != header)

# Transformación: Parsear el CSV (dividir cada línea por comas)
rdd_datos = rdd_data.map(lambda line: line.split(","))

print("\n" + "="*60)
print(f"{BOLD}{CYAN} TAREA 5: ANÁLISIS DE TRASACCIONES Y VOLUMEN DE FACTURACIÓN{RESET}")
print("="*60)
print(f"{BOLD} Muestra parcial de arrays segmentados (take 5):{RESET}")
for fila in rdd_datos.take(5):
    print(f"   {fila}")

# Transformación: Calcular el total de ventas (Precio [11] * Cantidad [10]) por registro
# Devuelve: (Pedido, Fecha, Cliente, Pais, Cat, Cantidad, Precio, ImporteCalculado)
rdd_ventas = rdd_datos.map(lambda x: (
    x[0], x[1], x[2], x[4], x[8], x[10], x[11], 
    float(x[10]) * float(x[11])
))

print("-" * 60)
print(f"{BOLD} Ventas Detalladas con Importe Calculado (collect):{RESET}")
for row in rdd_ventas.collect():
    print(f"   Orden: {row[0]} | Fecha: {row[1]} | País: {row[3]} | Total: {GREEN}${row[7]:.2f}{RESET}")

print("-" * 60)
print(f"{BOLD} Métricas Consolidadas y Distribución por Día:{RESET}")
# Acción: Contar el número total de transacciones de ventas
print(f"    Número total de operaciones de venta: {YELLOW}{rdd_ventas.count()}{RESET}")

# Transformación: Mapear a pares (fecha, 1) para contar frecuencias
rdd_fechas = rdd_ventas.map(lambda cols: (cols[1], 1))

# Transformación: Reducir por clave para agrupar ventas por día
rdd_ventas_por_dia = rdd_fechas.reduceByKey(lambda x, y: x + y)

print(f"\n    Volumen agrupado por fecha de facturación:")
for fecha, ventas in rdd_ventas_por_dia.collect():
    print(f"      Fecha: {CYAN}{fecha}{RESET} -> Transacciones Realizadas: {GREEN}{ventas}{RESET}")
print("="*60 + "\n")

spark.stop()

```

### Resultado esperado

![resultado](../curso_python_spark/images/lab6_resultado.png)
