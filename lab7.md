# PYT_SPARK_DEVESS

## Práctica 7. Aspectos avanzados

### Objetivo

Al finalizar la práctica, se espera que el estudiante sea capaz de aplicar técnicas avanzadas a conjuntos de datos, como shuffling, accumulators, partitioning y broadcast de variables.


### Objetivo visual

Se espera que el estudiante observe de forma clara la relación entre la actividad propuesta y el resultado que debe obtener al ejecutar los pasos del laboratorio.
### Duración aproximada

* 60 minutos.

---

## Preparación del Entorno Local

Antes de comenzar con los scripts de Python, prepare los directorios necesarios y simule los archivos de prueba. Ejecute el siguiente bloque en su terminal de Bash integrada en VS Code para poblar los archivos con un mínimo de 10 registros por archivo:

```bash
# Crear las subcarpetas necesarias localmente utilizando la bandera -p
mkdir -p data/Model
mkdir -p salidas

# 1. Crear Products.csv (10 registros con ID, Nombre y Categoría)
echo -e "id,name,category\n1,ProductA,Cat1\n2,ProductB,Cat2\n3,ProductC,Cat1\n4,ProductD,Cat3\n5,ProductE,Cat2\n6,ProductF,Cat1\n7,ProductG,Cat3\n8,ProductH,Cat2\n9,ProductI,Cat1\n10,ProductJ,Cat3" > data/Model/Products.csv

# 2. Crear Customers.csv (10 registros con ID y Nombre)
echo -e "id,customer_name\n1,CustomerA\n2,CustomerB\n3,CustomerC\n4,CustomerD\n5,CustomerE\n6,CustomerF\n7,CustomerG\n8,CustomerH\n9,CustomerI\n10,CustomerJ" > data/Model/Customers.csv

# 3. Crear Sales.csv (10 registros relacionales con 11 columnas completas)
echo -e "id,date,store,country,product_id,customer_id,product_name,qty,price,tax,amount\n1,2026-07-09,Store1,Mexico,101,1,ProductA,2,50.0,0.15,100.0\n2,2026-07-09,Store2,Canada,102,2,ProductB,1,150.0,0.15,150.0\n3,2026-07-10,Store1,Mexico,103,3,ProductC,5,20.0,0.15,100.0\n4,2026-07-10,Store3,USA,101,4,ProductA,1,50.0,0.15,50.0\n5,2026-07-11,Store2,Canada,104,5,ProductD,3,30.0,0.15,90.0\n6,2026-07-11,Store1,Mexico,102,6,ProductB,2,150.0,0.15,300.0\n7,2026-07-12,Store3,USA,105,7,ProductE,10,12.0,0.15,120.0\n8,2026-07-12,Store2,Canada,103,8,ProductC,4,20.0,0.15,80.0\n9,2026-07-13,Store1,Mexico,106,9,ProductF,1,200.0,0.15,200.0\n10,2026-07-13,Store3,USA,104,10,ProductD,2,30.0,0.15,60.0" > data/Sales.csv

```

---


### Instrucciones

Se describen los pasos requeridos para completar la práctica de forma ordenada y coherente.

## Tarea 1. Crear y ajustar el número de particiones

### Paso 1. Crear un RDD con un número específico de particiones

Crea un archivo llamado `tarea1_particiones.py` en VS Code y ejecútalo presionando el botón **Play (▶)** o la tecla **F5**.

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

# Inicializar SparkSession unificada
spark = SparkSession.builder \
    .appName("Particionamiento") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Crear un RDD con 4 particiones de forma explícita
rdd = sc.parallelize(range(10), 4)

print("\n" + "="*60)
print(f"{BOLD}{CYAN} TAREA 1.1: CREACIÓN DE RDD CON PARTICIONES EXPLICITAS{RESET}")
print("="*60)
# Ver el número de particiones
print(f"{BOLD} Número de particiones creadas:{RESET} {rdd.getNumPartitions()}")
print("-" * 60)
# Mostrar el contenido distribuido de cada partición
print(f"{BOLD} Contenido distribuido por partición (glom):{RESET}")
for i, part in enumerate(rdd.glom().collect()):
    print(f"    Partición {i}: {part}")
print("="*60 + "\n")

# Cerrar la sesión de Spark de forma limpia
spark.stop()

```

* **Explicación:** `sc.parallelize(range(10), 4)` fragmenta una lista de 10 elementos distribuyéndola en 4 particiones internas. El método `glom()` agrupa los elementos de cada partición en una lista para su visualización.

### Paso 2. Cargar un archivo CSV forzando un mínimo de particiones

Crea un archivo llamado `tarea1_cargar_csv.py` y ejecútalo desde VS Code.

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Cargar CSV con 4 particiones") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

GREEN = "\033[92m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Ruta relativa local adaptada para el entorno de desarrollo
ruta_csv = "data/Model/Products.csv"

# Cargar el archivo CSV en un RDD exigiendo un mínimo de 4 particiones
rdd = sc.textFile(ruta_csv, minPartitions=4)

# Transformación: Dividir cada línea por el delimitador coma
rdd_columnas = rdd.map(lambda linea: linea.split(","))

# Extraer y filtrar la cabecera del RDD
cabecera = rdd_columnas.first()
rdd_datos = rdd_columnas.filter(lambda linea: linea != cabecera)

# Inspeccionar el contenido de cada partición
print("\n" + "="*60)
print(f"{BOLD}{GREEN} TAREA 1.2: CARGA DE ARCHIVO FORZANDO MINPARTITIONS{RESET}")
print("="*60)
print(f"{BOLD} Número de particiones reales creadas:{RESET} {rdd.getNumPartitions()}")
print("-" * 60)
print(f"{BOLD} Contenido detallado de las particiones (Filas del CSV):{RESET}")
particiones = rdd_datos.glom().collect()

for i, particion in enumerate(particiones):
    print(f"    Partición {i}: {particion}")
print("="*60 + "\n")

spark.stop()

```

> **Nota:** El número real de particiones puede ser mayor que 4 si el archivo físico en el disco es de gran tamaño, ya que el parámetro `minPartitions` actúa como una restricción mínima, no como un límite máximo estricto.

---

## Tarea 2. Reparticionamiento de datos

El reparticionamiento permite reajustar dinámicamente la granularidad de las particiones de un RDD para optimizar el paralelismo. Se dispone de dos funciones con comportamientos críticamente diferentes: `repartition()` y `coalesce()`.

Crea el archivo `tarea2_reparticionar.py` y ejecútalo en VS Code.

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Reparticionamiento Local") \
    .master("local[4]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

CYAN = "\033[96m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Uso de ruta relativa local
rdd = sc.textFile("data/Model/Customers.csv")

print("\n" + "="*60)
print(f"{BOLD}{CYAN} TAREA 2: EVALUACIÓN DE REPARTITION() VS COALESCE(){RESET}")
print("="*60)
print(f"{BOLD} Particiones originales del archivo CSV:{RESET} {rdd.getNumPartitions()}")
print("-" * 60)

# Reparticionar el RDD incrementando a 10 particiones (Provoca Shuffling completo)
rdd_reparticionado1 = rdd.repartition(10)

# Reducir u optimizar particiones existentes (Evita Shuffling si se reduce)
rdd_reparticionado2 = rdd.coalesce(2) 

print(f"{BOLD} Resultados tras repartition(10) (Shuffle Amplio):{RESET}")
print(f"    Número de particiones: {rdd_reparticionado1.getNumPartitions()}")
print(f"    Distribución glom(): {rdd_reparticionado1.glom().collect()[:3]}... [Muestra]")

print("-" * 60)
print(f"{BOLD} Resultados tras coalesce(2) (Agrupación Optimizada):{RESET}")
print(f"    Número de particiones: {rdd_reparticionado2.getNumPartitions()}")
print(f"    Distribución glom(): {YELLOW}{rdd_reparticionado2.glom().collect()}{RESET}")
print("="*60 + "\n")

spark.stop()

```

---

## Tarea 3. Optimización con particionamiento por clave y persistencia

Crea el archivo `tarea3_optimizacion.py` y ejecútalo desde VS Code.

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import StorageLevel

spark = SparkSession.builder \
    .appName("Optimización con Particionamiento") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Cargar datos desde ruta relativa local
rdd = sc.textFile("data/Sales.csv")

# Eliminar cabecera
cabecera = rdd.first()
rdd_datos = rdd.filter(lambda linea: linea != cabecera)

# Transformación: Mapear a tuplas de tipo (producto, cantidad)
# El nombre del producto está en el índice 6 y la cantidad en el índice 7
rdd_productos = rdd_datos.map(lambda linea: (linea.split(",")[6], float(linea.split(",")[7])))

# Reparticionar de manera inteligente por clave utilizando una función Hash interna
rdd_reparticionado = rdd_productos.partitionBy(4)

# Persistir el RDD en caché combinando Memoria y Disco para optimizar re-cálculos
rdd_reparticionado.persist(StorageLevel.MEMORY_AND_DISK)

# Reducción: Calcular el volumen total agrupado por producto
rdd_total = rdd_reparticionado.reduceByKey(lambda x, y: x + y)

# Acción: Recopilar resultados finales al Driver
resultados = rdd_total.collect()

print("\n" + "="*60)
print(f"{BOLD}{GREEN} TAREA 3: REDUCCIÓN AGRUPADA CON HASH-PARTITIONING Y PERSISTENCIA{RESET}")
print("="*60)
print(f"{BOLD} Volumen total acumulado por clave de producto:{RESET}")

for producto, total in resultados:
    print(f"    Producto: {producto:<12} | Total Unidades Vendidas: {YELLOW}{total}{RESET}")
print("-" * 60)

# Liberar explícitamente el espacio ocupado en la memoria por la persistencia
rdd_reparticionado.unpersist()
print(f"   {GREEN}[INFO]{RESET} Almacenamiento persistente liberado del RDD (unpersist).")
print("="*60 + "\n")

spark.stop()

```

---

## Tarea 4. Reconocer el impacto del Shuffling

El **shuffling** ocurre cuando los datos necesitan redistribuirse entre hilos o nodos para ser agrupados por una clave común. Al implicar transferencias o escrituras, es la operación más costosa en Spark.

Crea el archivo `tarea4_shuffling.py` en tu entorno VS Code.

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ejemplo de Shuffling") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

CYAN = "\033[96m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# RDD inicial con duplicidad de claves (Incrementado a un set representativo)
rdd = sc.parallelize([
    ("a", 1), ("b", 2), ("a", 3), ("b", 4), ("c", 5),
    ("a", 10), ("c", 2), ("b", 8), ("d", 1), ("d", 4)
])

# Operación de Shuffling amplio: Agrupa valores pesadamente en red o memoria
rdd_agrupado = rdd.groupByKey()

resultados = rdd_agrupado.collect()

print("\n" + "="*60)
print(f"{BOLD}{CYAN}  TAREA 4: EVALUACIÓN DE AGREGACIONES MEDIANTE SHUFFLING (groupByKey){RESET}")
print("="*60)
print(f"{BOLD} Claves redistribuidas con sus listas de valores consolidadas:{RESET}")

for clave, valores in resultados:
    print(f"    Clave: {clave:<3} -> Lista de valores agrupados: {YELLOW}{list(valores)}{RESET}")
print("="*60 + "\n")

spark.stop()

```

---

## Tarea 5. Minimizar Shuffling mediante agregaciones locales

Para optimizar rendimiento, sustituya siempre que sea posible `groupByKey()` por `reduceByKey()`. Esta última efectúa una combinación local en la partición antes de transferir datos.

Crea el archivo `tarea5_minimizar_shuffling.py` en VS Code.

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Minimizacion Shuffling") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Cargar origen local
rdd = sc.textFile("data/Sales.csv")

header = rdd.first()
rdd_data = rdd.filter(lambda line: line != header)
rdd_datos = rdd_data.map(lambda line: line.split(","))

# Extraer el País (Índice 3) y el Importe calculado (Multiplicación de Qty [7] * Price [8])
rdd_ventas = rdd_datos.map(lambda x: (x[3], float(x[7]) * float(x[8])))

# Reducción altamente eficiente con combinación local (ReduceByKey)
rdd_total = rdd_ventas.reduceByKey(lambda x, y: x + y)

resultados = rdd_total.collect()

print("\n" + "="*60)
print(f"{BOLD}{GREEN} TAREA 5: OPTIMIZACIÓN COMBINADA MEDIANTE MAP-SIDE COMBINE (reduceByKey){RESET}")
print("="*60)
print(f"{BOLD} Importe total consolidado por país de forma óptima:{RESET}")

for pais, total in resultados:
    print(f"     País: {pais:<10} : Importe Total Acumulado: {YELLOW}${total:.2f}{RESET}")
print("="*60 + "\n")

spark.stop()

```

---

## Tarea 6. Broadcast de variables (Variables Transmitidas)

El mecanismo de variables `broadcast` permite clonar estructuras de datos de solo lectura directamente en la memoria de los nodos trabajadores (*executors*) una única vez, impidiendo que se retransmitan con cada ejecución lambda.

### Paso 3. Enriquecer un RDD con un diccionario transmitido

Crea el archivo `tarea6_broadcast_dict.py` en VS Code.

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Broadcast de Diccionarios") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

CYAN = "\033[96m"
GREEN = "\033[92m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Diccionario extendido a 10 elementos para un mapeo completo
diccionario_productos = {
    101: "ProductoA", 102: "ProductoB", 103: "ProductoC", 104: "ProductD", 105: "ProductE",
    106: "ProductF", 107: "ProductG", 108: "ProductH", 109: "ProductI", 110: "ProductJ"
}

# Declarar la variable Broadcast
broadcast_diccionario = sc.broadcast(diccionario_productos)

rdd_ventas = sc.parallelize([
    (101, 2), (102, 3), (103, 1), (104, 5), (105, 2),
    (106, 1), (107, 4), (108, 8), (109, 3), (110, 1)
])

# Acceder al valor del broadcast local usando .value
rdd_enriquecido = rdd_ventas.map(lambda venta: (
    venta[0], 
    venta[1], 
    broadcast_diccionario.value.get(venta[0], "Desconocido")
))

resultados = rdd_enriquecido.collect()

print("\n" + "="*60)
print(f"{BOLD}{CYAN}  TAREA 6.1: ENRIQUECIMIENTO ASOCIATIVO CON BROADCAST VARIABLES{RESET}")
print("="*60)
print(f"{BOLD} Registros cruzados eficientemente en memoria executor:{RESET}")

for res in resultados:
    print(f"    ID Producto: {res[0]} | Cantidad: {res[1]} | Nombre Mapeado: {GREEN}{res[2]}{RESET}")

broadcast_diccionario.unpersist()
print("="*60 + "\n")

spark.stop()

```

### Paso 4. Filtrado rápido utilizando listas transmitidas

Crea el archivo `tarea6_broadcast_lista.py` en VS Code.

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Broadcast de Listas") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Lista de productos prioritarios o en oferta
productos_oferta = [101, 103, 105, 107, 109]
broadcast_oferta = sc.broadcast(productos_oferta)

rdd_ventas = sc.parallelize([
    (101, 2), (102, 3), (103, 1), (104, 5), (105, 2),
    (106, 1), (107, 4), (108, 8), (109, 3), (110, 1)
])

# Operación de filtro referenciando la variable remota compartida
rdd_filtrado = rdd_ventas.filter(lambda venta: venta[0] in broadcast_oferta.value)

resultados = rdd_filtrado.collect()

print("\n" + "="*60)
print(f"{BOLD}{GREEN} TAREA 6.2: FILTRADO DISTRIBUIDO USANDO LISTAS TRANSMITIDAS{RESET}")
print("="*60)
print(f"{BOLD} Registros emparejados con la lista de ofertas estáticas:{RESET}")

for res in resultados:
    print(f"    {BOLD}Venta en Oferta ->{RESET} ID Producto: {YELLOW}{res[0]}{RESET} | Unidades: {res[1]}")

broadcast_oferta.unpersist()
print("="*60 + "\n")

spark.stop()

```

---

## Tarea 7. Broadcast con tablas de parámetros de configuración

Crea el archivo `tarea7_broadcast_config.py` en VS Code.

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Broadcast de Configuración") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

configuracion = {
    "tasa_impuesto": 0.15,
    "descuento": 0.10
}

broadcast_config = sc.broadcast(configuracion)

rdd_ventas = sc.parallelize([
    ("ProductoA", 100), ("ProductoB", 200), ("ProductoC", 150), ("ProductoD", 80), ("ProductoE", 300),
    ("ProductoF", 120), ("ProductoG", 450), ("ProductoH", 90),  ("ProductoI", 250), ("ProductoJ", 175)
])

# Transformación de cálculo en paralelo usando los parámetros globales integrados
rdd_calculado = rdd_ventas.map(lambda venta: (
    venta[0],
    venta[1],
    venta[1] * (1 + broadcast_config.value["tasa_impuesto"]),
    venta[1] * (1 - broadcast_config.value["descuento"])
))

resultados = rdd_calculado.collect()

print("\n" + "="*60)
print(f"{BOLD}{CYAN}  TAREA 7: PIPELINE FINANCIERO CON MATRIZ DE CONFIGURACIÓN DIRECTA{RESET}")
print("="*60)
print(f"{BOLD} Auditoría de importes corregidos por impuestos y descuentos:{RESET}")

for res in resultados:
    print(f"    {res[0]:<12} | Base: ${res[1]:<5} | Con IVA (15%): {GREEN}${res[2]:<6.2f}{RESET} | Con Desc (10%): {YELLOW}${res[3]:.2f}{RESET}")

broadcast_config.unpersist()
print("="*60 + "\n")

spark.stop()

```

---

## Tarea 8. Uso avanzado de acumuladores

Los acumuladores son variables nativas distribuidas de **solo adición**. Permiten computar contadores globales o métricas agregadas agregando valores desde las tareas executoras al hilo principal.

### Paso 5. Contar elementos condicionales mediante Foreach

Crea el archivo `tarea8_contador.py` en tu espacio de trabajo.

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ejemplo Acumulador Contador") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

CYAN = "\033[96m"
GREEN = "\033[92m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Lista incrementada a un set de 10 números muestreables
rdd = sc.parallelize([1, 2, 3, 6, 7, 8, 9, 10, 4, 12])

# Inicializar un acumulador entero en cero
contador = sc.accumulator(0)

# El Driver envía la lógica, las tareas ejecutan .add() de forma aislada
rdd.foreach(lambda x: contador.add(1) if x > 5 else None)

print("\n" + "="*60)
print(f"{BOLD}{CYAN} TAREA 8.1: CONTEO GLOBAL MEDIANTE ACUMULADORES DISTRIBUIDOS{RESET}")
print("="*60)
print(f"    {BOLD}Números mayores que 5 contabilizados en el clúster:{RESET} {GREEN}{contador.value}{RESET}")
print("="*60 + "\n")

spark.stop()

```

### Paso 6. Sumar métricas de RDD y captura de inconsistencias de datos

Crea el archivo `tarea8_inconsistencias.py` en tu VS Code.

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Acumuladores Inconsistencias") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# RDD ampliado a 10 registros combinando datos correctos y múltiples anomalías
rdd_usuarios = sc.parallelize([
    (1, "Ana", 25),
    (2, "Carlos", -1), # Edad inválida
    (3, "Luis", 30),
    (4, None, 40),     # Nombre nulo
    (5, "Sofía", 0),   # Edad inválida
    (6, "Fernando", 19),
    (7, "Gabriela", 31),
    (8, None, -5),     # Doble anomalía
    (9, "Hugo", 45),
    (10, "Isabel", -2) # Edad inválida
])

# Acumulador para registrar la cantidad de anomalías detectadas en la ingesta
acumulador_errores = sc.accumulator(0)

# Filtrar registros incrementando dinámicamente el acumulador si hay inconsistencias
def filtrar_y_contar_errores(registro):
    global acumulador_errores
    nombre = registro[1]
    edad = registro[2]
    
    if nombre is None or edad <= 0:
        acumulador_errores.add(1)
        return False
    return True

rdd_usuarios_validos = rdd_usuarios.filter(filtrar_y_contar_errores)

# Importante: Los acumuladores requieren de una acción (como collect) para gatillar la evaluación
resultado_final = rdd_usuarios_validos.collect()

print("\n" + "="*60)
print(f"{BOLD}{GREEN} TAREA 8.2: CONTROL DE CALIDAD E INGESTIÓN DE AUDITORÍA DE DATOS{RESET}")
print("="*60)
print(f"{BOLD} Usuarios catalogados como válidos (post-filtro):{RESET}")
for user in resultado_final:
    print(f"    ID: {user[0]} | Nombre: {user[1]:<10} | Edad: {user[2]}")

print("-" * 60)
print(f"    {BOLD}Total de registros inválidos detectados por el acumulador:{RESET} {YELLOW}{acumulador_errores.value}{RESET}")
print("="*60 + "\n")

spark.stop()

```

---


### Resultado esperado

![resultado](../curso_python_spark/images/lab7_resultado.png)


