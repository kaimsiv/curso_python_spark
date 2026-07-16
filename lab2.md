
# PYT_SPARK_DEVESS

## Práctica 2. Creación de RDD en PySpark

### Objetivo

Al finalizar la práctica, se espera que el estudiante sea capaz de iniciar la carga de datos en RDD a partir de diferentes fuentes de información directamente desde Visual Studio Code utilizando rutas relativas locales.


### Objetivo visual

Se espera que el estudiante observe de forma clara la relación entre la actividad propuesta y el resultado que debe obtener al ejecutar los pasos del laboratorio.
### Duración aproximada:

* 45 minutos.

### Prerrequisitos

* Haber completado la instalación del ambiente simplificado en VS Code (Práctica 1).
* Contar con el entorno de Java 17 activo y las librerías `pyspark` y `findspark` instaladas.
---


### Instrucciones

Se describen los pasos requeridos para completar la práctica de forma ordenada y coherente.

## Tarea 1. Crear y configurar SparkSession y SparkContext

En las versiones modernas de Spark, **no se recomienda instanciar `SparkContext` directamente** mediante `SparkContext(conf)`. En su lugar, utilizamos `SparkSession` como interfaz unificada. Ella se encarga de gestionar de forma transparente el contexto, el motor de SQL y la conectividad.

#### Paso 1. Configuración de una aplicación básica en VS Code

Crea un archivo en tu carpeta de trabajo llamado `tarea1_basico.py` y pega el siguiente código para inicializar tu sesión local asignando 2 núcleos de tu procesador (`local[2]`):

```python
import os
# 1. Configuración dinámica del entorno
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

# 2. Inicializar la SparkSession con 2 núcleos y nombre de aplicación
spark = SparkSession.builder \
    .master("local[2]") \
    .appName("MiPrimeraAplicacionSpark") \
    .getOrCreate()

print("\n" + "="*40)
print(f"Sesión activa: {spark}")
print("="*40 + "\n")

# 3. Detener ordenadamente la sesión
spark.stop()
```

#### Paso 2. Configuración avanzada y control de recursos

Si necesitas limitar el consumo de memoria del Driver o del Executor, o definir un número específico de hilos distribuidos, puedes pasar parámetros `.config()`. Crea el archivo `tarea1_avanzado.py`:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

# Inicializar SparkSession optimizada unificando el contexto anterior
spark = SparkSession.builder \
    .appName("MiAppAvanzada") \
    .master("local[*]") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.cores.max", "4") \
    .getOrCreate()

print("\n" + "="*40)
print("SparkSession avanzada inicializada con éxito.")
print("="*40 + "\n")

spark.stop()
```

---

## Tarea 2. Crear un RDD a partir de conjuntos en memoria

Un RDD (Resilient Distributed Dataset) representa una colección inmutable de elementos que se procesa en paralelo. Usaremos el método `parallelize` expuesto a través del componente `.sparkContext` de nuestra sesión unificada.

#### Paso 3. RDD Vacío y RDD con Rangos Numéricos

Crea un archivo llamado `tarea2_conjuntos.py`:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

# Inicializar Spark
spark = SparkSession.builder.master("local[*]").appName("RDDListasTuplas").getOrCreate()
sc = spark.sparkContext

# --- MEJORA 1: Silenciar los molestos mensajes de WARNING ---
sc.setLogLevel("ERROR")

# Paleta de colores ANSI para una terminal llamativa
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RESET = "\033[0m"
BOLD = "\033[1m"

# ==========================================
# 1. RDD DE PRODUCTOS (Lista nativa)
# ==========================================
datos_lista = ["Laptop", "Impresora", "Teclado", "Memoria"]
rdd_productos = sc.parallelize(datos_lista)

print("\n" + "="*50)
print(f"{BOLD}{CYAN} SECCIÓN 1: RDD DE PRODUCTOS SIMPLES{RESET}")
print("="*50)
print(f"{BOLD} Elementos recuperados:{RESET}")
for producto in rdd_productos.collect():
    print(f"  • {producto}")

# ==========================================
# 2. RDD DE PERSONAS (Lista de Tuplas)
# ==========================================
datos_tuplas = [("Ana", 25), ("Berta", 30), ("Carolina", 35)]
rdd_personas = sc.parallelize(datos_tuplas)

print("\n" + "="*50)
print(f"{BOLD}{GREEN} SECCIÓN 2: RDD DE ESTRUCTURAS COMPLEJAS (TUPLAS){RESET}")
print("="*50)

# Mostrar como una tabla ordenada usando Spark DataFrames para aprovechar su diseño
print(f"{BOLD} Vista Estructurada:{RESET}")
df_personas = spark.createDataFrame(rdd_personas, ["Nombre", "Edad"])
df_personas.show(truncate=False)

# Metadatos del objeto
print("-" * 50)
print(f"{BOLD}ℹ  Información del Sistema:{RESET}")
print(f"   Type: {YELLOW}{type(rdd_personas)}{RESET}")
print("="*50 + "\n")

spark.stop()
```

#### Paso 4. RDD a partir de Colecciones (Listas y Tuplas de Python)

Crea un archivo llamado `tarea2_listas.py`:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

# Inicializar Spark
spark = SparkSession.builder.master("local[*]").appName("RDDArchivos").getOrCreate()
sc = spark.sparkContext

# --- Configuración para limpiar la pantalla de avisos innecesarios ---
sc.setLogLevel("ERROR")

# Estilos visuales para la terminal
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"


# ==========================================
# 1. Leer un único archivo plano (salesrpt.txt)
# ==========================================
text_rdd = sc.textFile("data/salesrpt.txt")

print("\n" + "="*50)
print(f"{BOLD}{CYAN} SECCIÓN 1: CONTENIDO DEL ARCHIVO DE VENTAS{RESET}")
print("="*50)

# Imprimir fila por fila para que se vea como una tabla real
for linea in text_rdd.collect():
    print(f"  {linea}")

print("-" * 50)
print(f" {BOLD}Número total de líneas:{RESET} {YELLOW}{text_rdd.count()}{RESET}")
print("="*50)


# ==========================================
# 2. Leer múltiples archivos (*.txt)
# ==========================================
multi_rdd = sc.textFile("data/dirtxt/*.txt")

print("\n" + "="*50)
print(f"{BOLD}{GREEN} SECCIÓN 2: CONTENIDO UNIFICADO DE LA CARPETA{RESET}")
print("="*50)

# Imprimir ordenadamente todas las líneas consolidadas
for linea in multi_rdd.collect():
    print(f"  {linea}")

print("-" * 50)
print(f" {BOLD}Total líneas agregadas de la carpeta:{RESET} {YELLOW}{multi_rdd.count()}{RESET}")
print("="*50)


# ==========================================
# 3. Vista previa controlada (Muestra)
# ==========================================
print(f"\n {BOLD}Muestra de las primeras 2 líneas procesadas:{RESET}")
for linea in multi_rdd.take(2):
    print(f"    > {linea}")
print()

spark.stop()
```

---

## Tarea 3. Crear un RDD a partir de fuentes externas (Archivos)

#### Paso 5. Preparar el entorno de datos local

Asegúrate de estar en tu ruta actual `~/1python/netec` en la terminal de VS Code y ejecuta los siguientes comandos para estructurar la carpeta interna de datos de prueba:

```bash
# 1. Crear la estructura de carpetas
mkdir -p data/dirtxt

# 2. Crear un archivo de ventas más grande (10 registros con ID, Producto y Cantidad)
echo -e "id,producto,cantidad\n1,Laptop,15\n2,Teclado,45\n3,Impresora,8\n4,Memoria,120\n5,Monitor,22\n6,Mouse,85\n7,Audifonos,60\n8,Disco_Duro,40\n9,Cargador,30\n10,Cable_HDMI,95" > data/salesrpt.txt

# 3. Crear el reporte del primer trimestre (10 meses/registros simulados con metas)
echo -e "mes,meta_ventas,alcanzado\nEnero,100,105\nFebrero,150,140\nMarzo,200,210\nAbril,250,245\nMayo,180,190\nJunio,220,230\nJulio,170,165\nAgosto,210,215\nSeptiembre,190,185\nOctubre,240,250" > data/dirtxt/enero.txt

# 4. Crear otro archivo con 10 registros de ciudades y sus ventas totales
echo -e "ciudad,ventas_totales,vendedores\nMadrid,5000,12\nBarcelona,4200,9\nValencia,2800,6\nSevilla,3100,7\nBilbao,2500,4\nZaragoza,1900,3\nMalaga,2900,5\nMurcia,1500,3\nPalma,2100,4\nLas_Palmas,1800,3" > data/dirtxt/marzo.txt
```

#### Paso 6. Lectura usando rutas relativas

Crea un archivo llamado `tarea3_archivos.py` en tu VS Code:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

# Inicializar Spark
spark = SparkSession.builder.master("local[*]").appName("RDDArchivos").getOrCreate()
sc = spark.sparkContext

# --- Configuración para limpiar la pantalla de avisos innecesarios ---
sc.setLogLevel("ERROR")

# Estilos visuales para la terminal
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"


# ==========================================
# 1. Leer un único archivo plano (salesrpt.txt)
# ==========================================
text_rdd = sc.textFile("data/salesrpt.txt")

print("\n" + "="*50)
print(f"{BOLD}{CYAN} SECCIÓN 1: CONTENIDO DEL ARCHIVO DE VENTAS{RESET}")
print("="*50)

# Imprimir fila por fila para que se vea como una tabla real
for linea in text_rdd.collect():
    print(f"  {linea}")

print("-" * 50)
print(f" {BOLD}Número total de líneas:{RESET} {YELLOW}{text_rdd.count()}{RESET}")
print("="*50)


# ==========================================
# 2. Leer múltiples archivos (*.txt)
# ==========================================
multi_rdd = sc.textFile("data/dirtxt/*.txt")

print("\n" + "="*50)
print(f"{BOLD}{GREEN} SECCIÓN 2: CONTENIDO UNIFICADO DE LA CARPETA{RESET}")
print("="*50)

# Imprimir ordenadamente todas las líneas consolidadas
for linea in multi_rdd.collect():
    print(f"  {linea}")

print("-" * 50)
print(f" {BOLD}Total líneas agregadas de la carpeta:{RESET} {YELLOW}{multi_rdd.count()}{RESET}")
print("="*50)


# ==========================================
# 3. Vista previa controlada (Muestra)
# ==========================================
print(f"\n {BOLD}Muestra de las primeras 2 líneas procesadas:{RESET}")
for linea in multi_rdd.take(2):
    print(f"    > {linea}")
print()

spark.stop()
```

---

## Tarea 4. Inspeccionar y parsear un RDD estructurado (CSV)

#### Paso 7. Generar el CSV de prueba en tu directorio actual

Ejecuta esto en tu terminal integrada para añadir el archivo delimitado directamente a tu subcarpeta `data/`:

```bash
echo -e "id,cliente,edad\n1,Alicia,25\n2,Bernardo,30\n3,Carla,35\n4,Daniel,28\n5,Elena,42\n6,Fernando,19\n7,Gabriela,31\n8,Hugo,50\n9,Isabel,24\n10,Jorge,37" > data/clientes.csv
```

#### Paso 8. Parseo funcional del RDD

Crea un archivo llamado `tarea4_parseo.py` en VS Code:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

# Inicializar Spark
spark = SparkSession.builder.master("local[*]").appName("ParseoCSV").getOrCreate()
sc = spark.sparkContext

# --- Configuración para limpiar la pantalla de avisos de Spark ---
sc.setLogLevel("ERROR")

# Estilos visuales para la terminal
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Leer el CSV utilizando la ruta relativa local
rdd_raw = sc.textFile("data/clientes.csv")

# Extraer el encabezado de forma aislada
header = rdd_raw.first()

# Filtrar para omitir la cabecera y mapear la segmentación por comas
rdd_data = rdd_raw.filter(lambda line: line != header)
rdd_split = rdd_data.map(lambda line: line.split(","))


# ==========================================
# VISTA DE REGISTROS PARSEADOS
# ==========================================
print("\n" + "="*60)
print(f"{BOLD}{CYAN} RDD ESTRUCTURADO EN MATRICES DISCRETAS (ARRAYS){RESET}")
print("="*60)

print(f"{BOLD} Encabezado omitido:{RESET} {YELLOW}[{header}]{RESET}\n")
print(f"{BOLD} Lista de registros transformados (Fila por Fila):{RESET}")

# Recorrer las listas internas para que no se amontonen en una sola línea de texto
for registro in rdd_split.collect():
    print(f"   {registro}")

print("-" * 60)


# ==========================================
# VISTA PREVIA DEL PRIMER ELEMENTO
# ==========================================
print(f" {BOLD}Primer registro parseado individualmente:{RESET}")
print(f"    {GREEN}{rdd_split.first()}{RESET}")
print("="*60 + "\n")

spark.stop()
```

---

## Tarea 5. Persistencia y Exportación de Datos (Guardar RDDs)

#### Paso 9. Configuración de almacenamiento local

Crea la carpeta de salidas directamente en tu directorio de trabajo actual:

```bash
mkdir -p salidas/
```

#### Paso 10. Conversión a DataFrame para formatos estructurados

Crea el script `tarea5_exportar.py`:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import findspark
findspark.init()

from pyspark.sql import SparkSession

# Inicializar Spark
spark = SparkSession.builder.master("local[*]").appName("ExportarResultados").getOrCreate()
sc = spark.sparkContext

# --- Configuración para limpiar la pantalla de avisos de Spark ---
sc.setLogLevel("ERROR")

# Estilos visuales alineados con el laboratorio
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# 1. Crear RDD base con registros tipados
datos_origen = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
rdd = sc.parallelize(datos_origen)

# 2. Convertir el RDD directamente a DataFrame definiendo las columnas del esquema
df = spark.createDataFrame(rdd, schema=["nombre", "id"])


# ==========================================
# VISTA DEL ESQUEMA DEL DATAFRAME
# ==========================================
print("\n" + "="*60)
print(f"{BOLD}{CYAN} TAREA 5: ESTRUCTURA DEL DATAFRAME PARA PERSISTENCIA{RESET}")
print("="*60)

print(f"{BOLD} Esquema detectado (Schema):{RESET}")
df.printSchema()

print("-" * 60)


# ==========================================
# EXPORTACIÓN Y PERSISTENCIA DE DATOS
# ==========================================
print(f" {BOLD}Iniciando exportación de formatos locales...{RESET}")

# Escritura en múltiples formatos utilizando rutas locales relativas
df.write.mode("overwrite").parquet("salidas/datos_parquet")
print(f"   {GREEN}[OK]{RESET} Formato columnar optimizado -> {YELLOW}salidas/datos_parquet{RESET}")

df.write.mode("overwrite").json("salidas/datos_json")
print(f"   {GREEN}[OK]{RESET} Formato semiestructurado    -> {YELLOW}salidas/datos_json{RESET}")

df.write.mode("overwrite").csv("salidas/datos_csv")
print(f"   {GREEN}[OK]{RESET} Formato delimitado estándar -> {YELLOW}salidas/datos_csv{RESET}")

print("-" * 60)
print(f" {BOLD}{GREEN}¡Archivos exportados exitosamente dentro de 'salidas/'!{RESET}")
print("="*60 + "\n")

spark.stop()
```

* **`collect()` vs `take(n)`:** Recuerda que `.collect()` extrae la totalidad de los datos distribuidos en los nodos de ejecución y los centraliza en el programa Driver (tu máquina). Utilízalo únicamente con muestras reducidas. Para grandes volúmenes de registros en producción, utiliza `.take(n)` o `.first()`.
* **Inmutabilidad:** Un RDD nunca es modificado in-place. Cualquier transformación (`map`, `filter`) genera una nueva instancia de RDD derivado en el grafo de linaje de ejecución.

### Resultado esperado

Se espera que el estudiante complete los pasos de la práctica y obtenga una salida coherente en la terminal o en los archivos generados, según corresponda al laboratorio.
