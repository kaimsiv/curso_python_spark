# PYT_SPARK_DEVESS

## Práctica 1. Instalación de ambiente (Spark, Python y bibliotecas)

### Objetivo

Al finalizar la práctica, serás capaz de:

* Configurar un entorno moderno de PySpark en Linux Ubuntu y ejecutar scripts de Big Data utilizando el IDE Visual Studio Code.

### Duración aproximada

* 20 minutos.

### Prerrequisitos

* Acceso a un entorno Linux Ubuntu 24.04 (local o virtual) con interfaz gráfica.
* Conexión a internet.

---

### Instrucciones

### Tarea 1. Instalar prerrequisitos del sistema (Java 17 y Pip3)

Las versiones modernas de Apache Spark requieren componentes actualizados de Java y el gestor de paquetes de Python.

#### Paso 1. Instalar Java Development Kit (JDK 17)

Abre una ventana de terminal (`Ctrl + Alt + T`) e instala Java 17 ejecutando el siguiente comando:

```bash
sudo apt update && sudo apt install -y openjdk-17-jdk

```

#### Paso 2. Configurar Java 17 como predeterminado

Para asegurar que el sistema operativo priorice esta versión, ejecuta:

```bash
sudo update-alternatives --config java

```

*Si aparece una lista de opciones, escribe el número correspondiente a `java-17-openjdk-amd64` y presiona **Enter**.*

#### Paso 3. Instalar PIP3 (Gestor de paquetes de Python)

Instala las herramientas de empaquetado de Python ejecutando:

```bash
sudo apt install -y python3-pip

```

---

### Tarea 2. Instalación directa de PySpark y FindSpark

Ya no es necesario descargar manualmente archivos `.tgz` de la web de Apache ni configurar variables de entorno en el archivo `.bashrc`. PySpark se instalará de manera directa y aislada para tu usuario.

#### Paso 1. Descargar las bibliotecas mediante PIP3

Ejecuta el siguiente comando en la terminal utilizando la bandera `--break-system-packages` para cumplir con las normativas de seguridad de los Linux modernos (PEP 668):

```bash
pip3 install pyspark findspark --break-system-packages
```

---

### Tarea 3. Configuración del entorno de desarrollo en VS Code

Utilizaremos Visual Studio Code para escribir y ejecutar de forma visual nuestros scripts de datos.

#### Paso 1. Preparar la extensión de Python

1. Abre **Visual Studio Code**.
2. Ve a la sección de **Extensiones** en la barra lateral izquierda (o presiona `Ctrl + Shift + X`).
3. Busca **Python** (desarrollada por Microsoft) y haz clic en **Instalar**.

#### Paso 2. Seleccionar el Intérprete de Python adecuado

Para evitar errores de importación de librerías dentro del editor:

1. Abre tu carpeta de trabajo en VS Code.
2. Presiona la combinación de teclas **`Ctrl + Shift + P`** para abrir la paleta de comandos.
3. Escribe y selecciona **`Python: Select Interpreter`**.
4. Elige el intérprete global del sistema (usualmente listado como `/usr/bin/python3` o **Python 3.12.x**).

---

### Tarea 4. Creación y ejecución del Script de Verificación

Para validar que el motor de Spark se comunica de forma correcta con Python, crearemos un flujo mínimo de datos.

#### Paso 1. Estructura del código base

Crea un archivo llamado `prueba_spark.py` en tu directorio de trabajo y pega el siguiente código.

> **Importante:** Las primeras líneas configuran dinámicamente la ruta de Java y localizan el motor de Spark en el sistema, eliminando la necesidad de configuraciones manuales en el sistema operativo.

```python
import os
import socket

# Forzar de manera dinámica la ruta correcta de Java 17
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

import findspark
findspark.init() # Inicializa Spark automáticamente

from pyspark.sql import SparkSession

# Obtener dinámicamente el nombre de la máquina y el usuario actual
nombre_maquina = socket.gethostname()
usuario_actual = os.getlogin()

# Iniciar la sesión local de Spark
spark = SparkSession.builder \
    .appName("ValidacionNetec") \
    .getOrCreate()

print("\n" + "="*40)
print(f"Versión de Spark detectada: {spark.version}")
print("="*40 + "\n")

# Estructurar un DataFrame de prueba de forma dinámica
datos = [(usuario_actual, "Usuario"), (nombre_maquina, "Servidor")]
columnas = ["Nombre", "Rol"]

df = spark.createDataFrame(datos, schema=columnas)

# Desplegar la información en la consola de salida
df.show()

# Finalizar ordenadamente la sesión
spark.stop()

```

#### Paso 2. Ejecución

Haz clic en el botón de **Play (▶)** ubicado en la esquina superior derecha de VS Code o presiona **`F5`**.

---

### Resultado esperado

La terminal integrada de VS Code mostrará algunos mensajes de inicialización (`WARN`) seguidos de la confirmación de la versión y la estructura de datos procesada exitosamente por el clúster local:

```text
========================================
Versión de Spark detectada: 4.1.2
========================================

+------+----------+
|Nombre|       Rol|
+------+----------+
| Kevin|Instructor|
|Arthas|  Servidor|
+------+----------+

```