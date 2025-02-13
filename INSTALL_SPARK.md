# INSTALL APACHE SPARK

Este tutorial tiene por objetivo guiar en la instalación de Apache Spark. Aunque se suele utilizar combinado con herramientas como Hadoop, vamos a omitirlo y concentranos solo en Spark.


### **1. Instalación de JDK**

Utiliza el siguiente código para instalar JDK 11

```bash
sudo apt install openjdk-11-jdk wget ssh rsync -y
```

Una vez instalado, configura la siguiente variable de entorno en el archivo ```~/.bashrc```

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

### **2. Descarga Apache Spark**

1. Ve al sitio oficial de Apache Spark: [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html).
2. Descarga la versión de Spark buscada. Selecciona una distribución preconfigurada para Hadoop (por ejemplo, Hadoop 3.x). Debes obtener un enlace con el que puedes utilizar la siguiente instrucción. 

```bash
wget https://downloads.apache.org/spark/spark-<version>/spark-<version>-bin-hadoop<version>.tgz
```

1. Descomprime el archivo descargado:

```bash
tar -xvzf spark-<version>-bin-hadoop<version>.tgz
sudo mv spark-<version>-bin-hadoop<version> /opt/spark
```

### **3. Configura las variables de entorno para Spark**

Edita tu archivo `~/.bashrc` para agregar las variables de entorno necesarias:


```bash
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
```

Recarga el archivo para aplicar los cambios:

```bash
source ~/.bashrc

```

### **4. Configura Spark**

Realiza una copia el archivo base de configuraciones y luego ábrelo.

```bash
cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
vim $SPARK_HOME/conf/spark-env.sh
```

Agrega las siguientes líneas al archivo cambiando <tamaño de memoria> por el valor de memoria a asignar a cada ejecutor. Recuerda que debes distribuir tu memoria RAM entre la requerida para: el funcionamiento de la instancia, el driver y los ejecutores de Spark. Por ejemplo, si tu computadora tiene 16GB de RAM, puedes asignar 4GB a cada ejecutor y crear dos. 

```bash
export SPARK_WORKER_MEMORY=<tamaño de memoria>g  
export SPARK_MASTER_HOST=127.0.0.1  
```

### **5. Inicia Spark**

1. Lanza el modo maestro (Master) de Spark:

```bash
start-master.sh
```

Inicia al menos un ejecutor (Worker):

```bash
start-worker.sh spark://127.0.0.1:7077
```

Verifica que Spark esté funcionando accediendo a la interfaz web en http://localhost:8080.

### **6. Prueba Spark**

1. Abre el shell interactivo de Spark:

```bash
spark-shell

```

Ejecuta un ejemplo básico para confirmar que Spark funciona:

```bash
val data = sc.parallelize(Seq(1, 2, 3, 4, 5))
val result = data.map(x => x * 2).collect()
result.foreach(println)
```
