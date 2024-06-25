# Prueba SDG

Propuesta d esolución para la prueba técnica de SDG.

Aplicación Spark Scala de validación dinámica de datos.

Arquitectura basada en contenedores de Docker compuesto por un cluster hadoop-spark, un cluster de kafka y airflow como orquestador

## Comenzando 🚀

Mira **Instalación** para conocer como instalar los componentes necesarios

Mira **Deployment** para conocer como desplegar el proyecto.


### Pre-requisitos 📋

```
Ubuntu 20.4 >
JDK 8 >
Docker
```

### Instalación 🔧

_Instalación Cluster Hadoop y Spark con dos nodos workers_

_En el directorio docker-hadoop/docker/_
```
sudo sh start.sh
```
_Una vez instalado comprobar _namenode_ information y _Spark resource manager__
```
http://localhost:50070
http://localhost:8088/cluster
```

_Instalación Cluster Kafka con Zookeper_

_En el directorio docker-kafka/_
```
sudo docker compose up -d
```

_Instalación Airflow_

_En el directorio docker-airflow/_
```
docker pull apache/airflow
sudo docker run -d -p 8090:8080 --name airflow -e LOAD_EX=y -v /SDG/docker-airflow/dags:/opt/airflow/dags apache/airflow bash -c "airflow db init && airflow webserver"
```
_Una vez finalizada la instalación. Entrar en el contenedor, usuario para airflow y arrancar scheduler_
```
docker exec -ti airflow bash
airflow users create --username admin --firstname FIRST_NAME --lastname LAST_NAME --role Admin --email admin@example.org
airflow scheduler
```
_Una vez esté todo configurado,se podrá acceder a la interfaz web de Apache Airflow_ 
```
http://localhost:8090/,
```

## Despliegue 📦

_Una vez han sido instalados todos los componentes_

_Arrancar Cluster Hadoop_
```
sudo docker start hadoop-master hadoop-slave1 hadoop-slave2
```
_Arrancar Cluster Kafka_
```
sudo docker start zookeeper kafka
```
_Arrancar Airflow_
```
sudo docker start airflow
sudo docker exec -ti airflow bash
airflow scheduler
```

## Autor ✒️
Karim Basha Gutierrez
