# Proyecto: Data Lake
Este proyecto consiste en la construcción de un ETL pipeline para extraer datos de un S3, procesandolos usando Spark y cargandolos de regreso dentro de un S3 como un conjunto de tablas.

# Dataset
El dataset contiene dos datasets que estan almacenados en S3.
 * Song data: 
  ```
  s3://udacity-dend/song_data
  ```
 * Long data:  
  ```
  s3://udacity-dend/log_data
  ```

 ## Song dataset
 Es un conjunto de datos provenientes de [Million Song Dataset](http://millionsongdataset.com/) en formato JSON. Cada archivo contiene metadatos sobre artistas y sus canciones.


 ```
{
  "num_songs",
  "artist_id", 
  "artist_latitude",
  "artist_longitude",
  "artist_location",
  "artist_name",
  "song_id",
  "title",
  "duration",
  "year"
}
```
## Log dataset

Esté conjunto de datos contiene archivos log en formato JSON que simula registros de actividades de la aplicación Sparkify.

```
{
  "artist",
  "auth",
  "firstName",
  "gender",
  "itemInSession",
  "lastName",
  "length",
  "level",
  "location",
  "method",
  "page",
  "Registration",
  "SessionId",
  "song",
  "status",
  "ts",
  "userAgent",
  "userId"
}
```
# Tablas

### **Tabla de hechos**
**SONG_PLAY**
```
songplay_id, 
user_id, 
level, 
song_id, 
artist_id, 
session_id, 
start_time,
location, 
user_agent
```

### **Tabla de dimensiones**

**USER**
```
user_id,
first_name,
last_name,
gender,
level
```

**SONG**
```
song_id,
artist_id,
title,
year,
duration
```
**ARTIST**
```
artist_id,
name,
location,
latitude,
longitude
```
**TIME**
```
start_time,
hour,
day,
week,
month,
year,
weekday
```
# Solución

* Paso 1. Crear un usuario IAM

  Para crear un nuevo usuario ir a tu cuenta *AWS* -> *Servicios* y seleccionar IAM.
  Escribe el nombre del usuario. Habilita *programmatic access*. Agregar persimos de 
 *AmazonS3FullAccess*. Descargar el archivo *Download.csv* que contiene las credenciales.

  ![clone](images/user.webp)

  * Paso 2. Crear archivos de configuración

  Crear el archivo credentials
  ```
  $ touch ~/.aws/credentials
  ```
  Abrir el archivo y pegar la siguiente estructura, llenado los campos con el archivo descargado.
  ```
  [default]
  aws_access_key_id = YOUR_ACCESS_KEY_ID
  aws_secret_access_key = YOUR_SECRET_ACCESS_KEY

  ```
  * Paso 3. Instalar pyspark
  ```
  $ pip install pyspark
  ```
  * Paso 4. Llena el archivo de configuración dl.cfg

  ![cfg](images/dl.png)

 * Paso 5. Ejecutar el archivo etl.py

  ![dlr](images/dlR.png)
