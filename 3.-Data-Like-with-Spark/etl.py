import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ['REGION']=config['AWS']['REGION']

def create_session():
    spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .getOrCreate()
    print("********* SPARK****************")
    print(spark)
    return spark

def process_song_data(spark,input_data,output_data):
    print("*******Inicio********")

    song_data = input_data + "song_data/A/B/C/*.json"
    df_song = spark.read.json(song_data)
    
    #SONG TABLE
    songs_table = df_song.select("song_id", "title", "artist_id", "year", "duration").distinct()

    #SONG TABLE
    artist_table  = df_song.selectExpr("artist_id", "artist_name name", "artist_location location","artist_latitude latitude", "artist_longitude longitude")

    #GUARDAR
    path_song = output_data + "song_table.parquet"
    path_artist = output_data + "artist_table.parquet"

    songs_table.write.parquet(path=path_song,partitionBy = ("year", "artist_id"),mode = "overwrite")
    artist_table.write.parquet(path=path_artist,mode = "overwrite")
    
    print("******Se guardo correctamente*******")
    return df_song
    



def process_long_data(spark,input_data,output_data):

    log_data  = input_data + "log_data/2018/11/*.json"
    df_long = spark.read.json(log_data)

    song_data = input_data + "song_data/A/B/C/*.json"
    df_song = spark.read.json(song_data)

    #Long data
    df_long = df_long.filter(df_long.page == "NextSong")

    #Convertir ts a timeStamp
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df_long = df_long.withColumn("start_time", get_datetime(df_long.ts))

    #USER TABLE
    user_table = df_long.selectExpr("userId user_id", "firstName first_name", "lastName last_name", "gender", "level").distinct()

    #TIME TABLE
    table_time = df_long.select("start_time").distinct()

    get_hour = udf(lambda x: x.hour)  
    table_time = table_time.withColumn("hour", get_hour(table_time.start_time))

    get_day = udf(lambda x: x.day)
    table_time = table_time.withColumn("day", get_day(table_time.start_time))

    get_week = udf(lambda x: x.isocalendar()[1])
    table_time = table_time.withColumn("week", get_week(table_time.start_time))
    
    get_month = udf(lambda x: x.month)
    table_time = table_time.withColumn("month", get_month(table_time.start_time))

    get_year = udf(lambda x: x.year)
    table_time = table_time.withColumn("year", get_year(table_time.start_time))

    get_weekday = udf(lambda x: x.weekday())
    table_time = table_time.withColumn("weekday", get_weekday(table_time.start_time))

    #SONG_PLAY TABLE
    cond = [df_long.artist == df_song.artist_name,df_long.song == df_song.title]
    df_join = df_long.join(df_song, cond, "inner")

    #Se agrega Identificador a la tabla SONG_PLAY
    df_join = df_join.withColumn("songplay_id", monotonically_increasing_id())
    #Tabla SONG_TABLE
    song_play_table = df_join.selectExpr("songplay_id","start_time","userId user_id","level","song_id","artist_id","sessionId session_id","location","userAgent user_agent")

    #GUARDAR
    path_user = output_data + "user_table.parquet"
    path_time = output_data + "time_table.parquet"
    path_songPlay = output_data + "songPlay_table.parquet"

    user_table.write.parquet(path=path_user, mode = "overwrite" )
    table_time.write.parquet(path=path_time,mode = "overwrite")
    song_play_table.write.parquet(path=path_songPlay,mode = "overwrite")
    print("*******Se guardo correctamente********")





def main():
    
    spark = create_session()
    input_data  = "s3a://udacity-dend/"
    output_data = "s3a://helaine-data-lake/parquet_files/"
    
    #df_song = process_song_data(spark, input_data, output_data)    
    process_long_data(spark, input_data, output_data)    
    
    spark.stop()

if __name__ == "__main__":
    main()