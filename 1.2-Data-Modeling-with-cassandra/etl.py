import glob
import pandas as pd
import numpy as np
import logging
from cassandra.cluster import Cluster
from cql_queries import *

#Función que obtiene todos los nombres de los archivos
def get_nameFiles(path):
    files = [f for f in glob.glob(path + "**/*.json", recursive=True)]
    return files

#Función que conviete el contenido de todos los archivos en un dataFrame
def files_To_DataFrame(Filejson):
    df_full= pd.DataFrame()
    for file in Filejson:
        #Read file
        df = pd.read_json(file,lines=True,encoding='utf-8') 
        #Data Frame
        df_full = df_full.append(df)
    return df_full

# Función que crea la conexión con Cassandra
def cassandra_connection():
    """
    Connection object for Cassandra
    :return: session, cluster
    """
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()
    session.execute("""CREATE KEYSPACE IF NOT EXISTS songs WITH REPLICATION ={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }""")
    session.set_keyspace('songs')
    return session, cluster

# Función que inserta los registros 
def insert_row(df,prepared,session):
    for i, row in df.iterrows():
        try:
            session.execute(prepared,list(row))
        except Exception as e:
            print('The cassandra error: {}'.format(e))
            break 

def main():
    #Se obtienen los nombre de todos los archivos los archivos
    files = get_nameFiles('data/log_data/')

    #Se crea un dataframe con la información de todos los archivos
    df_long_data = files_To_DataFrame(files)

    #Datos para tabla song_by_session_table
    df_by_session = df_long_data[['sessionId','itemInSession','artist','song','length']]
    df_by_session = df_by_session.drop_duplicates(['sessionId','itemInSession']).dropna(subset=['sessionId','itemInSession'])

    #Datos para tabla song_user_by_user_session_table
    convert_dict = {'userId': int } 
    df_by_user_session = df_long_data[['userId','sessionId','itemInSession','artist','song','firstName']]
    df_by_user_session = df_by_user_session.replace('', np.nan).dropna(subset=['userId','sessionId','itemInSession'])
    df_by_user_session = df_by_user_session.drop_duplicates(['userId','sessionId','itemInSession'])
    df_by_user_session = df_by_user_session.astype(convert_dict)

    #Datos para tabla song_by_title_table
    df_by_title = df_long_data[['song','userId','firstName','lastName']]
    df_by_title = df_by_title.replace('', np.nan).dropna(subset=['song','userId'])
    df_by_title = df_by_title.drop_duplicates(['song','userId'])
    df_by_title = df_by_title.astype(convert_dict)

    logging.basicConfig(level=logging.INFO)

    # Se crea la conexión con Cassandra
    session, cluster = cassandra_connection()

    try:
        # Se eliminan las tablas, si existen
        for table_drop in drop_table_queries:
            session.execute(table_drop)
        # Se crean las tablas 
        for table_create in create_table_queries:
            session.execute(table_create)
        # Insertar valores en la tabla song_by_session
        prepared = session.prepare(song_by_session_table_insert)
        insert_row(df_by_session,prepared)
        # Insertar valores en la tabla song_user_by_user_session
        prepared = session.prepare(song_user_by_use_session_insert)
        insert_row(df_by_user_session,prepared)
        # Insertar valores en la tabla song_by_title
        prepared = session.prepare(song_by_title_table_insert)
        insert_row(df_by_title,prepared)
    except Exception as e:
        print('The cassandra error: {}'.format(e))
    finally:
        #Se cierra la conexión
        session.shutdown()
        cluster.shutdown()

if __name__ == "__main__":
    main()


