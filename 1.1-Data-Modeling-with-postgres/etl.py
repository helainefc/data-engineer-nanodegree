import glob
import pandas as pd
from models.song import Song
from models.artist import Artist
from models.songPlay import SongPlay
from models.time import Time
from models.user import User
import numpy as np
from bd import Base,session,engine
from sqlalchemy import exc

#Función que obtiene el nombre de todos los archivos
def get_nameFiles(path):
    files = [f for f in glob.glob(path + "**/*.json", recursive=True)]
    return files

#Función que convierte el contenido de todos los archivos en un dataFrame 
def files_To_DataFrame(Filejson):
    df_full= pd.DataFrame()
    for file in Filejson:
        #Read file
        df = pd.read_json(file,lines=True,encoding='utf-8') 
        #Data Frame
        df_full = df_full.append(df)
    return df_full

def main():
    ## Se procesan los archivos 'data/log_data/'

    #Se obtienen los nombre de todos los archivos los archivos
    files = get_nameFiles('data/song_data/')

    #Se crea un dataframe con la información de todos los archivos
    df_song_data = files_To_DataFrame(files)

    #Se obtienen los datos para la tabla SONG
    df_song = df_song_data[['song_id','artist_id', 'title','year', 'duration']].dropna(subset=['song_id']).drop_duplicates(['song_id'])
    
    #Se obtienen los datos para la tabla ARTIST
    df_artist = df_song_data[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].dropna(subset=['artist_id']).drop_duplicates(['artist_id'])
    df_artist = df_artist.rename(columns={'artist_name': 'name', 'artist_location': 'location', 'artist_latitude': 'latitude', 'artist_longitude': 'longitude'})

    #Se procesan los archivos 'data/log_data/'

    #Se obtienen los nombre de todos los archivos los archivos
    files = get_nameFiles('data/log_data/')

    #Se crea un dataframe con la información de todos los archivos
    df_long_data = files_To_DataFrame(files)

    #Se obtienen los datos para la tabla TIME_FORMAT
    ts = pd.to_datetime(df_long_data['ts'], unit='ms')
    timeFormat = (df_long_data['ts'].values, ts.dt.hour.values, ts.dt.day.values, ts.dt.week.values, ts.dt.month.values, ts.dt.year.values, ts.dt.weekday.values)
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    df_time_format= pd.DataFrame(data=list(zip(*timeFormat)), columns=column_labels)
    df_time_format = df_time_format.drop_duplicates(['start_time'])

    #Se obtiene los datos para la tabla USER
    convert_dict = {'user_id': int } 
    df_user = df_long_data[['userId', 'firstName', 'lastName', 'gender', 'level']].rename(columns={'userId': 'user_id'}).replace('', np.nan).dropna(subset=['user_id'])
    df_user = df_user.astype(convert_dict)
    df_user = df_user.drop_duplicates(['user_id'])

    #Se obtiene los datos para la table SONG_PLAY
    df_song_data_aux = df_song_data[['artist_id','song_id', 'title','artist_name']].rename(columns={'title': 'song', 'artist_name': 'artist'})
    df_long_data_aux = df_long_data[['userId', 'sessionId','userAgent','level','location','ts','artist','song']]

    df_song_play_join = pd.merge(df_long_data_aux, df_song_data_aux, on=['song','artist'], how='left')
    df_song_play_join = df_song_play_join.rename(columns={'ts': 'start_time', 'sessionId': 'session_id','userAgent': 'user_agent','userId': 'user_id'}).replace('',0)

    #Se crea el indice songplay_id
    df_song_play_join['songplay_id'] = df_song_play_join.index

    #Se crean los objetos para ser guardados en BD
    listObjectSong       = [Song(**kwargs) for kwargs in df_song.to_dict(orient='records')]
    listObjectArtist     = [Artist(**kwargs) for kwargs in df_artist.to_dict(orient='records')]
    listObjectTime       = [Time(**kwargs) for kwargs in df_time_format.to_dict(orient='records')]
    listObjectUser       = [User(**kwargs) for kwargs in df_user.to_dict(orient='records')]
    listObjectSongPlay   = [SongPlay(**kwargs) for kwargs in df_song_play_join.to_dict(orient='records')]

    #Se eliminan las tablas, si existen y se crean
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    #Se guardan los objetos
    try:
        session.bulk_save_objects(listObjectSong)
        session.bulk_save_objects(listObjectArtist)
        session.bulk_save_objects(listObjectTime)
        session.bulk_save_objects(listObjectUser)
        session.bulk_save_objects(listObjectSongPlay)
        session.commit()
        session.close()
    except exc.SQLAlchemyError as ex:
        session.close()
        print('Exeption:', ex)

    df_song_play_join = df_song_play_join.drop(['song', 'artist','length'], axis=1)
    
if __name__ == "__main__":
    main()