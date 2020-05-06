
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_table import LoadTableOperator
from operators.data_quality import DataQualityOperator

from helpers.sql_queries import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 4, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG('udacity_music_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
         )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#Stage_events
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    redshift_conn='redshift',
    aws_credentials='aws_credentials',
    table='staging_events',
    s3_src_bucket='udacity-dend',
    s3_src_pattern='log_data',
    jsonpaths='s3://udacity-dend/log_json_path.json',
    data_format='json'
)
#Stage_song
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    redshift_conn='redshift',
    aws_credentials='aws_credentials',
    table='staging_song',
    s3_src_bucket='udacity-dend',
    s3_src_pattern='song_data/A/A/A',
    data_format='json'
)

#SONG_PLAY
load_songplays_table = LoadTableOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn='redshift',
    table="songplay",
    sql_select_stmt = SqlQueries.songplay_table_insert,
    append_data=True,
)

#USERS
load_users_table = LoadTableOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    redshift_conn='redshift',
    table="users",
    sql_select_stmt=SqlQueries.user_table_insert,
    append_data=True,
)
#SONG
load_songs_table = LoadTableOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn='redshift',
    table="song",
    sql_select_stmt=SqlQueries.song_table_insert,
    append_data=True,
)
#ARTIST
load_artist_table = LoadTableOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn='redshift',
    table="artist",
    sql_select_stmt=SqlQueries.artist_table_insert,
    append_data=True,
)
#TIME
load_time_table = LoadTableOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn='redshift',
    table="time",
    sql_select_stmt=SqlQueries.time_table_insert,
    append_data=True,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables=['songplay', 'users', 'song', 'artist', 'time'],
    redshift_conn='redshift'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift  >> load_songplays_table



load_songplays_table  >> load_users_table >> run_quality_checks
load_songplays_table  >> load_songs_table >> run_quality_checks
load_songplays_table  >> load_artist_table >> run_quality_checks
load_songplays_table  >> load_time_table >> run_quality_checks


run_quality_checks >> end_operator
