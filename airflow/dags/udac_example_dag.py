#Import all necessary packages and supporting files
from datetime import datetime, timedelta
import datetime
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


#Default arguments specified for DAG
default_args = {
    'owner': 'udacity',
    'start_date': datetime.datetime(2018, 11, 1, 0, 0, 0, 0),
    'start_date': datetime.datetime(2018, 11, 30, 0, 0, 0, 0),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

#Default arguments passed to DAG run and schedule interval set to run every hour
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
      )

#Dummy operator for Begin_execution task
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#Create table task which executes all create statement specified in sql files
create_tables = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

#Staging task(event) which pass parameters to StageToRedshiftOperator and copy files from S3 bucket to staging_events table
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}",
    json_format="s3://udacity-dend/log_json_path.json"
)

#Staging task(songs) which pass parameters to StageToRedshiftOperator and copy files from S3 bucket to staging_songs
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Staging_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A",
    json_format="auto"
)

#Fact Task: It pass paramters to LoadFactOperator and insert data from staging table to fact table(songplays)
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    sql=SqlQueries.songplay_table_insert,
    redshift_conn_id="redshift"      
)

#Dim task: It pass paramters to LoadDimensionOperator, delete and insert data from staging table to dim table(users)
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    sql=SqlQueries.user_table_insert,
    redshift_conn_id="redshift",
    table_name="users",
    is_append="False"
)

#Dim task: It pass paramters to LoadDimensionOperator, delete and insert data from staging table to dim table(songs)
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    sql=SqlQueries.song_table_insert,
    redshift_conn_id="redshift",
    table_name="songs",
    is_append="False"
)

#Dim task: It pass paramters to LoadDimensionOperator and append data from staging table to dim table(artists)
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    sql=SqlQueries.artist_table_insert,
    redshift_conn_id="redshift",
    table_name="artists",
    is_append="True"
)

#Dim task: It pass paramters to LoadDimensionOperator, delete and insert data from staging table to dim table(time)
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    sql=SqlQueries.time_table_insert,
    redshift_conn_id="redshift",
    table_name="time",
    is_append="False"
)

#Data quality task: It pass all the table names to DataQualityOperator and ensure data inserted into actual table or not
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    table_names = ["staging_events","staging_songs","songplays","artists","time","songs","users"]
)

#Dummy operator task for end execution 
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Defined dependencies between operator and task
start_operator >> create_tables >> [stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator

