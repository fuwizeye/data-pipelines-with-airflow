from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                DimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from subdag import load_dimension_tables_dag

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

parent_dag='udac_example_dag'

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    
}

dag = DAG(parent_dag,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift"
    
        )

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    table_name='staging_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket="s3://udacity-dend",
    s3_key="log-data",
    file_type="JSON",
    file_path="log_json_path.json",
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table_name='staging_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket="s3://udacity-dend",
    s3_key="song_data",
    file_type="JSON",
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    sql_statement=SqlQueries.songplay_table_insert
)

user_dimension_task_id = 'load_user_dimension_table'
load_user_dimension_table = SubDagOperator(
    subdag=load_dimension_tables_dag(
        parent_dag,
        user_dimension_task_id,
        "redshift",
        start_date=default_args["start_date"],
        sql_statement=SqlQueries.user_table_insert,
        delete_on_insert=True,
        table="users"         
    ),
    task_id=user_dimension_task_id,
    dag=dag,
)

song_dimension_task_id='load_song_dimension_table'
load_song_dimension_table = SubDagOperator(
    subdag=load_dimension_tables_dag(
        parent_dag,
        song_dimension_task_id,
        "redshift",
        start_date=default_args["start_date"],
        sql_statement=SqlQueries.song_table_insert,
        delete_on_insert=True,
        table="songs",         
    ),
    task_id=song_dimension_task_id,
    dag=dag,
)

artist_dimension_task_id='load_artist_dimension_table'
load_artist_dimension_table = SubDagOperator(
    subdag=load_dimension_tables_dag(
        parent_dag,
        artist_dimension_task_id,
        "redshift",
        start_date=default_args["start_date"],
        sql_statement=SqlQueries.artist_table_insert,
        delete_on_insert=True,
        table="artists",         
    ),
    task_id=artist_dimension_task_id,
    dag=dag,
)

time_dimension_task_id='load_time_dimension_table'
load_time_dimension_table = SubDagOperator(
    subdag=load_dimension_tables_dag(
        parent_dag,
        time_dimension_task_id,
        "redshift",
        start_date=default_args["start_date"],
        sql_statement=SqlQueries.time_table_insert,
        delete_on_insert=True,
        table="time",         
    ),
    task_id=time_dimension_task_id,
    dag=dag,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table_names = ["songs", "songplays", "users", "artists", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task dependencies
start_operator >> create_tables_task
create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift  >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator






