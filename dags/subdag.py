from airflow import DAG
from airflow.operators import DimensionOperator

from datetime import datetime

def load_dimension_tables_dag(
        parent_dag,
        task_id,
        redshift_conn_id,
        sql_statement,
        delete_on_insert,
        table,
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag}.{task_id}",
        **kwargs
    )
    
    load_dimension_table = DimensionOperator(
        task_id=task_id,
        redshift_conn_id=redshift_conn_id,
        load_statement=sql_statement,
        delete_on_insert=delete_on_insert,
        table=table
    
    )
    
    return dag
    

    
    
  