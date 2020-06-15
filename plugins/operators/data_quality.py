from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_names=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id=redshift_conn_id
        self.table_names=table_names

    def execute(self, context):
        """ Run checks on the data itself """
        redshift_hook=PostgresHook(self.redshift_conn_id)
        
        for table_name in self.table_names:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table_name}")
            
            num_records = records[0][0]    
            if len(records) < 1 or len(records[0]) < 1 or num_records < 1:

                raise ValueError(f"Data quality check failed, {table_name} returned no results or contains 0 rows")
                self.log.info(f"Data quality check failed. {table_name} contained 0 rows")

            self.log.info(f"Data quality on table {table_name} check passed with {num_records} records")
        self.log.info('DataQualityOperator not implemented yet')