from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 load_statement="",
                 delete_on_insert=False,
                 table_name="",
                 *args, **kwargs):

        super(DimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.load_statement=load_statement
        self.delete_on_insert=delete_on_insert
        self.table_name=table_name
        
    def execute(self, context):
        """  Loads data into dimension tables """
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        if delete_on_insert:
            self.log.info(f"Deleting records from {self.table_name} table")
            redshift_hook.run(f"DELETE FROM {self.table_name}")
        
        else:
            self.log.info(f"Inserting records into {self.table_name} table")
            redshift_hook.run(self.load_statement)
            self.log.info(f"Finished inserting records into {self.table_name} table!" )