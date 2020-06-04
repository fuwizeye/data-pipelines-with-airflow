from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

import configparser


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    # STAGING TABLES

    staging_events_copy = ("""
        COPY  {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION 'us-west-2'
        FORMAT AS json '{}';
    """)

    staging_songs_copy = ("""
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION 'us-west-2'
        FORMAT AS json 'auto';

    """)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table_name="",
                 s3_bucket="",
                 s3_key="",
                 file_type="",
                 file_path="",
                 
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table_name=table_name
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.file_type=file_type
        self.file_path=file_path
        self.execution_time = kwargs.get('execution_time')

    def execute(self, context):
        """ load any JSON formatted files from S3 to Amazon Redshift """
        
        aws_hook=AwsHook(self.aws_credentials_id)
        redshift_hook=PostgresHook(self.redshift_conn_id)
        credentials=aws_hook.get_credentials()
        
        self.log.info(f"Deleting existing files from {self.table_name}")
        redshift_hook.run(f"DELETE FROM {self.table_name}")
        
        s3_source = "{}/{}".format(self.s3_bucket, self.s3_key)
        
        if self.file_path != "":
            self.file_path = "{}/{}".format(self.s3_bucket, self.file_path)
            copy_query = self.staging_events_copy.format(self.table_name, s3_source, credentials.access_key, credentials.secret_key,                                        self.file_path)
        else:
            copy_query = self.staging_songs_copy.format(self.table_name, s3_source, credentials.access_key, credentials.secret_key)
            
        self.log.info(f"Copying files from S3 to {self.table_name}")
        
        redshift_hook.run(copy_query)
        
        self.log.info(f"Copying files from S3 to {self.table_name} is complete!")




