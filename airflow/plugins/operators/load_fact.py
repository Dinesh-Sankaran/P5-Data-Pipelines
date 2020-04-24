from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
"""
LoadFactOperator insert the data into fact tables from Staging table

Parameters:
        redshift_conn_id: To access redshift database
        sql: sql query to select data from staging table
"""
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        self.log.info('LoadFactOperator just started....')
        redshift = PostgresHook(self.redshift_conn_id)
        redshift.run(self.sql)
        
