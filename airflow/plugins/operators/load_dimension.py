from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
"""
LoadDimensionOperator insert the data into Dimension tables from Staging table
Note: Based on user parameter(is_append) value, it can able to delete and load data or append the data

Parameters:
        redshift_conn_id: To access redshift database
        table: name of the table
        sql: sql query to select data from staging table
        is_append: either True or False
"""
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table_name="",
                 is_append="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table_name = table_name
        self.is_append = is_append

    def execute(self, context):
        self.log.info('LoadDimensionOperator just started...')
        redshift = PostgresHook(self.redshift_conn_id)
        if self.is_append == "True":
            sql_statement = "INSERT INTO {} {}".format(self.table_name,self.sql)
            redshift.run(sql_statement)
        else:
            delete_statement = "DELETE FROM {}".format(self.table_name)
            redshift.run(delete_statement)
            insert_statement = "INSERT INTO {} {}".format(self.table_name,self.sql)
            redshift.run(insert_statement)
