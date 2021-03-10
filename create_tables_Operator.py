from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
from datetime import datetime
import logging


class create_tables_Operator(BaseOperator):

    @apply_defaults
    def __init__(self,redshift_conn_id,*args, **kwargs):
        super(create_tables_Operator, self).__init__(*args, **kwargs)
      
        #self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """This function has the definition necessary for creating the fact and dimension tables"""
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        #logging.info(f"Creating table {self.table}")
        #print(self.sql)
        #self.cur.execute(self.sql)
        #redshift.run(f"""{self.table} 
        #                 """)
        #logging.info(f"Created table {self.table}")
