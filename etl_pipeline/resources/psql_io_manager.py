from typing import Any
from dagster._core.execution.context.input import InputContext
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine
from psycopg2 import connect

class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):       
        conn_string = 'postgresql://admin:admin@localhost:5432/demo_pg'
        engine = create_engine(conn_string)
        conn = connect(database='demo_pg', user='admin', password='admin',port='5432')
        obj.to_sql(name='sales_values_by_category', con=engine, if_exists='replace', index=False, schema='gold')
        conn.commit()
        conn.close()
    
    def load_input(self, context: InputContext):
        pass