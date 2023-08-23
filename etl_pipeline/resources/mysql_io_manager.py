import pandas as pd
from mysql.connector import connect
import pymssql

class MySQLIOManager:
    def __init__(self, config):
        self._config = config
        
    
    def extract_data(self, sql: str) -> pd.DataFrame:
        print("my sql config: ", self._config)
        db_connection = connect(host='localhost', database='demo_mysql', user='admin', password='admin', port='3306')
        df = pd.read_sql_query(sql=sql, con=db_connection)        
        return df
