import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from io import BytesIO
from minio import Minio

class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config
    
    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        file_name = context.asset_key
        print(file_name)
        client = Minio(
            "localhost:9000",
            access_key="minio",
            secret_key="minio123",
            secure=False
        )
        csv = obj.to_csv().encode('utf-8')
        client.put_object(
            "bronze",
            "ecom/{file_name}.csv".format(file_name=file_name),
            data=BytesIO(csv),
            length=len(csv),
            content_type='application/csv'
        )
    
    def load_input(self, context: InputContext) -> pd.DataFrame:
        file_name = context.asset_key
        print(file_name)
        client = Minio(
            "localhost:9000",
            access_key="minio",
            secret_key="minio123",
            secure=False
        )
        
        obj = client.get_object(
            "bronze",
            "ecom/{file_name}.csv".format(file_name=file_name),
        )
        
        df = pd.read_csv(obj)
        return df