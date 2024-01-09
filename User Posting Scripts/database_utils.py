import json
from datetime import datetime
import random
import sqlalchemy
from sqlalchemy import text

# Custom module for credentials
import creds

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class AWSDBConnector:

    def __init__(self):
        # RDS database connection details
        self.HOST = creds.RDS_database["HOST"]
        self.USER = creds.RDS_database["USER"]
        self.PASSWORD = creds.RDS_database["PASSWORD"]
        self.DATABASE = creds.RDS_database["DATABASE"]
        self.PORT = creds.RDS_database["PORT"]


    def create_db_connector(self):
        return sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")

def fetch_row_from_db(connection, table_name):
    random_row = random.randint(0, 11000)
    query = text(f"SELECT * FROM {table_name} LIMIT {random_row}, 1")
    selected_row = connection.execute(query)
    for row in selected_row:
        return dict(row._mapping)