import pandas as pd
import cx_Oracle
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError


class Extractor:

    def __init__(self, connection: dict):
        self.user = dict.get('USER')
        self.paswd = dict.get('PASSWD')
        self.host = dict.get('HOST')
        self.sdi = dict.get('SID')
        self.sdi1 = dict.get('SID1')
        self.port = dict.get('PORT')
        self.engine = sqlalchemy.create_engine(
            f"oracle+cx_oracle://{self.user}:{self.paswd}@{self.host}:{self.port}/?service_name={self.sdi}",
            arraysize=1000)
        print('extractor class')

    def extract(self, query):
        try:
            print('extract data on cruz verde bd')
            query_sql = query
            df_result = pd.read_sql(query_sql, self.engine)
            return df_result
        except Exception as e:
            raise SQLAlchemyError
