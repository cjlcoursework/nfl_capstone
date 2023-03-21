import re
import sqlite3

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, event, engine
from sqlalchemy.engine import Connection, Engine
from dotenv import load_dotenv
import os
from dotenv import load_dotenv


class DatabaseLoader:
    connection_string: str = None
    conn: Connection = None
    engine: Engine = None
    sql_type: bool = False

    def __init__(self, connection_string_env_url=None):

        load_dotenv()
        url_value = os.getenv(connection_string_env_url)
        if url_value is None:
            url_value = connection_string_env_url
        self.connection_string = url_value

        m = re.search(r'(?P<dialect>.*)\:\/\/(?P<db>.*)\:(?P<password>.*)\@(?P<host>.*)\/(.*)', url_value)

        self.sql_type = True
        if m is None:
            self.sql_type = False
        elif len(m.groups()) == 5:
            if m['dialect'] == 'postgresql':
                self.connect_sql()
            else:
                raise Exception("unsupported dialect: ", m['dialect'])
        else:
            raise Exception("unsupported connection string: ", connection_string_env_url)

    def connect_sql(self, force_reconnect: bool = False):
        if self.conn is not None:
            if not force_reconnect:
                return self.conn
            else:
                self.conn.close()

        self.engine = create_engine(self.connection_string)
        self.conn = self.engine.connect()
        return self.conn

    def read_table(self, table_name: str) -> pd.DataFrame:
        if not self.sql_type:
            return self.read_table_csv(table_name=table_name)
        else:
            return self.read_table_sql(table_name=table_name)

    def read_table_csv(self, table_name: str) -> pd.DataFrame:
        file_name = table_name + ("" if table_name.endswith(".csv") else ".csv")
        p = os.path.join(self.connection_string, file_name)
        fullpath = os.path.abspath(p)
        return pd.read_csv(fullpath)

    def read_table_sql(self, table_name: str) -> pd.DataFrame:
        dataFrame = pd.read_sql(f"select * from \"{table_name}\"", self.conn)
        return dataFrame

    def load_table(self,
                       df: pd.DataFrame,
                       table_name: str,
                       handle_exists='replace') -> None:

        if not self.sql_type:
            self.load_table_csv(df, table_name=table_name, handle_exists=handle_exists)
        else:
            self.load_table_sql(df, table_name=table_name, handle_exists=handle_exists)

    def load_table_csv(self,
                       df: pd.DataFrame,
                       table_name: str,
                       handle_exists='replace') -> None:
        try:
            file_name = table_name + ("" if table_name.endswith(".csv") else ".csv")
            p = os.path.join(self.connection_string, file_name)
            fullpath = os.path.abspath(p)
            df.reset_index().to_csv(fullpath, index=False)
        except Exception as e:
            print(e)
            raise (e)

    def load_table_sql(self,
                   df: pd.DataFrame,
                   table_name: str,
                   handle_exists='replace') -> None:
        try:
            df.to_sql(table_name, self.engine, if_exists=handle_exists, index=False, chunksize=500, method='multi')
        except Exception as e:
            print(e)
            raise (e)

