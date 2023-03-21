import os
import unittest

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from database_loader import DatabaseLoader

# todo - use mocks for database engine


class DatabaseTest(unittest.TestCase):

    def setUp(self) -> None:
        load_dotenv()

    def test_sql_raw(self):
        data_df = pd.DataFrame([
            {'system_time': pd.to_datetime('2022-03-04T07:52:26Z'), 'status': np.NaN, 'id': 772,
             'date': '22-03-04 07:52:26+00:00'},
            {'system_time': pd.to_datetime('2022-06-22T17:52:42Z'), 'status': 'Pending', 'id': np.NaN,
             'date': '22-03-04 07:52:26+00:00'},
            {'system_time': pd.to_datetime('2022-08-13T01:34:44Z'), 'status': 'Pending', 'id': 1052,
             'date': '22-08-13 01:34:44+00:00'},
            {'system_time': pd.to_datetime('2022-08-24T01:46:31.115Z'), 'status': 'Pending', 'id': 1052,
             'date': '22-08-24 01:46:31.115000+00:00'},
            {'system_time': pd.to_datetime('2022-08-14T06:04:54.736Z'), 'status': 'Complete', 'id': 1053,
             'date': '22-08-14 06:04:54.736000+00:00'},
            {'system_time': pd.to_datetime('2022-03-04T17:51:15.025Z'), 'status': 'Incomplete', 'id': 772,
             'date': '22-03-04 17:51:15.025000+00:00'},
            {'system_time': pd.to_datetime('2022-08-24T06:24:54.736Z'), 'status': 'Pending', 'id': np.NaN,
             'date': '22-08-24 06:24:54.736000+00:00'}])

        before_shape = data_df.shape
        db = DatabaseLoader(connection_string_env_url="postgresql://postgres:chinois1@localhost/postgres")
        db.load_table(data_df, "nfl_test1")
        df2 = db.read_table("nfl_test1")

        assert before_shape == df2.shape

    def test_sql_env(self):
        data_df = pd.DataFrame([
            {'system_time': pd.to_datetime('2022-03-04T07:52:26Z'), 'status': np.NaN, 'id': 772,
             'date': '22-03-04 07:52:26+00:00'},
            {'system_time': pd.to_datetime('2022-06-22T17:52:42Z'), 'status': 'Pending', 'id': np.NaN,
             'date': '22-03-04 07:52:26+00:00'},
            {'system_time': pd.to_datetime('2022-08-13T01:34:44Z'), 'status': 'Pending', 'id': 1052,
             'date': '22-08-13 01:34:44+00:00'},
            {'system_time': pd.to_datetime('2022-08-24T01:46:31.115Z'), 'status': 'Pending', 'id': 1052,
             'date': '22-08-24 01:46:31.115000+00:00'},
            {'system_time': pd.to_datetime('2022-08-14T06:04:54.736Z'), 'status': 'Complete', 'id': 1053,
             'date': '22-08-14 06:04:54.736000+00:00'},
            {'system_time': pd.to_datetime('2022-03-04T17:51:15.025Z'), 'status': 'Incomplete', 'id': 772,
             'date': '22-03-04 17:51:15.025000+00:00'},
            {'system_time': pd.to_datetime('2022-08-24T06:24:54.736Z'), 'status': 'Pending', 'id': np.NaN,
             'date': '22-08-24 06:24:54.736000+00:00'}])

        before_shape = data_df.shape
        db = DatabaseLoader(connection_string_env_url="DB_CONNECTION_URL")
        db.load_table(data_df, "nfl_test1")
        df2 = db.read_table("nfl_test1")

        assert before_shape == df2.shape

    def test_csv(self):
        data_df = pd.DataFrame([
            {'system_time': pd.to_datetime('2022-03-04T07:52:26Z'), 'status': np.NaN, 'id': 772,
             'date': '22-03-04 07:52:26+00:00'},
            {'system_time': pd.to_datetime('2022-06-22T17:52:42Z'), 'status': 'Pending', 'id': np.NaN,
             'date': '22-03-04 07:52:26+00:00'},
            {'system_time': pd.to_datetime('2022-08-13T01:34:44Z'), 'status': 'Pending', 'id': 1052,
             'date': '22-08-13 01:34:44+00:00'},
            {'system_time': pd.to_datetime('2022-08-24T01:46:31.115Z'), 'status': 'Pending', 'id': 1052,
             'date': '22-08-24 01:46:31.115000+00:00'},
            {'system_time': pd.to_datetime('2022-08-14T06:04:54.736Z'), 'status': 'Complete', 'id': 1053,
             'date': '22-08-14 06:04:54.736000+00:00'},
            {'system_time': pd.to_datetime('2022-03-04T17:51:15.025Z'), 'status': 'Incomplete', 'id': 772,
             'date': '22-03-04 17:51:15.025000+00:00'},
            {'system_time': pd.to_datetime('2022-08-24T06:24:54.736Z'), 'status': 'Pending', 'id': np.NaN,
             'date': '22-08-24 06:24:54.736000+00:00'}])

        dir_name = "../../../data/interim"
        file_name = "nfl_test1"
        full_path = os.path.join(dir_name,file_name+".csv")

        before_shape = data_df.shape
        db = DatabaseLoader(connection_string_env_url=dir_name)
        db.load_table(data_df, file_name)
        df2 = db.read_table(file_name)

        assert os.path.exists(full_path)
        os.remove(path=full_path)
        assert before_shape[0] == df2.shape[0]


    def test_csv_env(self):
        data_df = pd.DataFrame([
            {'system_time': pd.to_datetime('2022-03-04T07:52:26Z'), 'status': np.NaN, 'id': 772,
             'date': '22-03-04 07:52:26+00:00'},
            {'system_time': pd.to_datetime('2022-06-22T17:52:42Z'), 'status': 'Pending', 'id': np.NaN,
             'date': '22-03-04 07:52:26+00:00'},
            {'system_time': pd.to_datetime('2022-08-13T01:34:44Z'), 'status': 'Pending', 'id': 1052,
             'date': '22-08-13 01:34:44+00:00'},
            {'system_time': pd.to_datetime('2022-08-24T01:46:31.115Z'), 'status': 'Pending', 'id': 1052,
             'date': '22-08-24 01:46:31.115000+00:00'},
            {'system_time': pd.to_datetime('2022-08-14T06:04:54.736Z'), 'status': 'Complete', 'id': 1053,
             'date': '22-08-14 06:04:54.736000+00:00'},
            {'system_time': pd.to_datetime('2022-03-04T17:51:15.025Z'), 'status': 'Incomplete', 'id': 772,
             'date': '22-03-04 17:51:15.025000+00:00'},
            {'system_time': pd.to_datetime('2022-08-24T06:24:54.736Z'), 'status': 'Pending', 'id': np.NaN,
             'date': '22-08-24 06:24:54.736000+00:00'}])

        env_name = "DB_TEST_FILENAME_URL"
        table_name = "nfl_test"

        before_shape = data_df.shape
        db = DatabaseLoader(connection_string_env_url=env_name)
        db.load_table(data_df, table_name)
        df2 = db.read_table(table_name)

        load_dotenv()
        dir = os.getenv(env_name)
        file_name = table_name + ("" if table_name.endswith(".csv") else ".csv")
        p = os.path.join(dir, file_name)
        fullpath = os.path.abspath(p)

        assert os.path.exists(fullpath)
        os.remove(path=fullpath)
        assert before_shape[0] == df2.shape[0]