import unittest
import doctest
import numpy as np
import pandas as pd

from src.features.wrangling import get_metrics

"""
Todo - just some basic test - needs more
"""


class GetTest(unittest.TestCase):
    """Utility class to gather metrics
    The idea is that we want to get parameters from different pandas functions
    such as describe(), unique()
    for each column all in one place
    And... we want to set some configurations to each column as well, such as
        - whether we think it's:
            * a categorical column
            * a continuous column tha might have some distribution characteristics
            * a "key" - that is a category that acts as a primary key
            * analytics
    """

    def setUp(self) -> None:
        pass

    def test_metrics(self):
        data_df = pd.DataFrame([
            {'system_time': '2022-03-04T07:52:26Z', 'status': np.NaN,       'id': 772, 'date': '22-03-04 07:52:26+00:00'},
            {'system_time': '2022-06-22T17:52:42Z', 'status': 'Pending',    'id': np.NaN, 'date': '22-03-04 07:52:26+00:00'},
            {'system_time': '2022-08-13T01:34:44Z', 'status': 'Pending',    'id': 1052, 'date': '22-08-13 01:34:44+00:00'},
            {'system_time': '2022-08-24T01:46:31.115Z', 'status': 'Pending', 'id': 1052, 'date': '22-08-24 01:46:31.115000+00:00'},
            {'system_time': '2022-08-14T06:04:54.736Z', 'status': 'Complete', 'id': 1053, 'date': '22-08-14 06:04:54.736000+00:00'},
            {'system_time': '2022-03-04T17:51:15.025Z', 'status': 'Incomplete', 'id': 772, 'date': '22-03-04 17:51:15.025000+00:00'},
            {'system_time': '2022-08-24T06:24:54.736Z', 'status': 'Pending', 'id': np.NaN, 'date': '22-08-24 06:24:54.736000+00:00'}])
        print(data_df.shape)

        df2 = get_metrics.GetMetrics().get_metrics(data_df, None).reset_index()

        def do_assert(column_name: str, missing, good, unique):
            g = df2.loc[df2['column_name'] == column_name]
            assert g['missing_count'].item() == missing
            assert g['good_count'].item() == good
            assert g['unique_counts'].item() == unique

        do_assert('status', 1, 6, 3)
        do_assert('id', 2, 5, 3)
        do_assert('system_time', 0, 7, 7)
        do_assert('system_time', 0, 7, 7)


    def test_update2(self):
        data_df = pd.DataFrame([
            {'system_time': '2022-03-04T07:52:26Z', 'status': np.NaN, 'id': 772, 'date': '22-03-04 07:52:26+00:00'},
            {'system_time': '2022-06-22T17:52:42Z', 'status': 'Pending', 'id': 963, 'date': '22-06-22 17:52:42+00:00'},
            {'system_time': '2022-08-13T01:34:44Z', 'status': 'Pending', 'id': 1052, 'date': '22-08-13 01:34:44+00:00'},
            {'system_time': '2022-08-24T01:46:31.115Z', 'status': 'Pending', 'id': 1052, 'date': '22-08-24 01:46:31.115000+00:00'},
            {'system_time': '2022-08-14T06:04:54.736Z', 'status': 'Pending', 'id': 1053, 'date': '22-08-14 06:04:54.736000+00:00'},
            {'system_time': '2022-03-04T17:51:15.025Z', 'status': 'Pending', 'id': 772, 'date': '22-03-04 17:51:15.025000+00:00'},
            {'system_time': '2022-08-24T06:24:54.736Z', 'status': 'Pending', 'id': 999, 'date': '22-08-24 06:24:54.736000+00:00'}])
        print(data_df.shape)

        passers_df = pd.DataFrame(
            [{'id': 1052, 'task_status': 'Complete'},
             {'id': 772, 'task_status': 'Incomplete'}]
        )

        before = data_df.loc[(data_df['status'].isna()), 'id'].count()
        before_shape = data_df.shape

        df2 = get_metrics.update_by_lookp(
            left_df=data_df, left_col='status', left_on='id',
            lookup_df=passers_df, lookup_col='task_status', lookup_on='id', nulls_only=True)

        after = df2.loc[(df2['status'].isna()), 'id'].count()
        after_shape = df2.shape

        p = df2.iloc[4, 1]

        assert df2.iloc[0, 1] == passers_df.iloc[1,1]
        assert df2.iloc[4, 1] != passers_df.iloc[1,1]
        assert before >= after
        assert before_shape == after_shape

    def test_segmentation(self):
        data_df = pd.DataFrame([
            {'system_time': '2022-03-04T07:52:26Z', 'status': np.NaN, 'id': 772, 'date': '22-03-04 07:52:26+00:00'},
            {'system_time': '2022-06-22T17:52:42Z', 'status': 'Incomplete', 'id': 963, 'date': '22-06-22 17:52:42+00:00'},
            {'system_time': '2022-08-13T01:34:44Z', 'status': 'Pending', 'id': 1052, 'date': '22-08-13 01:34:44+00:00'},
            {'system_time': '2022-08-24T01:46:31.115Z', 'status': 'Pending', 'id': 1052, 'date': '22-08-24 01:46:31.115000+00:00'},
            {'system_time': '2022-08-14T06:04:54.736Z', 'status': 'Pending', 'id': 1053, 'date': '22-08-14 06:04:54.736000+00:00'},
            {'system_time': '2022-03-04T17:51:15.025Z', 'status': 'Incomplete', 'id': 772, 'date': '22-03-04 17:51:15.025000+00:00'},
            {'system_time': '2022-08-24T06:24:54.736Z', 'status': 'Pending', 'id': 999, 'date': '22-08-24 06:24:54.736000+00:00'}])
        before_shape = data_df.shape

        df1 = data_df[['id', 'date']]
        df2 = data_df[['status']]

        df3 = df1.merge(df2, left_index=True, right_index=True)
        aftere_shape = df3.shape
        print('bye')

    def test_camel(self):
        # todo - fix fails
        fails =  [
            ('snake___case' ,'snake_case'),
            ('snake__Case', 'snake_case'),
            ('sNAKEcASE', 'snake_case'),
            ('snake..Case','snake_case'),
        ]

        expected = [
            ('SnakeCase', 'snake_case'),
            ('snake.Case','snake_case'),
            ('snake_case', 'snake_case'),
        ]

        for row in expected:
            before, after = row
            app = get_metrics.camel_to_snake(before)
            assert app == after


doctest.testmod()