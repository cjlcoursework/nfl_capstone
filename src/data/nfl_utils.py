import re
import os
import warnings

import numpy as np
import pandas as pd
import os
import sys

module_path = os.path.abspath(os.path.join('../src'))
print("Adding modules", module_path)
if module_path not in sys.path:
    sys.path.append(module_path)

from src.features.wrangling.database_loader import DatabaseLoader
from src.features.wrangling.get_metrics import GetMetrics, update_by_lookp
from src.data.s3utils import download_from_s3

AWS_S3_BUCKET = "cjl-project-data"
AWS_S3_PREFIX = "nfl_capstone/data/raw"

RAW_DATA_PATH = '../data/raw'
INTERIM_DATA_PATH='../data/interim'

#
USE_CONNECTION="DB_FILENAME_URL"   # DB_FILENAME_URL for csv or DB_CONNECTION_URL for postgres
#
# #inputs
DATA_FILE = os.path.join(INTERIM_DATA_PATH,"nflverse.2009.reviewed.parquet")
# TEAMS_DATA = os.path.join(RAW_DATA_PATH,"nfl_teams_scraped.csv")
# DIMENSIONS_DATA = os.path.join(RAW_DATA_PATH,"dimensions.csv")
#
# #outputs
# GAMEPLAY_FACTS_DF_NAME=os.path.join(INTERIM_DATA_PATH, "gameplay_facts_cleaned_01.parquet")
# GAMEPLAY_DIM_DF_NAME=os.path.join(INTERIM_DATA_PATH, "gameplay_dimensions_cleaned_01.parquet")
# ANALYTICS_DF_NAME=os.path.join(INTERIM_DATA_PATH, "analytic_events_cleaned_01.parquet")
# ADMIN_DF_NAME=os.path.join(INTERIM_DATA_PATH, "admin_events_cleaned_01.parquet")
# READ_ME = os.path.join(INTERIM_DATA_PATH,"README.02-cjl-clean.txt")
#
# # tables
METRICS_INPUT_TABLE_NAME="nfl_metrics"
# CATEGORY_OUTPUT_TABLE_NAME="nfl_cleaned_categories"
# METRICS_OUTPUT_TABLE_NAME="nfl_cleaned_metrics"
#

def copy_raw_files_to_local(bucket: str, prefix: str, local_dir: str, file_names: list[str]) -> pd.DataFrame:

    download_from_s3(
        prefix=prefix,
        local_dir=os.path.abspath(local_dir),
        wishlist=file_names)

def get_local_parquet(file_name: str) -> pd.DataFrame:

    if not os.path.exists(file_name):
        raise Exception(f"Can't find the input file {file_name} .  Have you run the preceding notebooks? ")

    return pd.read_parquet(file_name)


def add_unique_play_id(data_df : pd.DataFrame) -> pd.DataFrame:
    ## todo - create a new play id feild in ALL record
    return data_df


def split_column_names(data_df : pd.DataFrame, metrics_df : pd.DataFrame) -> (list,list,list):

    # save analytics columns to a separate df, and remove from the dataset
    all_columns = set(data_df.columns)
    analytics_columns = set(metrics_df.loc[(metrics_df.feature_type == 'analytics'), 'column_name'])
    non_analytics_columns = all_columns.difference(analytics_columns)

    gameplay_facts_columns = set(
        metrics_df.loc[(metrics_df.c_dimension == 'fact') ,
        'column_name'])

    gameplay_facts_columns = gameplay_facts_columns.intersection(non_analytics_columns)
    dimensional_columns = non_analytics_columns.difference(gameplay_facts_columns)

    assert len(gameplay_facts_columns.intersection(dimensional_columns)) == 0
    assert len(gameplay_facts_columns.intersection(analytics_columns)) == 0

    return (
        gameplay_facts_columns,
        analytics_columns,
        dimensional_columns
    )


# In[43]:

def split_gameplay_colums(data_df : pd.DataFrame, metrics_df : pd.DataFrame) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):

    gameplay_facts_columns, analytics_columns, dimensional_columns = split_column_names(data_df=data_df, metrics_df=metrics_df)

    before_shape = data_df.shape
    print("data shape before splitting columns", before_shape)

    analytics_df =  data_df[list(analytics_columns)].copy()
    data_df.drop(columns=analytics_columns, inplace=True)

    dimensions_df = data_df[list(dimensional_columns)].copy()
    data_df.drop(columns=dimensional_columns, inplace=True)

    after_shape = data_df.shape
    print("data shape after splitting columns", after_shape)

    assert before_shape[0] == after_shape[0]
    assert before_shape[1] > after_shape[1]
    return data_df, dimensions_df, analytics_df


def separate_admin_rows(data_df : pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    data_df["admin_event"] = 0

    # maybe drop or add category [gameplay vs admin]
    data_df.loc[data_df["play_type_nfl"].isin([
        'END_QUARTER',
        'GAME_START',
        'END_GAME',
        'COMMENT',
        'TIMEOUT'
    ]), "admin_event"] = 1

    before_shape = data_df.shape
    print("separate_admin_events: data shape before splitting rows", before_shape)

    admin_events_df = data_df.loc[data_df.admin_event == 1].copy()
    print("separate_admin_events: admin_events shape", admin_events_df.shape)

    gameplay_facts_df = data_df.loc[data_df.admin_event == 0].copy()
    print("separate_admin_events: game_events shape", gameplay_facts_df.shape)

    assert before_shape[1] == admin_events_df.shape[1]
    assert before_shape[1] == gameplay_facts_df.shape[1]
    assert before_shape[0] == (admin_events_df.shape[0] + gameplay_facts_df.shape[0])

    return gameplay_facts_df, admin_events_df



def test_main():

    copy_raw_files_to_local(
        bucket=AWS_S3_BUCKET,
        prefix=AWS_S3_PREFIX,
        local_dir=os.path.abspath(RAW_DATA_PATH),
        file_names=['nfl_teams_scraped.csv', 'dimensions.csv'])

    data_df = get_local_parquet(DATA_FILE)

    db = DatabaseLoader(connection_string_env_url=USE_CONNECTION)
    metrics_df = db.read_table(METRICS_INPUT_TABLE_NAME)

    data_df = add_unique_play_id(data_df)

    data_df, dimensions_df, analytics_df = split_gameplay_colums(data_df=data_df, metrics_df=metrics_df)

    data_df, admin_df = separate_admin_rows(data_df)

    return





