import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import re
from math import ceil

from src.features.wrangling.database_loader import DatabaseLoader


def get_versioned_name(file_name: str) -> str:
    """
    Given a file name this checks whether the name alreadys exists
    If it exists we add a number to the file name

    e.g.
        input file_name == myexistingfile.py
        returns myexistingfile(1).py

    :param file_name:
    :return:
    """
    split_tup = os.path.splitext(file_name)
    name = file_name
    counter = 0
    while os.path.exists(name):
        counter += 1
        name = os.path.join(f"{split_tup[0]}({str(counter)}){split_tup[1]}" )
    return name


def update_by_lookp(left_df, left_col, left_on, lookup_df, lookup_col, lookup_on , nulls_only: bool = False) -> pd.DataFrame:
    """
    The main dataframe has a column we want to change, and the lookup dataframe has the new value
    We join the dataframes together on the left_on and lookup_on keys
    Then re replace the old value with the new value from the lookup dataframe

    :param left_df:  main dataframe
    :param left_col:  the column we plan to change - the 'old' value
    :param left_on:  the column we are merging on

    :param lookup_df:  lookup data
    :param lookup_col: the column with the new value
    :param lookup_on: the column we will merge on the main dataframe

    :param nulls_only:  if true, we'll only replace the old value if it's null
    :return:
    """

    before_shape = left_df.shape

    lookup_df.drop_duplicates(subset=lookup_on, inplace=True)

    if left_col in lookup_df.columns:
        lookup_df.rename(columns={left_col: f"{left_col}_fix"}, inplace=True)

    df2 = left_df.merge(lookup_df, left_on=left_on, right_on=lookup_on, how='left', indicator=True)

    query = (~df2[lookup_col].isnull()) & (df2['_merge'] == 'both')
    if nulls_only:
        query = query & (df2[left_col].isna())

    df2[left_col] = np.where(query, df2[lookup_col], df2[left_col])
    df2.drop(columns=[lookup_col, '_merge'], inplace=True)

    after_shape = df2.shape

    if after_shape != before_shape:
        raise Exception("Shape of output is too big.  Something fanned out.  Make sure the lookup df is absolutely unique")

    return df2


def plot_missing(metrics_df, missing_only=True):
    """
    Plot the missing values in a dataframe - thie is from a metrics dataframe that we create

    :param metrics_df:   a custom dataframe with metrics (See get_metrics below)
    :param missing_only:   if this is true then we'll only plot columns with missing values
    :return:
    """
    df = metrics_df.loc[metrics_df['missing_count'] > 0] if missing_only else metrics_df
    s = int(len(df.index.values)*.6)

    sns.set(style="darkgrid")
    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(25, s))

    ax.barh(
        df.index.values,
        df.completeness,
        color='teal',
        alpha=.6)

    ax.set_title("Percentage completeness of categories with missing values")
    for i in ax.patches:
        plt.text(i.get_width() + 0.01, i.get_y() + 0.5,
                 str(round((i.get_width()), 5)),
                 fontsize=10, fontweight='bold',
                 color='black')

    plt.show()


def plot_continuous(data_df, metrics_df):
    """
    Create a grid of seaborn histograms from the dataframe - and the "metrcs data we collected (See get_metrics() below)

    todo: I think there must be an easier way to do this.

    :param data_df:
    :param metrics_df:
    :return:
    """

    # generator
    def gen_feature(columns):
        for x in columns:
            yield x

    # criteria for histogram
    query = (metrics_df['feature_type'] == 'continuous')

    # data and shape
    subset = metrics_df[query]
    cols = subset.index
    data_columns = subset.index
    grid_cols = 3
    grid_rows = ceil(cols.size / grid_cols)

    # plot
    sns.set(style="darkgrid")
    fig, axes = plt.subplots(grid_rows, grid_cols, figsize=((grid_cols * 10), (grid_rows * 6)))
    fig.subplots_adjust(wspace=.3)
    fig.subplots_adjust(hspace=.4)
    kde = True

    # initialize generator
    generator = gen_feature(data_columns)

    # create subplots
    for gr in range(grid_rows):
        for gc in range(grid_cols):
            try:
                col = next(generator)
                sns.histplot(data=data_df, x=col, kde=kde, color='teal', ax=axes[gr, gc])
            except StopIteration:
                break

    plt.show()


def camel_to_snake(name):
    """Given a camelcase name - convert it to snake case
    :param name:  the camelcase name to convert
    :return: a snake case string
    
    >>>camel_to_snake("CamelCase")
    camel_case
    """
    name = str(name).replace(".", "_")   # replace any periods in anes with underscores
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)    # camel case to snake convertion
    name = re.sub('__([A-Z])', r'_\1', name)             # convert muliple underlines to a single
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    return name.lower()


def conform_column_names(data_df: pd.DataFrame) -> (list[str], list[str]):
    """Convert all the column names in a dataframe to snake_case

    :param data_df:
    :return: a list of the original column names, and a list of new column names
    
    >>>conform_column_names(pd.DataFrame({EmployeeName: 'freddy', AgeOfEmployee: 28}))
    ['EmployeeName', 'AgeOfEmployee'], ['employee_name', 'age_of_employee']
    
    """
    original_cols = data_df.columns
    new_cols = [camel_to_snake(x) for x in data_df.columns]
    data_df.columns = new_cols
    return original_cols, new_cols


class GetMetrics:
    """
    Creates a custom metrics dataframe from a dataframe.
    The idea is to store the describe() and unique() and value_count() data
    so we can use it programmably and we can also manually adjust
    metrics data to get a different result.

    For example,
    I might classify something that looks like a continuous column to a category or a "key"
    so later programs can use the change to operate on the data in a different way
    """

    # class attributes
    data_df: pd.DataFrame
    metrics_df: pd.DataFrame
    unique_values_threshold: int = 40
    sparse_column_boundary: float = .9  # percentage of non-null values to break out as good or poor completeness

    def __init__(self):
        pass

    def number_metrics(self, df: pd.DataFrame, column_name: str) -> dict:
        """helper function to collect metrics for a numeric type
        :param df:
        :param column_name:
        :return: a dict of the metrics we are capturing
        """
        m = df[column_name].describe()
        return dict(
            top=np.NaN,
            freq=np.NaN,
            count=m['count'],
            mean=m['mean'],
            std=m['std'],
            min=m['min'],
            max=m['max'],
            median=m['50%']
        )

    def object_metrics(self, df: pd.DataFrame, column_name: str) -> dict:
        """helper function to collect metrics for a non-numeric type

        :param df:
        :param column_name:
        :return: a dict of the metrics we are capturing
        """
        m = df[column_name].describe()
        return dict(
            top=m['top'],
            freq=m['freq'],
            count=np.NaN,
            mean=np.NaN,
            std=np.NaN,
            min=np.NaN,
            max=np.NaN,
            median=np.NaN
        )

    def get_describe_metrics(self, df: pd.DataFrame, col_name: str) -> dict:
        # numeric data gets different output from the describe() function
        # so call two different funcs depending on type
        this_dtype = str(df[col_name].dtype)
        if df[col_name].dtype == np.dtype('object') or  df[col_name].dtype == np.dtype('datetime64[ns]'):
            m_dict = self.object_metrics(df=df, column_name=col_name)
        else:
            m_dict = self.number_metrics(df=df, column_name=col_name)
        return m_dict

    def categorize_by_missing(self, good_count, total_count: int) -> (float, str):
        completeness_ratio = good_count / total_count
        missing_count = total_count - good_count
        if missing_count == 0:
            completeness_cat = "perfect"
        elif completeness_ratio >= self.sparse_column_boundary:
            completeness_cat = "good"
        else:
            completeness_cat = "poor"

        return completeness_ratio, completeness_cat

    def get_metrics(self, df: pd.DataFrame, dim_df: pd.DataFrame = None) -> pd.DataFrame:
        """collect metrics from a dataframe

        :param df:    the dataframe we want metrics from
        :param dim_df:  an optional dimensions df that we can use to override feature_type or dimension
        :return:  a dict of the metrics we are capturing
        
        >>>get_metrics(pd.DataFrame({employee_name: 'freddy', age_of_employee: 28}, pd.DataFrame({'column_name': 'employee_name', 'feature_type': 'key', 'c_dimension' : 'fact'}))
        
        [
        dict( column_name=employee_name, data_type=object, unique_counts=1, feature_type=key, c_dimension=fact, row_count=1, good_count=1,
                missing_count=0, completeness=100%, quality='perfect', mean=NaN, std=NaN, min=NaN, max=NaN, mdian=NaN,  top=freddy, freq=2 )
         dict(column_name=age_of_employee, data_type=int64, unique_counts=1, feature_type=category, c_dimension=fact, row_count=1, good_count=1,
                missing_count=0, completeness=100%, quality='perfect', mean=.9, std=.02, min=10, max=65, median=29,  top=Nan, freq=Nan )
        ]

       """
        r_count, c_count = df.shape   # save the original shape of the dataframe
        metrics_populated = []

        # save the unique counts to a series
        unique_counts = df.nunique().rename("unique_counts")

        # loop through each column and get its metrics
        for col_name in df.columns:

            # get good and missing values
            good_values = df[col_name].notna().sum()
            missing_values = df[col_name].isna().sum()

            # numeric data gets different output from the describe() function
            # so call two different funcs depending on type
            this_dtype = str(df[col_name].dtype)
            m_dict = self.get_describe_metrics(df, col_name=col_name)

            # calculate how complete the data is good values divided by total rows,
            # and 'grade' the outcome: poor, good, perfect
            completeness_ratio, completeness_cat = self.categorize_by_missing(good_count=good_values, total_count=r_count)

            # get unique counts for just this column
            unique_count = unique_counts[col_name]

            # take a stab at categorizing the column as a category or continuous
            #  I like to change these values manually after reviewing, so save any overrides in a dimensions dataframe
            #  the c_dimensions column is to designate a column as if we were creating fact and dimensions from the data
            #  this tells us whether a column is a 'fact' that we expect to always be populated
            #    or a dimension - that is only valid for certain cases.
            #    for example, the fact that there was a pass play is different from the data about the pass - like the passer and receiver.
            #    the dimension data could be sparse with a ot of missing data that looks bad until we realize that it's just not applicable for this specific row
            #    The 'passer' field would be null if this is a run play, and that's ok - but in some cases we want to remove it
            #
            feature_type = 'continuous' if unique_count > self.unique_values_threshold and this_dtype in ['int64', 'float64'] else 'category'
            c_dimension = None
            if dim_df is not None and not dim_df.empty:
                try:
                    feature_type = dim_df.loc[(dim_df['column_name'] ==  col_name), 'feature_type'].item()
                    c_dimension = dim_df.loc[(dim_df['column_name'] ==  col_name), 'c_dimension'].item()
                except:
                    pass

            # this is the metrics output for each column
            base_dict = dict(
                column_name=col_name,
                data_type=this_dtype,
                unique_counts=unique_count,
                feature_type=feature_type,
                c_dimension=c_dimension,
                row_count=r_count,
                good_count=good_values,
                missing_count=missing_values,
                completeness=completeness_ratio,
                quality=completeness_cat,
                mean=m_dict['mean'],
                std=m_dict['std'],
                min=m_dict['min'],
                max=m_dict['max'],
                median=m_dict['median'],
                top=m_dict['top'],
                freq=m_dict['freq']

            )
            metrics_populated.append(base_dict)

        df = pd.DataFrame(metrics_populated)
        df.set_index('column_name', inplace=True)
        df.sort_values('completeness', inplace=True)
        self.metrics_df = df
        return df

    def get_categories(self, data_df,  unique_count_threshold=40) -> pd.DataFrame:
        """For each column that we designated as a 'category' save the unique values
        so that we can quickly review them manually, and also document the
        allowable values for this column

        For example, The allowable values for a status field could be 'Good', 'Great' or 'Bad'
        and we want to document that and validate any new data we get

        :param data_df:
        :param unique_count_threshold:
        :return:
        """
        categories = []
        for p in self.metrics_df[(self.metrics_df['feature_type'] == 'category')].index:
            col = data_df[p]
            cat = col.value_counts().to_dict()
            for k, v in cat.items():
                categories.append(dict(index=p, value=k, count=v))
        cat_df = pd.DataFrame(categories)
        return cat_df

