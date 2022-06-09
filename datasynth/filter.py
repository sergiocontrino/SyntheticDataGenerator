"""
Submodule for filtering a dataframe by individual frequencies of values per column.
Provide guarantee that each column contains only 'frequent' values, i.e. only values that
occur more often than a certain threshold min_count
"""
from get_args import get_args
import random
import numpy as np
import pandas as pd


def values_per_column(df: pd.DataFrame, min_count: int) -> dict:
    args = get_args()
    do_dates = args.filter_dates
    if do_dates:
        return values_per_column_all(df, min_count)
    else:
        return values_per_column_no_date(df, min_count)


def values_per_column_all(df: pd.DataFrame, min_count: int) -> dict:
    values_to_keep = {}
    for col in df.columns:
        # print("-*", col)
        df_counts = df.value_counts(col)
        mask = df_counts >= min_count
        mask_index = df_counts[mask].index.to_list()
        values_to_keep[col] = mask_index
    return values_to_keep


def values_per_column_no_date(df: pd.DataFrame, min_count: int) -> dict:
    values_to_keep = {}
    for col in df.columns:
        # print("-*", col)
        if "date" in col:
            values_to_keep[col] = df[col].tolist()
        else:
            df_counts = df.value_counts(col)
            mask = df_counts >= min_count
            mask_index = df_counts[mask].index.to_list()
            values_to_keep[col] = mask_index
    return values_to_keep


def bool_dataframe_columns_to_string(dfin):
    df = dfin.convert_dtypes()
    bool_cols = df.dtypes[df.dtypes == pd.BooleanDtype()].index.tolist()
    # print(bool_cols)
    if len(bool_cols) == 0:
        return df, []
    d = {True: 'TRUE', False: 'FALSE'}

    ddf = df.apply(lambda c: c.replace(d) if c.name in bool_cols else c)
    # print(ddf)

    #    return df.where(mask, df.replace(d)), bool_cols
    #    return df.replace(d), bool_cols
    # print("++++++")
    return ddf, bool_cols


def filter_common_categories(df: pd.DataFrame, min_count: int = 3) -> pd.DataFrame:
    df_filt = df.copy()

    # print("--init")
    # print(df_filt)


    # drop columns that only contain nan
    df_filt.dropna(how="all", axis=1, inplace=True)


    df_filt, bool_cols = bool_dataframe_columns_to_string(df_filt)

    # print("--pre")
    # print(df_filt)

    nan_hash = random.getrandbits(16)

    # print("-->>>pre", nan_hash)

    # df_filt.fillna(nan_hash, inplace=True, downcast='infer')
    df_filt.fillna(np.nan, inplace=True, downcast='infer')
    # print("--post")
    # print(df_filt)

    values_to_keep = values_per_column(df_filt, min_count)
    # print("d2", df_filt.shape)
    # print(df_filt.columns)
    # print("----")
    # print(values_to_keep['patientattended'])

    multi_index = tuple([values_to_keep[col] for col in df_filt.columns])

    # print("d3", multi_index)
    # print(df_filt.columns.to_list())

    df_mult = df_filt.reset_index().set_index(df_filt.columns.to_list())

    # print("d4", df_mult.shape)

    df_mult = df_mult.loc[multi_index, :].reset_index().set_index('index')

    # index = pd.MultiIndex.from_tuples(multi_index)
    # df_mult = df_mult.loc[multi_index, :]
    # df_mult.reset_index().set_index('index')

    # df_mult.replace(nan_hash, np.nan)
    # print("&&&&&&&&")
    return df_mult
