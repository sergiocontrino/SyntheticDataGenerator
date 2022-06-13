"""
Submodule for filtering a dataframe by individual frequencies of values per column.
Provide guarantee that each column contains only 'frequent' values, i.e. only values that
occur more often than a certain threshold min_count
"""
import warnings
from datasynth.get_args import get_args

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
        df_counts = df.value_counts(col, dropna=False)
        mask = df_counts >= min_count
        mask_index = df_counts[mask].index.to_list()
        values_to_keep[col] = mask_index
    return values_to_keep


def values_per_column_no_date(df: pd.DataFrame, min_count: int) -> dict:
    values_to_keep = {}
    for col in df.columns:
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
    if len(bool_cols) == 0:
        return df, []
    d = {True: 'TRUE', False: 'FALSE'}

    ddf = df.apply(lambda c: c.replace(d) if c.name in bool_cols else c)
    return ddf, bool_cols


def filter_common_categories(df: pd.DataFrame, min_count: int) -> pd.DataFrame:
    if min_count <= 1:
        warnings.warn('min_count <= 1 will cause no filter to be applied', UserWarning)
        return df

    df_filt = df.copy()

    # drop columns that only contain nan
    df_filt.dropna(how="all", axis=1, inplace=True)
    df_filt, bool_cols = bool_dataframe_columns_to_string(df_filt)

    values_to_keep = values_per_column(df_filt, min_count)

    multi_index = tuple([values_to_keep[col] for col in df_filt.columns])

    df_mult = df_filt.reset_index().set_index(df_filt.columns.to_list())
    df_mult = df_mult.loc[multi_index, :].reset_index().set_index('index')

    return df_mult
