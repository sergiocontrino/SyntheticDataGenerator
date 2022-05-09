"""Submodule for filtering adataframe by individual frequencies of values per column."""
import pandas as pd


def values_per_column(df: pd.DataFrame, min_count: int) -> dict:
    values_to_keep = {}
    for col in df.columns:
        df_counts = df.value_counts(col)
        mask = df_counts >= min_count
        mask_index = df_counts[mask].index.to_list()
        values_to_keep[col] = mask_index
    return values_to_keep


def bool_dataframe_columns_to_string(df):
    bool_cols = df.dtypes[df.dtypes == bool].index.tolist()
    if len(bool_cols) == 0:
        return df, []

    mask = df.applymap(type) != bool
    d = {True: 'TRUE', False: 'FALSE'}
    return df.where(mask, df.replace(d)), bool_cols


def filter_common_categories(df: pd.DataFrame, min_count: int = 10) -> pd.DataFrame:
    df_filt = df.copy()
    df_filt.dropna(how="all", axis=1, inplace=True)
    df_filt, bool_cols = bool_dataframe_columns_to_string(df_filt)
    values_to_keep = values_per_column(df_filt, min_count)
    multi_index = tuple([values_to_keep[col] for col in df_filt.columns])
    df_mult = df_filt.reset_index().set_index(df_filt.columns.to_list())
    df_mult = df_mult.loc[multi_index, :].reset_index().set_index('index')
    return df_mult
