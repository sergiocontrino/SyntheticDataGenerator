"""
Submodule for filtering a dataframe by individual frequencies of values per column.
Provide guarantee that each column contains only 'frequent' values, i.e. only values that
occur more often than a certain threshold min_count
"""
import pandas as pd


def values_per_column(df: pd.DataFrame, min_count: int) -> dict:
    values_to_keep = {}
    for col in df.columns:
        df_counts = df.value_counts(col)
        mask = df_counts >= min_count
        mask_index = df_counts[mask].index.to_list()
        values_to_keep[col] = mask_index
    return values_to_keep


def bool_dataframe_columns_to_string(dfin):
    df = dfin.convert_dtypes()
    print("========")
    print(df.dtypes)
    print(df.dtypes == pd.BooleanDtype)
    bool_cols = df.dtypes[df.dtypes == pd.BooleanDtype()].index.tolist()
    print()
    print(bool_cols)
    if len(bool_cols) == 0:
        return df, []
 #   mask = df.applymap(type) != bool
 #   print("MASK ", mask)
    d = {True: 'TRUE', False: 'FALSE'}
    ddf = df.apply(lambda c: c.replace(d) if c.name in bool_cols else c)
    print(ddf)
#    return df.where(mask, df.replace(d)), bool_cols
#    return df.replace(d), bool_cols
    return ddf, bool_cols

def filter_common_categories(df: pd.DataFrame, min_count: int = 10) -> pd.DataFrame:
    df_filt = df.copy()
#    df_filt.dropna(inplace=True)
    df_filt.dropna(how="all", axis=1, inplace=True)
    df_filt, bool_cols = bool_dataframe_columns_to_string(df_filt)
    print("dd", bool_cols)
    values_to_keep = values_per_column(df_filt, min_count)
    print("d2", df_filt.shape)
    print(df_filt.columns)
    print("----")
    print(values_to_keep)
    multi_index = tuple([values_to_keep[col] for col in df_filt.columns])
    print("d3", len(multi_index))
    df_mult = df_filt.reset_index().set_index(df_filt.columns.to_list())
    print("d4", df_mult.shape)
    print(multi_index)
    #breakpoint()
    print("=-=-=")
    df_mult = df_mult.loc[multi_index, :].reset_index().set_index('index')
    print("d5", df_mult.shape)
    return df_mult
