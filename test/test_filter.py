"""Test module for datasynth.filter"""
import pytest

import random
import pandas as pd
import numpy as np
import pytest

from datasynth.filter import filter_common_categories, values_per_column_all


def test_value_count():
    df = pd.DataFrame(data={'column1': [1] * 5 + [2] * 10,
                            'column2': [1] * 3 + [2] * 6 + [3] * 3 + [4] * 3,
                            'column3': [1] * 15}
                      )
    expected = pd.DataFrame(data={'column1': [2] * 4,
                                  'column2': [2] * 4,
                                  'column3': [1] * 4}, index=[5, 6, 7, 8])

    expected.index.name = 'index'

    outcome = filter_common_categories(df, min_count=6)
    assert pd.testing.assert_frame_equal(expected, outcome) is None


def test_filter_not_empty_vanilla():
    df = pd.DataFrame(data={'column1': [1] * 5 + [2] * 10,
                            'column2': [1] * 3 + [2] * 6 + [3] * 3 + [4] * 3,
                            'column3': [1] * 15})

    initial_rows = df.shape[0]
    df = filter_common_categories(df, 6)
    outcome_rows = df.shape[0]
    assert outcome_rows > 0
    assert initial_rows > outcome_rows


def test_filter_not_empty():
    df = pd.read_csv("/Users/sergio/git/SyntheticDataGenerator/test/filtrami.csv", index_col=False)

    initial_rows = df.shape[0]
    df = filter_common_categories(df, 2)
    outcome_rows = df.shape[0]
    assert outcome_rows > 0
    assert initial_rows > outcome_rows


def test_filter_empty_vanilla():
    df = pd.DataFrame(data={'column1': [1] * 5 + [2] * 10,
                            'column2': [1] * 3 + [2] * 6 + [3] * 3 + [4] * 3,
                            'column3': [1] * 15})

    initial_rows = df.shape[0]
    df = filter_common_categories(df, 6)
    outcome_rows = df.shape[0]
    assert outcome_rows == 4
    assert initial_rows > outcome_rows


def test_filter_empty():
    df = pd.read_csv("/Users/sergio/git/SyntheticDataGenerator/test/filtrami.csv", index_col=False)

    initial_rows = df.shape[0]
    df = filter_common_categories(df, 2)
    outcome_rows = df.shape[0]
    assert outcome_rows == 7
    assert initial_rows > outcome_rows


def test_filter_empty_warning():
    df = pd.read_csv("/Users/sergio/git/SyntheticDataGenerator/test/contact.csv", index_col=False)
    initial_rows = df.shape[0]
    with pytest.warns(UserWarning):
        df = filter_common_categories(df, 1)
    outcome_rows = df.shape[0]
    assert initial_rows == outcome_rows


def test_filter_empty_big():
    df = pd.read_csv("/Users/sergio/git/SyntheticDataGenerator/test/contact.csv", index_col=False)

    initial_rows = df.shape[0]
    df = filter_common_categories(df, 2)
    outcome_rows = df.shape[0]
    print(df)
    assert outcome_rows == 7
    assert initial_rows > outcome_rows


def test_value_to_keep_simple():
    data = {'col_1': [0, 0, 0, 1, 2, 2], 'col_2': [1, 1, 1, 2, 2, 2]}
    df = pd.DataFrame(data=data)

    expect = {'col_1': [0, 2], 'col_2': [1, 2]}
    output = values_per_column_all(df, min_count=2)

    assert expect == output


def test_value_to_keep_nan():
    data = {'col_1': [0, 0, 0, 1, 2, 2, np.nan], 'col_2': [1, 1, 1, 2, 2, 2, 2]}
    df = pd.DataFrame(data=data)

    expect = {'col_1': [0, 2], 'col_2': [1, 2]}
    output = values_per_column_all(df, min_count=2)

    assert expect == output


def test_value_to_keep_nan():
    data = {'col_1': [0, 0, 0, 1, 2, 2, np.nan, np.nan, np.nan], 'col_2': [1, 1, 1, 2, 2, 2, 3, 3, 3]}
    df = pd.DataFrame(data=data)

    expect = {'col_1': [0, 2, np.nan], 'col_2': [1, 2, 3]}
    output = values_per_column_all(df, min_count=2)

    assert set(expect['col_1']).difference(set(output['col_1'])) == {np.nan}


def test_nan_multiindex():
    df = pd.DataFrame(data={'col_1': [np.nan, 1, 2], 'col_2': [1, 2, 3], 'col_3': [4, 5, 6]})
    df.set_index(['col_1', 'col_2'], inplace=True)
    assert df.loc[(np.nan, 1), :].iloc[0] == 4
    assert df.loc[([np.nan], [1]), :].values[0] == 4


def test_filter_no_date_empty():
    df = pd.read_csv("/Users/sergio/git/SyntheticDataGenerator/test/filtrami_nodate.csv", index_col=False)

    initial_rows = df.shape[0]
    df = filter_common_categories(df, 3)
    outcome_rows = df.shape[0]
    assert outcome_rows == 6
    assert initial_rows > outcome_rows
