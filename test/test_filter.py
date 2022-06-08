"""Test module for datasynth.filter"""
import pandas as pd
from datasynth.filter import filter_common_categories


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
    print(df)
    assert outcome_rows == 7
    assert initial_rows > outcome_rows


def test_filter_nodate_empty():
    df = pd.read_csv("/Users/sergio/git/SyntheticDataGenerator/test/filtrami_nodate.csv", index_col=False)

    initial_rows = df.shape[0]
    df = filter_common_categories(df, 3)
    outcome_rows = df.shape[0]
    assert outcome_rows == 6
    assert initial_rows > outcome_rows
