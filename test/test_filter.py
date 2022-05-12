"""Test module for datasynth.filter"""
import pandas as pd
from synthdatagen.filter import filter_common_categories


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


def test_filter_not_empty():
    # TODO: Add realistic example with fake data
    # df = pd.read_csv("<more realistic example>", index_col=0)
    df = pd.DataFrame(data={'column1': [1] * 5 + [2] * 10,
                            'column2': [1] * 3 + [2] * 6 + [3] * 3 + [4] * 3,
                            'column3': [1] * 15})

    initial_rows = df.shape[0]
    df = filter_common_categories(df, 5)
    outcome_rows = df.shape[0]
    assert outcome_rows > 0
    assert initial_rows > outcome_rows


def test_filter_empty():
    # TODO: Add realistic example with fake data
    # df = pd.read_csv("<more realistic example>", index_col=0)
    df = pd.DataFrame(data={'column1': [1] * 5 + [2] * 10,
                            'column2': [1] * 3 + [2] * 6 + [3] * 3 + [4] * 3,
                            'column3': [1] * 15})

    initial_rows = df.shape[0]
    df = filter_common_categories(df)
    outcome_rows = df.shape[0]
    assert outcome_rows == 0
    assert initial_rows > outcome_rows
