# !/usr/bin/python
# -*- coding: utf-8 -*-
import random
from config import config
from typing import NoReturn
from get_args import get_args
import psycopg2
import pandas as pd


def main() -> NoReturn:
    """

    :return: none
    """


def get_excluded_tables() -> str:
    """
    the list of tables we don't want to consider
    (for example in an intermine schema)
    """
    # intermine build tables
    exclusion_list = "'tracker', 'intermineobject', 'intermine_metadata', 'executelog', 'osbag_int'"
    # cv and tables filled by loader (dataset/datasource)
    exclusion_list += ", 'riskfactordefinition', 'problem', 'datasource', 'dataset', 'ethnicity'"
    return exclusion_list


def get_precision() -> str:
    """
    the number of decimals we want to keep for our floats
    """
    precision = """{:.2f}"""
    return precision


def connection():
    # read connection parameters
    params = config()
    con: connection = psycopg2.connect(**params)
    con.set_client_encoding('UTF8')
    return con


def sample(args):
    """
    queries the db to get tables and their size.
    for each table get the relevant columns, filter rarely occurring values
    and produce a csv file of required size of synthetic data.

    :param args: the args
    :return: none
    """

    # Connect to the PostgreSQL database server
    conn = connection()
    try:
        # connect to the PostgreSQL server
        # create a cursor
        cur = conn.cursor()

        # just show the db we are querying
        show_dbname(cur)

        # get the tables size (nr of rows)
        df_tsizes = get_tables_size(args, cur)
        print(df_tsizes)
        print()

        # types are not strictly needed
        # TODO remove

        columns_types = """
        SELECT column_name, data_type
FROM   information_schema.columns
WHERE  table_name = %s
AND column_name not like '%%id'
AND column_name not IN ('class', 'identifier')
ORDER  BY ordinal_position
        """

        target = args.target_size
        threshold = args.filter_threshold

        # for each table
        for table in df_tsizes.index:

            t_size = int(df_tsizes.loc[table].at["rows"])
            # t_cols = []

            # initialise the df of synth data for the table
            syn_table = pd.DataFrame()

            # get the columns (and their types)
            cur.execute(columns_types, (table,))
            column_type = cur.fetchall()
            # for each column get the all the values and their count
            for column in column_type:
                # t_cols.append(column[0])
                # get the counts
                cols_count = value_counter(cur, table, column, threshold)
                # build the synthetic column
                syn_col = build_synth_col(t_size, cols_count, target)
                # add it to the data frame
                syn_table[column[0]] = syn_col

            # scale the table's data according to original size
            # get scaling factor
            scaling_factor = int(float(df_tsizes.loc[table, 'ratio']) * args.target_size)
            print(table, ":  sampling", scaling_factor, "records..")
            # print("for columns: >>", col_list)

            # dump csv file of sampled data
            if len(syn_table) > 0:
                if args.no_seed:
                    syn_table.sample(n=scaling_factor, replace=True).to_csv('{0}.csv'.format(table), index=False)
                else:
                    syn_table.sample(n=scaling_factor,
                                     random_state=args.seed, replace=True).to_csv('{0}.csv'.format(table),
                                                                                             index=False)
            else:
                print("WARNING: table", table, "is now empty! Try reducing the threshold for common values, now",
                      threshold)

        # close the communication with PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def value_counter(cur, table, column, threshold):
    """
    remove rare occurrences and get the counts
    """

    columns_counts = """
    select {}, count(1) 
    from {}
    group by 1
    having count(1) >= {} 
    order by 2 desc
            """

    cur.execute(columns_counts.format(column[0], table, threshold))

    column_count = cur.fetchall()
    return column_count


def build_synth_col(table_size, cols_count, target_size):
    """
    input:
    table_size: 90426
    column: patientattended
    col_counts: [(True, 73906), (False, 15470), (None, 1050)]
    target_size: 100

    """
    col = []
    added = 0
    this_value = ""

    for line in cols_count:
        this_value = line[0]
        this_count = line[1]
        tg_count = int(this_count * target_size/table_size)
        added += tg_count
        for t in range(tg_count):
            col.append(this_value)

    if added < target_size:
        for r in range(target_size - added):
            col.append(this_value)

    # reproducibility
    random.seed(3)
    random.shuffle(col)
    return col


def verbose_output(col, synthetic_col):
    # TODO: case insensitive null
    print("-" * 20)
    print("Column", col, ": size of synth data set =", len(synthetic_col), "with",
          synthetic_col.count("NULL"), "null values")
    print(synthetic_col)


def get_tables_size(args, cur):
    """
    queries the db to get the tables and their size

    :param args:
    :param cur:
    :return: data frame with table size
    """

    q_tables_size = """
        SELECT relname,reltuples
        FROM pg_class C
        LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
        WHERE 
          nspname NOT IN ('pg_catalog', 'information_schema') 
          AND relname NOT IN (""" + get_excluded_tables() + """) 
          AND relkind='r' 
          and reltuples > 0
        ORDER BY reltuples DESC
                """

    cur.execute(q_tables_size)
    class_counts = []  #
    table_row = cur.fetchall()
    for row in table_row:
        if row[0] == args.scaling_class:
            den = row[1]
        class_counts.append(row)
    cols = ['rows', 'ratio']
    rows = []
    ind = []
    tables_sizes = {}
    for rec in class_counts:
        rows.append([rec[1], get_precision().format(rec[1] / den)])
        ind.append(rec[0])
        tables_sizes.update({rec[0]: rec[1]})
    df_tsizes = pd.DataFrame(rows, columns=cols, index=ind)
    return df_tsizes


def get_db_version(cur):
    print('PostgreSQL database version:')
    cur.execute('SELECT version()')
    db_version = cur.fetchone()
    print(db_version)


def show_dbname(cur: connection()) -> NoReturn:
    """
    print header with database in use
    """
    cur.execute('SELECT current_database()')
    db_name = cur.fetchone()[0]
    print()
    print('Connecting to database:', db_name)
    print("==" * 20)


if __name__ == '__main__':
    sample(get_args())
