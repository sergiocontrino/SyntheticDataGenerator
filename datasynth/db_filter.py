# !/usr/bin/python
# -*- coding: utf-8 -*-
import random
from typing import NoReturn
from get_args import get_args
import pandas as pd
import queries as q

def main() -> NoReturn:
    """

    :return: none
    """


def get_excluded_tables() -> str:
    """
    the list of tables we don't want to consider
    (for example in an intermine schema)

    TODO: get them from a config file?
    """
    # intermine build tables
    exclusion_list = "'tracker', 'intermineobject', 'intermine_metadata', 'executelog', 'osbag_int'"
    # cv and tables filled by loader (dataset/datasource)
    exclusion_list += ", 'riskfactordefinition', 'problem', 'datasource', 'dataset', 'ethnicity'"
    return exclusion_list


def get_precision() -> str:
    """
    the number of decimals we want to keep for our floats

    TODO: do we really need it? get it from a config file?
    """
    precision = """{:.2f}"""
    return precision


def sample(args):
    """
    queries the db to get tables and their size.
    for each table get the relevant columns, filter rarely occurring values
    and produce a csv file of required size of synthetic data.

    :param args: the args
    :return: none
    """

    db_vendor = get_db_vendor(args)

    # Connect to the database server
    conn = q.connection(db_vendor)
    try:
        # create a cursor
        cur = conn.cursor()

        # just show the db we are querying
        if db_vendor == 'pg':
            show_dbname(cur)

        # get the tables size (nr of rows)
        df_tsizes = get_tables_size(args, cur)
        print(df_tsizes)
        print()

        # types are not strictly needed
        # TODO remove type, add get_excluded_columns?

        target = args.target_size
        threshold = args.filter_threshold
        seed = args.seed
        unseeded = args.no_seed

        # for each table
        for table in df_tsizes.index:
            t_size = int(df_tsizes.loc[table].at["rows"])

            # initialise the df of synth data for the table
            syn_table = pd.DataFrame()

            # get the columns (and their types)
            query_cols = getattr(q, '{}_columns'.format(db_vendor))
            cur.execute(query_cols, (table,))
            # cur.execute(q.pg_columns, (table,))
            columns = cur.fetchall()
            # for each column get the all the values and their count
            for column in columns:
                # get the counts
                cols_count = value_counter(cur, table, column, threshold)
                # build the synthetic column
                syn_col = build_synth_col(t_size, cols_count, target, seed, unseeded)
                # add it to the data frame
                syn_table[column[0]] = syn_col

            # scale the table's data according to original size
            # get scaling factor
            scaling_factor = int(float(df_tsizes.loc[table, 'ratio']) * args.target_size)
            print(table, ":  sampling", scaling_factor, "records..")
            # print("for columns: >>", col_list)

            # dump csv file of sampled data
            # note: removing trailing 0 in the date (introduced because of nulls -> float)
            if len(syn_table) > 0:
                if args.no_seed:
                    syn_table.sample(n=scaling_factor, replace=True).to_csv('{0}.csv'.format(table),
                                                                            float_format="%.0f", index=False)
                else:
                    syn_table.sample(n=scaling_factor,
                                     random_state=args.seed, replace=True).to_csv('{0}.csv'.format(table),
                                                                                  float_format="%.0f", index=False)
            else:
                print("WARNING: table", table, "is now empty! Try reducing the threshold for common values, now",
                      threshold)

        # close the communication with PostgreSQL
        cur.close()
    except (Exception, get_db_error(args)) as error:
        # except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def get_db_vendor(args):
    if args.mssqlserver:
        db_vendor = "ms"
    else:
        db_vendor = "pg"
    return db_vendor


def get_db_error(args):
    if args.mssqlserver:
        db_error = "pyodbc.DatabaseError"  # TODO check!
    else:
        db_error = "psycopg2.DatabaseError"
    return db_error


def value_counter(cur, table, column, threshold):
    """
    remove rare occurrences and get the counts

    note: - by default threshold =1
          - this query should not be db vendor dependant
    """

    q_columns_count = """
    select {}, count(1) 
    from {}
    group by 1
    having count(1) >= {} 
    order by 2 desc
            """

    cur.execute(q_columns_count.format(column[0], table, threshold))

    columns_count = cur.fetchall()
    return columns_count


def build_synth_col(table_size, cols_count, target_size, seed, unseeded):
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
        tg_count = int(this_count * target_size / table_size)
        added += tg_count
        for t in range(tg_count):
            col.append(this_value)

    if added < target_size:
        for r in range(target_size - added):
            col.append(this_value)

    # seed can be used for reproducibility
    if not unseeded:
        random.seed(seed)
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

    db_vendor = get_db_vendor(args)
    tables_size = getattr(q, '{}_tables_size'.format(db_vendor))
    cur.execute(tables_size)
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


def show_dbname(cur: q.connection('pg')) -> NoReturn:
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
