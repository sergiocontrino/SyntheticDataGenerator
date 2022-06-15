# !/usr/bin/python
# -*- coding: utf-8 -*-
from config import config
from filter import filter_common_categories
from typing import NoReturn
from get_args import get_args
import psycopg2
import pandas as pd


def main() -> NoReturn:
    """
    NOT USED ANY LONGER (substituted by db_filter)

    :return: none
    """


def get_excluded_tables() -> str:
    """
    the list of tables we don't want to consider
    (for example in an intermine schema)
    """
    # intermine build tables
    exclusion_list = "'tracker', 'intermineobject', 'intermine_metadata', 'executelog', 'osbag_int'"
    # cv and loader filled tables
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


def do_sampling(args):
    """
    queries the db to get tables and their size.
    for each table get the relevant columns and sample their data

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

        columns_types = """
        SELECT column_name, data_type
FROM   information_schema.columns
WHERE  table_name = %s
AND column_name not like '%%id'
AND column_name not IN ('class', 'identifier')
ORDER  BY ordinal_position
        """

        # TODO: add here condition regarding minimum count
        # i.e. where column(.value) not in (black listed values)
        # get the black listed values from value_counter
        table_export = """
        select {} 
        from {}
        """

        rows = []    # used for debug

        # for each table
        for table in df_tsizes.index:
            t_size = int(df_tsizes.loc[table].at["rows"])
            t_cols = []

            # get the columns and their types
            cur.execute(columns_types, (table,))
            column_type = cur.fetchall()
            # for each column get the all the values and their count
            for column in column_type:
                t_cols.append(column[0])
                # temp for check.. to be used for checking minimum size
                value_counter(column, cur, rows, t_size, table)
            col_list = ", ".join(t_cols)

            # get all the data for the relevant columns..
            cur.execute(table_export.format(col_list, table))
            tab_exp = cur.fetchall()

            # ..and put it in a data frame
            qq1 = pd.DataFrame(tab_exp, columns=t_cols)
            print(table, "-->", qq1.shape)

            # if table == "contact":
            #     continue

            # filter rarely occurring values if the filter threshold is > 1
            threshold = args.filter_threshold
            if threshold > 1:
                qq = filter_common_categories(qq1, threshold)
            else:
                qq = qq1

            # scale the table's data according to original size
            # get scaling factor
            scaling_factor = int(float(df_tsizes.loc[table, 'ratio']) * args.target_size)
            print(table, ":  sampling ", scaling_factor, " records for columns: >>", col_list)

            # dump csv file of sampled data
            if len(qq) > 0:
                if args.no_seed:
                    qq.sample(n=scaling_factor, replace=True).to_csv('{0}.csv'.format(table), index=False)
                else:
                    qq.sample(n=scaling_factor, random_state=args.seed, replace=True).to_csv('{0}.csv'.format(table), index=False)
            else:
                print("WARNING: table", table, "is now empty! Try reducing the threshold for common values, now", threshold)

        # temp: dump a debug summary
        cols = ['table', 'attribute', 'type', 'value', 'count']
        summaries = pd.DataFrame(rows, columns=cols)
        # print df to file
        summaries.to_csv("summaries.csv")
        print()

        # close the communication with PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def value_counter(column, cur, rows, t_size, table):

    columns_counts = """
            select {}, count(1) 
    from {}
    group by 1 
    order by 2 desc
            """

    cur.execute(columns_counts.format(column[0], table))
    column_count = cur.fetchall()
    for ccrow in column_count:
        rows.append([table, column[0], column[1], "\"" + str(ccrow[0]) + "\"",
                     get_precision().format(ccrow[1] / t_size * 100)])


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
    do_sampling(get_args())
