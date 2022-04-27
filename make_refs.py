# !/usr/bin/python
# -*- coding: utf-8 -*-
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
    exclusion_list = "'tracker', 'intermineobject', 'intermine_metadata', 'executelog', 'osbag_int'"
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

    # con: connection = psycopg2.connect(
    #    dbname='ithrivemine',
    #    user='modmine',
    #    password='modmine',
    #    host='localhost',
    #    port=5432
    # )


def get_tables(args):
    """ Connect to the PostgreSQL database server """
    conn = connection()
    try:
        # connect to the PostgreSQL server
        # create a cursor
        cur = conn.cursor()

        # just show the db we are querying
        show_dbname(cur)

        tables_rows = """
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

        cur.execute(tables_rows)

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
        all_identifiers = {}

        for rec in class_counts:
            rows.append([rec[1], get_precision().format(rec[1] / den)])
            ind.append(rec[0])
            tables_sizes.update({rec[0]: rec[1]})
        df_tsizes = pd.DataFrame(rows, columns=cols, index=ind)
        print(df_tsizes)

        print()

        identifiers = """
        SELECT id
        FROM  {}
        """

        ref_ids = """
        SELECT table_name
FROM   information_schema.columns
WHERE  column_name like '{}id'
        """

        fill_refs = """
        update patient
        set siteid = {}
        where id = {}
        """

        columns_counts = """
        select {}, count(1) 
from {}
group by 1 
order by 2 desc
        """

        table_export = """
        select {} 
        from {}
        """

        cols = ['table', 'attribute', 'type', 'value', 'count']
        rows = []

        col_dict = {}  # TODO: get directly from df cc

        for trow in table_row:
            tcols = []
            print(trow[0])
            cur.execute(identifiers.format(trow[0]))
            this_id_set = cur.fetchall()

            all_identifiers[trow[0]] = this_id_set

            cur.execute(ref_ids.format(trow[0]))
            this_ref_set = cur.fetchall()
            print(this_ref_set)

        print("**" * 20)
        for k, v in all_identifiers.items():
            print(k)

        print("----- UPDATE -----")
        cur.execute(fill_refs.format(3000002, 11000002))
        conn.commit()
        count = cur.rowcount
        print(count, "Record Updated successfully ")

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def dump_csv(args, qq, scaling_factor, trow):
    if args.no_seed:
        qq.sample(n=scaling_factor, replace=True).to_csv('{0}.csv'.format(trow[0]), index=False)
    else:
        qq.sample(n=scaling_factor, random_state=args.seed, replace=True).to_csv('{0}.csv'.format(trow[0]),
                                                                                 index=False)


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
    get_tables(get_args())
