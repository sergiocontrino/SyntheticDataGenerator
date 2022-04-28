# !/usr/bin/python
# -*- coding: utf-8 -*-
import random

from config import config
from typing import NoReturn, Dict, Any
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

        table_row, table_names, tables_sizes = get_tables_size(args, cur)

        identifiers = """
        SELECT id
        FROM  {}
        """

        ref_tables = """
        SELECT table_name
FROM   information_schema.columns
WHERE  column_name like '{}id'
        """
        fill_pat = """
        update {}
        set siteid = {}
        where id = {}
        """

        fill_refs = """
        update %s
        set siteid = %s
        where id = %s
        """

        fill_refss = """
        update {}
        set {}id = {}
        where id = {}
        """

        all_identifiers: dict[Any, Any]

        for trow in table_row:
            print(trow[0])
            ref_ids = get_all_table_ids(cur, identifiers, trow[0])

            #            all_identifiers[trow[0]] = this_id_set

            cur.execute(ref_tables.format(trow[0]))
            this_ref_set = cur.fetchall()
            ref_set = [item[0] for item in this_ref_set]
            print(ref_set, len(ref_set))
            # print(this_ref_set, len(this_ref_set))
            if len(this_ref_set) == 0:
                continue
            """
            get the referred tables
            for each
                get size (table_size.value)
                build sample
                for each id update with a sample element (scan both)
            """
            for table_name in ref_set:
                table_size = tables_sizes.get(table_name)
                if table_name == 'riskfactor':
                    continue
                print(table_name, table_size)
                sampled_ref_id = random.choices(ref_ids, k=int(table_size))
                ids = get_all_table_ids(cur, identifiers, table_name)
                print(sampled_ref_id[0:5], "-- ids size:", len(sampled_ref_id), " from", len(ref_ids), ref_ids[0:5], " for",
                      table_size)
                # cur.executemany(fill_refs, (r, sampled_ids, ids))
                counter = 0
                for i in ids:
                    print(fill_refss.format(table_name, trow[0], sampled_ref_id[counter], i))
                    print(table_name, trow[0], i, "-", sampled_ref_id[counter])
                    cur.execute(fill_refss.format(table_name, trow[0], sampled_ref_id[counter], i))
                    counter = counter + 1
                    count = cur.rowcount
                    print(count, "Recordss Updated successfully ")
                conn.commit()
                # cur.executemany(fill_refss.format(r, sampled_ids, ids))

        print("**" * 20)
        # for k, v in all_identifiers.items():
        #    print(k)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def get_all_table_ids(cur, q_identifiers, table_name):
    cur.execute(q_identifiers.format(table_name))
    id_set = cur.fetchall()
    ids = [item[0] for item in id_set]
    return ids


def get_tables_size(args, cur):
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
    table_names = []
    tables_sizes = {}  # k=table name, v=table size
    for rec in class_counts:
        rows.append([rec[1], get_precision().format(rec[1] / den)])
        table_names.append(rec[0])
        tables_sizes.update({rec[0]: rec[1]})
    df_tsizes = pd.DataFrame(rows, columns=cols, index=table_names)
    print(df_tsizes)
    print()
    print(tables_sizes)
    return table_row, table_names, tables_sizes


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
