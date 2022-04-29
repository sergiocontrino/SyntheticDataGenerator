# !/usr/bin/python
# -*- coding: utf-8 -*-
import random

from config import config
from typing import NoReturn, Dict, Any, List
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
    """
    reads connection parameters from default database.ini file
    """
    params = config()
    con: connection = psycopg2.connect(**params)
    con.set_client_encoding('UTF8')
    return con


def fill_references(args):
    """
    Fills references in an InterMine-like schema:

    each table has an 'id' column of unique identifiers (PK)
    and can have reference to other tables in columns named '{referencedtablename}id'

    e.g. 'diagnosis' table has a column 'id', and a 'patientid' where it stores the id of the referenced patient
    (patient.id = diagnosis.patientid)

    queries a db schema
    """
    # connect to the PostgreSQL server
    conn = connection()
    try:
        # create a cursor
        cur = conn.cursor()
        # just show the db we are querying
        show_dbname(cur)
        # get tables and their sizes

        tables_sizes = get_tables_size(args, cur)

        update_with_ref = """
        update {}
        set {}id = {}
        where id = {}
        """
        for tn in tables_sizes.keys():
            print(tn.upper() + ": ")
            ref_ids = get_all_table_ids(cur, tn)
            ref_set = get_referenced_tables(cur, tn)
            if len(ref_set) == 0:
                continue

            for table_name in ref_set:
                print(table_name + "... ", end='')
                table_size = tables_sizes.get(table_name)
                # subs with size check
                if table_name not in tables_sizes:
                    print(table_name, "is empty, no reference can be added for it")
                    continue

                sampled_ref_id = random.choices(ref_ids, k=int(table_size))
                ids = get_all_table_ids(cur, table_name)

                """ this is not working
                cur.executemany(update_with_ref.format(table_name, trow[0], sampled_ref_id, ids))
                count = cur.rowcount
                print(count, "Records Updated successfully ")
                """
                counter = 0
                for i in ids:
                    cur.execute(update_with_ref.format(table_name, tn, sampled_ref_id[counter], i))
                    counter = counter + 1
                conn.commit()
            print("\n")
        print("**" * 20)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def get_referenced_tables(cur, tn):
    """

    :param cur:
    :param tn:
    :return:
    """

    q_referenced_tables = """
        SELECT table_name
        FROM   information_schema.columns
        WHERE  column_name like '{}id'
        """

    cur.execute(q_referenced_tables.format(tn))
    this_ref_set = cur.fetchall()
    ref_set = [item[0] for item in this_ref_set]
    return ref_set


def get_all_table_ids(cur, table_name):
    """
    get all the ids for the table

    :param cur: the db connection cursor
    :param table_name: the table name :)
    :return: the list of ids for the table
    """
    q_identifiers = """
    SELECT id
    FROM  {}
    """
    cur.execute(q_identifiers.format(table_name))
    id_set = cur.fetchall()
    ids = [item[0] for item in id_set]
    return ids


def get_tables_size(args, cur):
    """
    queries a postgres schema and get the list of non-empty tables

    :param
            - args (the command line args)
            - cur (the connection cursor)
    :returns
            - a list of table names
            - a dictionary {table_name; table_size}
    """
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
    table_names: list[str] = []
    tables_sizes = {}  # k=table name, v=table size
    for rec in class_counts:
        rows.append([rec[1], get_precision().format(rec[1] / den)])
        table_names.append(rec[0])
        tables_sizes.update({rec[0]: rec[1]})
    df_tsizes = pd.DataFrame(rows, columns=cols, index=table_names)
    print(df_tsizes)
    print()
    return tables_sizes


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
    fill_references(get_args())
