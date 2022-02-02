# !/usr/bin/python
# -*- coding: utf-8 -*-
from typing import NoReturn

import psycopg2
from config import config

import argparse

import pandas as pd
import numpy as np


def main() -> NoReturn:
    """

    :return: none
    """

"""
    parser = argparse.ArgumentParser(description=__doc__)

    #    parser.add_argument(
    #        "input", nargs="?", default="-",
    #        metavar="INPUT_FILE", type=argparse.FileType("r"),
    #        help="path to the input file (read from stdin if omitted)")

    parser.add_argument(
        "output", nargs="?", default="-",
        metavar="OUTPUT_FILE", type=argparse.FileType("w"),
        help="path to the output file (write to stdout if omitted)")

    args = parser.parse_args()
"""


def get_scaling_class() -> str:
    """ the class we use to scale the db """
    scaling_class = "patient"
    return scaling_class


def get_scaling_size() -> int:
    """ the class we use to scale the db """
    scaling_size = 5000
    return scaling_size


def get_excluded_tables() -> str:
    """ the list of tables we don't want to consider """
    exclusion_list = "'tracker', 'intermineobject', 'intermine_metadata', 'executelog'"
    return exclusion_list


def get_precision() -> str:
    """ the number of decimals we want to keep """
    precision = """{:.2f}"""
    return precision


def connection():
    # read connection parameters
    params = config()
    con: connection = psycopg2.connect(**params)
    con.set_client_encoding('UTF8')
    return con


    #con: connection = psycopg2.connect(
    #    dbname='ithrivemine',
    #    user='modmine',
    #    password='modmine',
    #    host='localhost',
    #    port=5432
    #)

def get_stats():
    """ Connect to the PostgreSQL database server """
    conn = connection()
    try:
        # connect to the PostgreSQL server

        # create a cursor
        cur = conn.cursor()

        get_dbname(cur)

        class_counts = []  # not necessary
        counts = []  # to hold the summaries (for all tables)
        df = pd.DataFrame()

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

        table_row = cur.fetchall()
        for row in table_row:
            if row[0] == get_scaling_class():
                den = row[1]
            class_counts.append(row)

        cols = ['rows', 'ratio']
        rows = []
        ind = []
        for rec in class_counts:
            rows.append([rec[1], get_precision().format(rec[1]/den)])
            ind.append(rec[0])
        aa = pd.DataFrame(rows, columns=cols, index=ind)
        print(aa)

        print()

        columns_types = """
        SELECT column_name, data_type
FROM   information_schema.columns
WHERE  table_name = %s
AND column_name not like '%%id'
AND column_name not IN ('class', 'identifier')
ORDER  BY ordinal_position
        """

        columns_counts = """
        select {}, count(1) 
from {}
group by 1 
order by 2 desc
        """

        cols = ['table', 'attribute', 'type', 'value', 'count']
        rows = []

        col_dict = {}    # TODO: get directly from df cc


        for trow in table_row:
            #print(">>>", trow[0])

            tcols = []
            cur.execute(columns_types, (trow[0],))
            column_type = cur.fetchall()
            for crow in column_type:
                # do int -> summary stat, date ->?
                if not crow[0].endswith("date"):
#                    tcols.append([trow[0], crow[0]])
                    tcols.append(crow[0])
                    cur.execute(columns_counts.format(crow[0], trow[0]))
                    column_count = cur.fetchall()
                    for ccrow in column_count:
                        rows.append([trow[0], crow[0], crow[1], "\"" + str(ccrow[0]) + "\"",
                                     get_precision().format(ccrow[1] / trow[1] * 100)])
                        # cc.loc(len(cc)) = [trow[0], crow[0], crow[1], "\"" + str(ccrow[0]) + "\"",
                        #      "{:.2f}".format(ccrow[1]/trow[1] * 100)]

                        # print(trow[0], crow[0], crow[1], "\"" + str(ccrow[0]) + "\"",
                        #      "{:.2f}".format(ccrow[1]/trow[1] * 100))
            col_dict.update({trow[0]: tcols})

        cc = pd.DataFrame(rows, columns=cols)
        print(cc)
        # print df to file
        cc.to_csv("summary.csv")

#        for trow in table_row:
#            print("<><><>", trow[0])

        print()
        print(col_dict)
        print()

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def get_db_version(cur):
    print('PostgreSQL database version:')
    cur.execute('SELECT version()')
    db_version = cur.fetchone()
    print(db_version)


def get_dbname(cur: connection()) -> NoReturn:
    """
    print header with database in use
    """
    cur.execute('SELECT current_database()')
    db_name = cur.fetchone()[0]
    print("==" * 20)
    print('Connecting to database:', db_name)
    print("==" * 20)


if __name__ == '__main__':
    get_stats()
