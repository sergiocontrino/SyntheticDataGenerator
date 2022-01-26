# !/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
from config import config

import pandas as pd


def connection():
    # read connection parameters
    params = config()
    con: connection = psycopg2.connect(**params)
    con.set_client_encoding('UTF8')
    return con

    """
    con: connection = psycopg2.connect(
        dbname='ithrivemine',
        user='modmine',
        password='modmine',
        host='localhost',
        port=5432
    )
    """


def get_stats():
    """ Connect to the PostgreSQL database server """
    conn = connection()
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

        class_counts = []  # not necessary
        counts = []    # to hold the summaries (for all tables)
        df = pd.DataFrame()

        tables_rows = """
        SELECT relname,reltuples
FROM pg_class C
LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
WHERE 
  nspname NOT IN ('pg_catalog', 'information_schema') 
  AND relname NOT IN ('tracker', 'intermineobject', 'intermine_metadata') 
  AND relkind='r' 
  and reltuples > 0
ORDER BY reltuples DESC
        """

        cur.execute(tables_rows)

        table_row = cur.fetchall()
        for row in table_row:
            class_counts.append(row)
            #print(row)

        print("--" * 20)
        print(class_counts)
        print("--" * 20)

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

        for trow in table_row:
            print("==" * 10, trow[0])
            cur.execute(columns_types, (trow[0],))
            column_type = cur.fetchall()
            for crow in column_type:
                print(crow)
                # do int -> summary stat, date ->?
                if not crow[0].endswith("date"):
                    cur.execute(columns_counts.format(crow[0], trow[0]))
                    column_count = cur.fetchall()
                    for ccrow in column_count:
                        print(trow[0], crow[0], "\"" + str(ccrow[0]) + "\"", "{:.2f}".format(ccrow[1]/trow[1] * 100))
                        #print (trow[0], crow[0], ccrow, ccrow[1]/trow[1] * 100)
        print("-+" * 20)

        print(counts)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    get_stats()
