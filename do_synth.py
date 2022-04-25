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
            #if row[0] == get_scaling_class():
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
            cur.execute(columns_types, (trow[0],))
            column_type = cur.fetchall()
            for crow in column_type:
                # do int -> summary stat, date ->?
                # quick temp fix to check date columns
                # TODO: remove condition!
                if not crow[0].endswith("dates"):
                    tcols.append(crow[0])
                    cur.execute(columns_counts.format(crow[0], trow[0]))
                    column_count = cur.fetchall()
                    for ccrow in column_count:
                        rows.append([trow[0], crow[0], crow[1], "\"" + str(ccrow[0]) + "\"",
                                     get_precision().format(ccrow[1] / trow[1] * 100)])
            collist = ", ".join(tcols)

            cur.execute(table_export.format(collist, trow[0]))
            tab_exp = cur.fetchall()

            qq = pd.DataFrame(tab_exp, columns=tcols)
            #print(qq)

            # scale the different tables according to original size.
            # get scaling factor
            scaling_factor = int(float(df_tsizes.loc[trow[0], 'ratio']) * args.target_size)
            print(trow[0], ":  sampling ", scaling_factor, " records for columns: >>", collist)

            # dump csv file of sampled data
            qq.sample(n=scaling_factor, random_state=1, replace=True).to_csv('{0}.csv'.format(trow[0]), index=False)

            col_dict.update({trow[0]: tcols})

        summaries = pd.DataFrame(rows, columns=cols)

        # print df to file
        summaries.to_csv("summaries.csv")
        print()

        """ 1 df per table, do sample, 
        n is scaling size for the scaling class, the other number are derived by the table counts)
        to cvs result        
        """

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
