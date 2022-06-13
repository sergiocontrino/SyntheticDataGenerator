# !/usr/bin/python
# -*- coding: utf-8 -*-
import random

from config import config
from filter import filter_common_categories
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

        rows = []  # used for debug
        target = args.target_size
        threshold = args.filter_threshold

        # for each table
        for table in df_tsizes.index:
            # if table != "contact":
            #     continue

            t_size = int(df_tsizes.loc[table].at["rows"])
            t_cols = []

            syn_table = pd.DataFrame()

            # get the columns and their types
            cur.execute(columns_types, (table,))
            column_type = cur.fetchall()
            # for each column get the all the values and their count
            # print(column_type)
            for column in column_type:
                t_cols.append(column[0])
                # print("--", t_cols, t_size, column)
                cols_count = value_counter(cur, table, column)
                # print(cols_count)
                syn_col = build_synth_col(table, t_size, column, cols_count, target)
                syn_table[column] = syn_col
            col_list = ", ".join(t_cols)

            print("==" *20)
            print(syn_table)
            print("--" *20)

            # # ..and put it in a data frame
            # qq1 = pd.DataFrame(tab_exp, columns=t_cols)
            # print(table, "-->", qq1.shape)
            #
            # # filter rarely occurring values if the filter threshold is > 1
            # threshold = args.filter_threshold
            # if threshold > 1:
            #     qq = filter_common_categories(qq1, threshold)
            # else:
            #     qq = qq1

            # scale the table's data according to original size
            # get scaling factor
            scaling_factor = int(float(df_tsizes.loc[table, 'ratio']) * args.target_size)
            print(table, ":  sampling ", scaling_factor, " records for columns: >>", col_list)

            # dump csv file of sampled data
            if len(syn_table) > 0:
                syn_table.to_csv('{0}.csv'.format(table), index=False)

                if args.no_seed:
                    syn_table.sample(n=scaling_factor, replace=True).to_csv('{0}scaled.csv'.format(table), index=False)
                else:
                    syn_table.sample(n=scaling_factor, random_state=args.seed, replace=True).to_csv('{0}sscaled.csv'.format(table),
                                                                                             index=False)
            else:
                print("WARNING: table", table, "is now empty! Try reducing the threshold for common values, now",
                      threshold)

        # # temp: dump a debug summary
        # cols = ['table', 'attribute', 'type', 'value', 'count']
        # summaries = pd.DataFrame(rows, columns=cols)
        # # print df to file
        # summaries.to_csv("summaries.csv")
        # print()

        # close the communication with PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')



def value_counter(cur, table, column):
    columns_counts = """
            select {}, count(1) 
    from {}
    group by 1 
    order by 2 desc
            """

    cur.execute(columns_counts.format(column[0], table))
    column_count = cur.fetchall()
    return column_count

    # for ccrow in column_count:
    #     print(column[0])
    #     rows.append([table, column[0], column[1], "\"" + str(ccrow[0]) + "\"",
    #                  get_precision().format(ccrow[1] / t_size * 100)])

def value_counter_o(column, cur, rows, t_size, table):
    columns_counts = """
            select {}, count(1) 
    from {}
    group by 1 
    order by 2 desc
            """

    cur.execute(columns_counts.format(column[0], table))
    column_count = cur.fetchall()
    print(column_count)
    for ccrow in column_count:
        print(column[0])
        rows.append([table, column[0], column[1], "\"" + str(ccrow[0]) + "\"",
                     get_precision().format(ccrow[1] / t_size * 100)])


def build_synth_col(table, table_size, column, cols_count, target_size):
    """
    input:
    table: contact
    table_size: 90426
    column: patientattended
    col_counts: [(True, 73906), (False, 15470), (None, 1050)]
    target_size: 100

    [table, column, column_type, value, frequency (percentage)]
    ['currentview', 'attendancedifficulties', 'boolean', '"True"', '33.15']

    """
    # target_size = 100
    col = []
    added = 0

    for line in cols_count:
        this_value = line[0]
        this_count = line[1]
        tg_count = int(this_count * target_size/table_size)
        # print(this_value, this_count, tg_count, table_size, target_size)
        added += tg_count
        for t in range(tg_count):
            col.append(this_value)

    if added < target_size:
        print(target_size-added)
        for r in range (target_size - added):
            col.append(this_value)

    random.shuffle(col)

    cc = pd.DataFrame({column: col})
    print(cc)

    return col

    # if i == 0:
    #         build_output(line, synthetic_col, target_size)
    #         # add_record(count, line, col, value)
    #         i = i + 1
    #     else:
    #         prev_col = col[len(col) - 1]
    #         if this_col != prev_col:
    #             # the RF changed, let's output the previous one
    #
    #             # - shuffle the synth list
    #             random.shuffle(synthetic_col)
    #             # - just some screen messages
    #             verbose_output(prev_col, synthetic_col)
    #
    #             # - make a df for export a csv list
    #             # TODO: there must be a better way...
    #             d = {"value": synthetic_col}
    #             pd.DataFrame(d).to_csv('{0}.csv'.format(col[len(col) - 1]), index=False)
    #
    #             # empty lists
    #             count, col, synthetic_col, value = [], [], [], []
    #
    #             # start anew with current RF
    #             build_output(line, synthetic_col, args)
    #             # add_record(count, line, col, value)
    #         else:
    #             # same RF: just append
    #             build_output(line, synthetic_col, args)
    #             # add_record(count, line, col, value)

    # for line in cols_count:
    #     this_col = line.split(',')[1]
    #     if i == 0:
    #         build_output(line, synthetic_col, target_size)
    #         # add_record(count, line, col, value)
    #         i = i + 1
    #     else:
    #         prev_col = col[len(col) - 1]
    #         if this_col != prev_col:
    #             # the RF changed, let's output the previous one
    #
    #             # - shuffle the synth list
    #             random.shuffle(synthetic_col)
    #             # - just some screen messages
    #             verbose_output(prev_col, synthetic_col)
    #
    #             # - make a df for export a csv list
    #             # TODO: there must be a better way...
    #             d = {"value": synthetic_col}
    #             pd.DataFrame(d).to_csv('{0}.csv'.format(col[len(col) - 1]), index=False)
    #
    #             # empty lists
    #             count, col, synthetic_col, value = [], [], [], []
    #
    #             # start anew with current RF
    #             build_output(line, synthetic_col, args)
    #             # add_record(count, line, col, value)
    #         else:
    #             # same RF: just append
    #             build_output(line, synthetic_col, args)
    #             # add_record(count, line, col, value)


    # the last rf
    #    d = {"value": value, "count": count}



    # random.shuffle(synthetic_col)
    # d = {"value": synthetic_col}
    # qq = pd.DataFrame(d)
    # qq.to_csv('{0}.csv'.format(col[len(col) - 1]), index=False)
    #
    # print("*" * 20)
    # verbose_output(col[len(col) - 1], synthetic_col)

    # return col, value, count


def build_output(line, risk_output, target_size):
    """
    add the value in synth data, proportionally to its original frequency,

    :param line:
    :param risk_output:
    :param args:
    :return:
    """

    # count/tot
    pc = float((line.split(',')[4]))

    # number of items of value value in the synth rf list
    rr = int(pc/100 * target_size)
    for t in range(rr):
        risk_output.append(line.split(',')[3])


def verbose_output(col, synthetic_col):
    # TODO: case insensitive null
    print("-" * 20)
    print("Column", col, ": size of synth data set =", len(synthetic_col), "with",
          synthetic_col.count("NULL"), "null values")
    print(synthetic_col)




def add_record(count, line, risk, value):
    """
    not needed!
    :param count:
    :param line:
    :param risk:
    :param value:
    :return:
    """
    risk.append(line.split(',')[0])
    value.append(line.split(',')[2])
    count.append(int(line.split(',')[3]))


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
