# !/usr/bin/python
# -*- coding: utf-8 -*-
import random
from typing import NoReturn
from get_args import get_args
import pandas as pd
import queries as q
import pyodbc


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

    db_vendor = "ms"

    # Connect to the database server
    # conn = q.connection(db_vendor)

    # Some other example server values are
    # server = 'localhost\sqlexpress' # for a named instance
    # server = 'myserver,port' # to specify an alternate port
    server = 'CPFT-CRATE-P01'
    database = 'M00999_CCC'
    username = 'XX'
    password = 'XX'
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    # conn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')
    # conn.setencoding('latin1')
    try:
        # create a cursor
        cur = conn.cursor()

        # get the tables size (nr of rows)
        df_tsizes = get_tables_size(args, cur)
        print(df_tsizes)
        print()

        # types are not strictly needed
        # TODO remove type, add get_excluded_columns?

        target = 1000
        threshold = 10
        seed = args.seed
        unseeded = args.no_seed

        ms_columns = """SELECT COLUMN_NAME
        FROM M00999_ccc.INFORMATION_SCHEMA.TABLES t INNER JOIN  M00999_ccc.INFORMATION_SCHEMA.COLUMNS c
        ON t.TABLE_NAME = c.TABLE_NAME
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        AND t.TABLE_NAME = ?
        """

        # for each table
        for t in df_tsizes.index:
            t_size = int(df_tsizes.loc[t].at["rows"])
            table = fix_name(t)
            print("==", table)

            # initialise the df of synth data for the table
            syn_table = pd.DataFrame()

            # get the columns (and their types)
            cur.execute(ms_columns, (table,))
            columns = cur.fetchall()
            print("--", columns)
            # for each column get the all the values and their count
            for column in columns:
                # get the counts
                cn = str(column)
                cname = fix_cname(cn)
                print("--colname:", cname)
                cols_count = value_counter(cur, table, cname, threshold)
                # build the synthetic column
                syn_col = build_synth_col(t_size, cols_count, target, seed, unseeded)
                # add it to the data frame
                syn_table[column[0]] = syn_col

            # scale the table's data according to original size
            # get scaling factor
            scaling_factor = int(float(df_tsizes.loc[table, 'ratio']) * target)
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

        # close the communication with db
        cur.close()
    except (Exception, get_db_error(args)) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def fix_name(name):
    tok = name.split('.')
    return tok[1].strip('[]')


def fix_cname(name):
    return str(name.strip('()\' ,'))


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

    note: - by default threshold =1, i.e. no filtering of rare occurring values is done
    """

    ms_columns_count_or = """
    select ?, count(*)
    from ?
    group by ?
    having count(*) >= 10
    order by 2 desc
            """

    ms_columns_count = "select " + column + ", count(*) from " + table + \
                       "group by " + column + "having count(*) >= 10 order by 2 desc"

    # cur.execute(q.columns_count.format(column[0], table, threshold))
    # cur.execute(ms_columns_count, column, table, column)
    #data = [column, table, column]
    #data = ([(column, ), (table, ), (column, )], )

#    print(ms_columns_count)
    cur.execute(ms_columns_count)

    cols_count = cur.fetchall()
    return cols_count


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


def get_tables_size(args, cur):
    """
    queries the db to get the tables and their size

    :param args:
    :param cur:
    :return: data frame with table size
    """

    # scaling_class = "[dbo].[PERSON_DIM]"
    scaling_class = "PERSON_DIM"

    ms_tables_size_1 = """
    SELECT
    QUOTENAME(SCHEMA_NAME(sOBJ.schema_id)) + '.' + QUOTENAME(sOBJ.name) AS [TableName]
    , SUM(sPTN.Rows) AS [RowCount]
    FROM
    sys.objects AS sOBJ
    INNER JOIN sys.partitions AS sPTN
    ON sOBJ.object_id = sPTN.object_id
    WHERE
    sOBJ.type = 'U'
    AND sOBJ.is_ms_shipped = 0x0
    AND index_id < 2 -- 0:Heap, 1:Clustered
    GROUP BY
    sOBJ.schema_id, sOBJ.name
    HAVING SUM(sPTN.Rows) > 0
    ORDER BY SUM(sPTN.Rows) DESC
                """

    ms_tables_size = """
    SELECT
    sOBJ.name, SUM(sPTN.Rows)
    FROM
    sys.objects AS sOBJ
    INNER JOIN sys.partitions AS sPTN
    ON sOBJ.object_id = sPTN.object_id
    WHERE
    sOBJ.type = 'U'
    AND sOBJ.is_ms_shipped = 0x0
    AND index_id < 2 -- 0:Heap, 1:Clustered
    GROUP BY
    sOBJ.schema_id, sOBJ.name
    HAVING SUM(sPTN.Rows) > 0
    ORDER BY SUM(sPTN.Rows) DESC
                """

    tables_size = ms_tables_size
    cur.execute(tables_size)
    class_counts = []  #
    table_row = cur.fetchall()
    for row in table_row:
        if row[0] == scaling_class:
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


if __name__ == '__main__':
    sample(get_args())
