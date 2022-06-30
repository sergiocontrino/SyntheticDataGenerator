#!/usr/bin/python

import psycopg2
import pyodbc
from configparser import ConfigParser


def config(vendor):
    conf_file = 'datasynth/database.ini'

    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(conf_file)

    # set section in the ini file, default to postgresql
    if vendor == "ms":
        section = 'mssqlserver'
    else:
        section = 'postgresql'

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, conf_file))
    return db


def connection(vendor):
    # read connection parameters
    params = config(vendor)
    if vendor == "pg":
        con: connection = psycopg2.connect(**params)
        con.set_client_encoding('UTF8')
    else:
        con: connection = pyodbc.connect(**params)
    return con


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


# --------------------------------
# give the list of tables and their sizes in the db
# some tables excluded
# sizes used to scale the amount of synth data produced
# --------------------------------

pg_tables_size = """
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

ms_tables_size = """
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
AND [TableName] not in (""" + get_excluded_tables() + """) 
AND [RowCount] > 0
GROUP BY 
sOBJ.schema_id
, sOBJ.name
ORDER BY [RowCount] DESC
            """

#
# make a list of a table columns
# note exceptions, (mostly no identifiers) based on an intermine-like schema
#

pg_columns = """
SELECT column_name, data_type
FROM   information_schema.columns
WHERE  table_name = %s
AND column_name not like '%%id'
AND column_name not IN ('class', 'identifier')
ORDER  BY ordinal_position
        """

ms_columns = """
USE dbname;
SELECT sys.columns.name
FROM sys.objects INNER JOIN sys.columns
ON sys.objects.object_id = sys.columns.object_id
WHERE sys.objects.type = 'U'
AND sys.objects.name = %s
AND sys.columns.name not in ('class', 'identifier');
AND sys.columns.name not like '%%id'
ORDER  BY 1
        """

# --------------------------------
# give a categorical break down of the values for a column of a table
# this query should be db vendor independent
# TODO: verify on ms
# --------------------------------

columns_count = """
select {}, count(1) 
from {}
group by 1
having count(1) >= {} 
order by 2 desc
        """
