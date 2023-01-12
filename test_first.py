from datetime import date, datetime
import logging
import psycopg2
import pyodbc
from csv import writer
import pandas as pd


def test_1():
#  x=10
#  y=10
#  assert x==y
 import pyodbc
 import pandas as pd
 import psycopg2
 from csv import writer
 from datetime import datetime

# datetime.now() gives current date and time
# The strftime() method returns a string representing date and time using date, time or datetime object.

date = datetime.now().strftime('%Y %m %d %H %M  %S')

#  date=datetime.datetime.now().strftime('%Y%m%d%H%M%S_%f')

#  log_file = 'Path to the log file/Logs/log{}.log'.format(datetime.datetime.now().strftime('%Y%m%d%H%M%S_%f'))
#  date = logging.FileHandler(log_file)

# Change name of database as per county name
database = 'rutherford'

####################################################################################

# Postgres table list 
# setup postgres connection
conn = psycopg2.connect(
    database="aristanalyticsdb", user='techment@aristanalyticsdb', password='Sanjeev@123',
    host='aristanalyticsdb.postgres.database.azure.com', port='5432'
)

# conn.cursor will return a cursor object, you can use this cursor to perform queries
cur = conn.cursor()

# execute our Query
cur.execute("SELECT * from INFORMATION_SCHEMA.TABLES ")

# retrieve the records from the database
schema = cur.fetchall()

# creating table list
table_list_pos = []
for i in range(len(schema)):
    if schema[i][1] == database and schema[i][3] == 'BASE TABLE':
        table_list_pos.append(schema[i][2])

conn.close()

######################################################################################

# SQL Server Table list
cnxn_str = ("Driver={SQL Server Native Client 11.0};"
            "Server=aristpoc.database.windows.net;"
            f"Database={database};"
            "UID=techment@aristpoc;"
            "PWD=Sanjeev@123;")
cnxn = pyodbc.connect(cnxn_str)
cursor = cnxn.cursor()

cursor.execute("SELECT * from INFORMATION_SCHEMA.TABLES ")
schema = cursor.fetchall()

table_list_sql = []
for i in range(len(schema)):
    if (schema[i][0] == database and schema[i][3] == 'VIEW'):
        table_list_sql.append(schema[i][2])

cnxn.close()
########################################################################################
# print("----------------------------")
# print(type(table_list_sql))

table_list_sql = set(table_list_sql)
table_list_pos = set(table_list_pos)
table_list1 = list(table_list_sql & table_list_pos)
print(table_list1)
#print("-------")
#print(table_list_sql)
#print(type(table_list_sql))
with open(f'Report_{"Rep"}.csv', 'a', newline='') as f_object:
    List = ['Table Name', 'Postgres_Rows', 'Postgres_Columns', 'SQL_Rows', 'SQL_Columns', 'Difference in Column',
            'Difference in column records', 'Reason']
    writer_object = writer(f_object)
    writer_object.writerow(List)
    # Close the file object
    f_object.close()

for table in table_list1:
    pos_db = "rutherford"
    sql_db = "dbo"

    print(" ")
    print(f"Postgres Database: {database} Table: {table}")
    print(f"SQL Database: rutherford.{sql_db} Table: {table}")

    pos_query = f'SELECT * from {database}."{table}"'
    sql_query = f"SELECT * FROM {sql_db}.{table}"

    # Postgres Database
    conn = psycopg2.connect(
        database="aristanalyticsdb", user='techment@aristanalyticsdb', password='Sanjeev@123',
        host='aristanalyticsdb.postgres.database.azure.com', port='5432'
    )
    cur = conn.cursor()

    # Fetching all rows from the table
    cur.execute(pos_query)  # BATCH_PROCESS_DETAIL [LRCOwnership] [LRCParcel]
    result = cur.fetchall()
    pos = pd.DataFrame(result)

    # Fetching the column names
    field_names1 = [i[0] for i in cur.description]
    pos.columns = field_names1
    # pos

    # Closing the connection
    conn.close()

    # SQL Server Database
    cnxn_str = ("Driver={SQL Server Native Client 11.0};"
                "Server=aristpoc.database.windows.net;"
                f"Database={database};"
                "UID=techment@aristpoc;"
                "PWD=Sanjeev@123;")
    cnxn = pyodbc.connect(cnxn_str)
    cursor = cnxn.cursor()

    # Fetching all rows from the table
    cursor.execute(sql_query)  # vwrutherford [LRCResidentialBuilding]
    res = cursor.fetchall()  # [dbo].[BATCH_PROCESS_DETAIL]
    sql = pd.DataFrame((tuple(t) for t in res))

    # Fetching the column names
    field_names = [i[0] for i in cursor.description]
    sql.columns = field_names
    # sql

    df_check = pd.DataFrame()
    for i in sql.columns:
        df_check[i] = pos[i]

    for i in range(len(sql.columns)):
        if len(pd.unique(sql[sql.columns[i]])) == sql.shape[0]:
            break
    # q.sort_values(by=[q.columns[i]], ignore_index=True, inplace=True)
    sql.sort_values(by=[sql.columns[i]], ignore_index=True, inplace=True)
    df_check.sort_values(by=[sql.columns[i]], ignore_index=True, inplace=True)

    diff = sql.compare(df_check, align_axis=1)
    # diff
    diff.to_csv(f"Difference_{table}.csv")

    diff_col = []
    for i in range(0, len(diff.columns), 2):
        diff_col.append(diff.columns[i][0])

    records = []
    for i in sql.columns:
        diff_record = sql[i].compare(df_check[i], align_axis=1)
        if diff_record.shape[0] != 0:
            # print(i)
            # print(diff.shape[0])
            records.append(diff_record.shape[0])

    print("----------------------------------------------------------------------")
    if diff_col:

        print(f"Changes required in {diff_col}.")
        print("----------------------------------------------------------------------")
        print(" ")
        print("Changes are as follows: ")
        print(" ")
    else:
        print("Postgres and SQL data are same.")
    # if diff_col:
    #     print("Changes: ")
    # else:
    #     print("No change required in data.")

    reason = []
    for i in range(len(diff_col)):
        try:
            sql[diff_col[i]] = sql[diff_col[i]].dt.round('s')
            df_check[diff_col[i]] = df_check[diff_col[i]].dt.round('s')
            sql[diff_col[i]] = sql[diff_col[i]].dt.round('min')
            df_check[diff_col[i]] = df_check[diff_col[i]].dt.round('min')
            print(f"SQL column {[diff_col[i]]} changes to %Y%m%d %H%M%S format")
            print(f"Postgres column {[diff_col[i]]} changes to %Y%m%d %H%M%S format")
            print("==================================================================")
            reason.append('Date format')
            continue
        except:
            pass

        try:
            sql[diff_col[i]] = sql[diff_col[i]].str.strip()
            df_check[diff_col[i]] = df_check[diff_col[i]].str.strip()
            print(f"Stripped the string of SQL column {[diff_col[i]]}")
            print(f"Stripped the string of Postgres column {[diff_col[i]]}")
            print("=========================================================")
            reason.append('Spacing')
            continue
        except:
            pass

        try:
            sql[diff_col[i]] = sql[diff_col[i]].astype(float)
            sql[diff_col[i]] = sql[diff_col[i]].round(0).astype(int)
            df_check[diff_col[i]] = df_check[diff_col[i]].astype(float)
            df_check[diff_col[i]] = df_check[diff_col[i]].round(0).astype(int)
            print(f"Round off the SQL column {[diff_col[i]]}")
            print(f"Round off the Postgres column {[diff_col[i]]}")
            print("================================================")
            reason.append('Round off')
            continue
        except:
            pass

        try:
            print(f"Converted the SQL column {[diff_col[i]]} in integer format from {type(sql[diff_col[i]][0])} format")
            print(
                f"Converted the Postgres column {[diff_col[i]]} in integer format from {type(sql[diff_col[i]][0])} format")
            print(
                "==========================================================================================================")
            sql[diff_col[i]] = sql[diff_col[i]].astype(int)
            df_check[diff_col[i]] = df_check[diff_col[i]].astype(int)
            reason.append('Change Data type')
            continue
        except:
            pass
    with open(f'Report_{"Rep"}.csv', 'a', newline='') as f_object:
        List = [table, pos.shape[0], pos.shape[1], sql.shape[0], sql.shape[1], diff_col, records, reason]
        writer_object = writer(f_object)
        writer_object.writerow(List) 
        # Close the file object
        f_object.close()
    diff = sql.compare(df_check, align_axis=1)
    if diff.shape == (0, 0):
        print("End of execution")
    else:
        print("Data need to be treated again.")
    # break
    # sql.to_csv(f"SQL_{table}.csv")
    # df_check.to_csv(f"Postgres_{table}.csv")
