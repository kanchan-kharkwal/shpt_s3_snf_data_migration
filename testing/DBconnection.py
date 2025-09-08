import json
import pathlib
##import pyodbc
import snowflake.connector as sf
from snowflake.connector.pandas_tools import pd_writer, write_pandas
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

import serviceaccountcon

# Abhishek
def db_connections(sourcedb, user):
    snowflake_conn=None
    connection_path = "./"
    if sourcedb == "SNOWFLAKE":
        try:
            snowflake_conn = serviceaccountcon.get_eversana_secure_connection()  # 04-10-2022
            print(snowflake_conn)
            print("Connected Successfully to {}...".format(sourcedb))
        except Exception as e:
            print(e)
        # print("Connected Successfully to {}...".format(sourcedb))
        return snowflake_conn
    # 05-01-2023
    elif sourcedb == "SNOWFLAKE_WEST":
        try:
            snowflake_west_conn = serviceaccountcon.get_eversana_secure_connection_WEST()
            print("Connected Successfully to {}...".format(sourcedb))

        except Exception as e:
            print(e)
        # print("Connected Successfully to {}...".format(sourcedb))
        return snowflake_west_conn
    # 05-01-2023
    # elif sourcedb == "SQLSERVER":
    #     # # added user=Service in data.xlsx file. it will fail,
    #     # # need to change below code accordingly like user=Abhishek in data.xlsx file
    #     # connection = json.loads(open(str(connection_path + "{}_Connection_Strings.json".format(user))).read())
    #     # SQL_Driver = (connection["SQLSERVER"]["Driver"],)
    #     # SQL_Server = (connection["SQLSERVER"]["Server"],)
    #     # SQL_Database = (connection["SQLSERVER"]["Database"],)
    #     # SQL_Trusted_Connection = connection["SQLSERVER"]["Trusted_Connection"]

    #     # sqlserver_conn = pyodbc.connect(
    #     #     Driver=SQL_Driver[0],
    #     #     Server=SQL_Server[0],
    #     #     Database=SQL_Database[0],
    #     #     Trusted_Connection=SQL_Trusted_Connection,
    #     # )
    #     # print("Connected Successfully to {}...".format(sourcedb))
    #     # return sqlserver_conn
    else:
        print("Not Connected ...")
 