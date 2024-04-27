from sqlalchemy.engine import URL
from sqlalchemy import create_engine, event
import pyodbc
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import String
from urllib.parse import quote_plus
from psycopg2 import extras


#dotenv
import os
from dotenv import load_dotenv, dotenv_values
load_dotenv()

SERVER_SQL = os.getenv('SERVER_SQL')
DATABASE_SQL = os.getenv('DATABASE_SQL')
USER_SQL = os.getenv('USER_SQL')
PASSWORD_SQL = os.getenv('PASSWORD_SQL')


#REDSHIFT AWS SERVER
SERVER_SQL_AWS = os.getenv('HOST_RS')
DATABASE_SQL_AWS = os.getenv('DATABASE_RS')
USER_SQL_AWS = os.getenv('USER_RS')
PASSWORD_SQL_AWS = os.getenv('PASSWORD_RS')




#EMP HYDRA
SERVER_CAT = os.getenv('SERVER_CAT')
DATABASE_CAT = os.getenv('DATABASE_CAT')
USER_CAT = os.getenv('USER_CAT')
PASSWORD_CAT = os.getenv(r'PASSWORD_CAT')

#HMO HYDRA
SERVER_CAT2 = os.getenv('SERVER_CAT2')
DATABASE_CAT2 = os.getenv('DATABASE_CAT2')
USER_CAT2 = os.getenv('USER_CAT2')
PASSWORD_CAT2 = os.getenv(r'PASSWORD_CAT2')

#GSO HYDRA
SERVER_CAT3 = os.getenv('SERVER_CAT3')
DATABASE_CAT3 = os.getenv('DATABASE_CAT3')
USER_CAT3 = os.getenv('USER_CAT3')
PASSWORD_CAT3 = os.getenv(r'PASSWORD_CAT3')

EDGE_USR = os.getenv('USER_EDGE')
EDGE_PASS = os.getenv('PASSWORD_EDGE')
#new note x
    ####COPY PASTE ESTO

# """
# used for creating a SQL Table "sql_tables_management" that will append the data of the
#  status of all the tables in the database, this script should run once a week 7 days a week.
#
# Author: Omar Sabag
# Date: 2023-08-29
# """
#
#




# import sys
# sys.path.append(r'C:\Users\TE289165\PycharmProjects\untitled\sabaglibraries')
# sys.path.append(r'C:\Users\TE289165\github\sbg_te_server1\sabaglibraries')
# sys.path.append(r'C:\Users\TE570809\OneDrive - TE Connectivity\github repo\sbg_te_server1\sabaglibraries')
# from sabagclass import SqlConn, CheckIfRun, SqlUtils, SqlUpload, SqlConnCAT, SqlConnAWS, SqlUploadAWS, CheckIfRunV2, SqlConnEdge
#
#
# #server main path (uncomment this when on the server)
# server_path = r'c:\sabag'
# server_path2 = r'C:\Users\TE289165\Desktop\Omar Sabag\MACROS\PRODUCTION\Data Analyst\SQLDATA'
#
# #anne computer path
# # server_path = r'C:\Users\TE570809\OneDrive - TE Connectivity\file dump\fig'
# # server_path2 = r'C:\Users\TE570809\OneDrive - TE Connectivity\file dump\MACROS\PRODUCTION\Data Analyst\SQLDATA'
#
#


## SQL CONNECTION TO EDGE REDSHIFT
# SqlConnEdge = SqlConnEdge()
# connEdge = SqlConnEdge.get_conn()


# #SQL CONNECTION
# sql_conn = SqlConn()
# conn = sql_conn.get_conn()

# #SQL CONNECTION AWS REDSHIFT
# sql_conn_aws = SqlConnAWS()
# connaws = sql_conn_aws.get_conn()
#
# #uploading sql MSSQL
# sql_upload = SqlUpload()
#
# #uploading sql AWS
# sql_upload_aws = SqlUploadAWS()
#
#
#
#
#
#
#     #for inserting data into scripts in production
# sql_utils = SqlUtils()
# starttimestamp, startfecha = sql_utils.get_timestamp()
#
# script = 'test.py'
# print(script)
# serialno = 99
# types = 'ETL'
#
# SqlUtils.insert_start_record(starttimestamp, startfecha, script, serialno, types)
# # SqlUtils.insert_error_record('error', script, starttimestamp)
# # SqlUtils.insert_finish_record(script, starttimestamp)

# #Check if another script has run successfully
# check_if_run = CheckIfRunV2(conn, SqlUtils)
# #check_if_run.check_if_run('ERROR MESSAGE', 'WHICH SCRIPT CHECK IF RUN', script, starttimestamp, 'TIME TO CHECK LAST RUN')
# check_if_run.check_if_run('ALL_MB51_prod.py didnt run', 'ALL_MB51_prod.py', script, starttimestamp, 29)


##SQL CONNECTION HYDRA
# sql_conn = SqlConnCAT()
# connCat = sql_conn.get_connection()



### UPLOAD TO MSSSQL
# sql_upload.upload_dataframe(df,
#                             table_name='testtable2',
#                             chunksize=5000,
#                             index=False,
#                             if_exists='append')


### UPLOAD TO SQL AWS REDSHIFT
# sql_upload_aws.upload_dataframe(df,
#                             table_name='testtable2',
#                             chunksize=5000,
#                             index=False,
#                             if_exists='append')




# #FOR Individual INSERTS:
# #update excecute
# sql_query = """
#     delete from all_zbmt_streamlit
# """
# conn = sql_conn.get_conn()
# with conn.connect() as conn:
#     conn.exec_driver_sql(sql_query)


class SqlUpload:
    """
    A class to manage connections and data uploading to a SQL Server database using SQLAlchemy.

    This class provides methods to establish a connection to a SQL Server database using SQLAlchemy,
    configure the connection with fast_executemany optimization, and upload DataFrame data to specified tables.

    :param driver: The ODBC driver to use for the connection. Default is '{ODBC Driver 17 for SQL Server}'.
    :param server: The server name.
    :param database: The database name.
    :param uid: The username for authentication.
    :param pwd: The password for authentication.


    ------------------------HOW TO USE IT ------------------------
        # Instantiate SqlConn to get connection

        #upload into SQL

        sql_upload.upload_dataframe(df,
                                    table_name='testtable2',
                                    chunksize=5000,
                                    index=False,
                                    if_exists='append')



                                    ,
                                    dtype={'material': String(40)})

    """

    def __init__(self, driver='{ODBC Driver 17 for SQL Server}', server=SERVER_SQL, database=DATABASE_SQL,
                 uid=USER_SQL, pwd=PASSWORD_SQL):
        """
        Initialize a SqlConn instance with connection details.

        :param driver: The ODBC driver to use for the connection.
        :param server: The server name.
        :param database: The database name.
        :param uid: The username for authentication.
        :param pwd: The password for authentication.
        """
        self.connection_string = f"DRIVER={driver};" \
                                 f"SERVER={server};" \
                                 f"DATABASE={database};" \
                                 f"UID={uid};" \
                                 f"PWD={pwd}"
        # self.engine = create_engine(f"mssql+pyodbc:///?odbc_connect={self.connection_string}")
        self.engine = create_engine(f"mssql+pyodbc://{uid}:{pwd}@{server}/{database}?driver=ODBC Driver 17 for SQL Server")



        # Configure fast_executemany
        @event.listens_for(self.engine, "before_cursor_execute")
        def receive_before_cursor_execute(
                conn, cursor, statement, params, context, executemany
        ):
            if executemany:
                cursor.fast_executemany = True

    def get_engine(self):
        """
        Get the SQLAlchemy engine associated with the connection.

        :return: The SQLAlchemy engine.
        """
        return self.engine


    def upload_dataframe(self, df, table_name, chunksize=10000, index=False, if_exists='replace', dtype=None):
        """
        Upload a DataFrame to a specified table in the database.

        :param df: The DataFrame to upload.
        :param table_name: The name of the table to upload the DataFrame to.
        :param chunksize: The number of rows to insert in each batch.
        :param index: Whether to include the DataFrame index as a column.
        :param if_exists: How to behave if the table already exists ('fail', 'replace', 'append').
        :param dtype: Data type mappings for the columns (e.g., {'column_name': String(50)}).


        """
        try:
            with self.engine.connect() as conn:
                df.to_sql(
                    name=table_name,
                    chunksize=chunksize,
                    con=self.engine,
                    index=index,
                    if_exists=if_exists,
                    dtype=dtype
                )
        except Exception as e:
            print(e)
            exit()
            print('t')


class SqlUploadRedshift:
    """
    A class to manage connections and data uploading to AWS Redshift directly using psycopg2,
    targeting the 'ss_rs_ts_aut_americas_analyst' schema by default. Automatically fetches Redshift
    credentials from environment variables.

    Usage example:
    sql_upload_redshift = SqlUploadRedshift()
    sql_upload_redshift.upload_dataframe(df, table_name='your_table', if_exists='replace')
    """

    def __init__(self):
        # Fetch Redshift credentials from environment variables
        self.host = SERVER_SQL_AWS
        self.database = DATABASE_SQL_AWS
        self.user = USER_SQL_AWS
        self.password = PASSWORD_SQL_AWS # URL-encode the password
        self.port = 5439  # Default port for Redshift
        self.schema = 'ss_rs_ts_aut_americas_analyst'  # Default schema

        self.connection_details = {
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port
        }

    def _get_connection(self):
        """Establishes a connection to the Redshift database."""
        return psycopg2.connect(**self.connection_details)

    def convert_df(self, df):
        """
        Converts all columns in the DataFrame from numpy types to native Python types.
        """
        for column in df.columns:
            # Apply conversion for each element in the column
            df[column] = df[column].apply(lambda x: x.item() if isinstance(x, (np.generic, np.ndarray)) else x)
        return df

    def upload_dataframe(self, df, table_name, if_exists='replace'):
        """
        Uploads a DataFrame to a specified table within the 'ss_rs_ts_aut_americas_analyst' schema
        in Redshift using psycopg2 for efficient bulk inserts. Converts numpy types to Python types
        to avoid psycopg2.ProgrammingError.

        :param df: The DataFrame to upload.
        :param table_name: The name of the table to upload the DataFrame to.
        :param if_exists: How to behave if the table already exists ('fail', 'replace', 'append').
        """
        conn = self._get_connection()
        cur = conn.cursor()

        # Convert DataFrame types before upload
        df = self.convert_df(df)

        full_table_name = f"{self.schema}.{table_name}"

        if if_exists == 'replace':
            cur.execute(f"DROP TABLE IF EXISTS {full_table_name};")
            conn.commit()

        df = self.convert_df(df)  # Convert DataFrame types before upload

        # Convert DataFrame to a list of tuples
        tuples = [tuple(x) for x in df.to_records(index=False)]

        # Generate column names for the INSERT INTO statement
        cols = ', '.join([f'"{col}"' for col in df.columns])  # Safeguard column names with double quotes
        placeholders = ', '.join(['%s' for _ in df.columns])  # Generate placeholders
        query = f"INSERT INTO {full_table_name} ({cols}) VALUES ({placeholders})"  # Use placeholders

        # Using execute_batch for efficient bulk inserts
        extras.execute_batch(cur, query, tuples)
        conn.commit()

        cur.close()
        conn.close()



class SqlUploadAWS:
    """
    A class to manage connections and data uploading to AWS Redshift using SQLAlchemy, targeting a specific schema.

    Parameters:
    - host: The Redshift cluster endpoint.
    - database: The Redshift database name.
    - user: The username for authentication.
    - password: The password for authentication.
    - port: The port Redshift is running on, usually 5439.

    Usage example:
    sql_upload_aws = SqlUploadAWS(host='your_host', database='your_database', user='your_user', password='your_password', port=5439)
    sql_upload_aws.upload_dataframe(df, table_name='your_table', chunksize=5000, index=False, if_exists='append')
    """
    #this is because the password contains an @ and there is a string on create_engine that requires a @
    password = quote_plus(PASSWORD_SQL_AWS)
    def __init__(self, host=SERVER_SQL_AWS, database=DATABASE_SQL_AWS, user=USER_SQL_AWS, password=password, port=5439):
        self.engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

    def get_engine(self):
        """
        Get the SQLAlchemy engine associated with the connection.
        """
        return self.engine

    def upload_dataframe(self, df, table_name, chunksize=10000, index=False, if_exists='replace', dtype=None):
        """
        Upload a DataFrame to a specified table in the database, targeting the 'ss_rs_ts_aut_americas_analyst' schema.

        :param df: The DataFrame to upload.
        :param table_name: The name of the table to upload the DataFrame to.
        :param chunksize: The number of rows to insert in each batch.
        :param index: Whether to include the DataFrame index as a column.
        :param if_exists: How to behave if the table already exists ('fail', 'replace', 'append').
        :param dtype: Data type mappings for the columns.
        """
        schema = 'ss_rs_ts_aut_americas_analyst'  # Hardcoded schema name
        try:
            df.to_sql(
                name=table_name,
                con=self.engine,
                schema=schema,
                index=index,
                if_exists=if_exists,
                chunksize=chunksize,
                dtype=dtype
            )
        except Exception as e:
            print(e)



class SqlConn:
    """

    ############ how to call it: ############

         # instantiate SqlConn to get connection
    from sabagclass import SqlConn, CheckIfRun
    sql_conn = SqlConn()
    conn = sql_conn.get_conn()

    """

    def __init__(self, driver='{ODBC Driver 17 for SQL Server}', server=SERVER_SQL, database=DATABASE_SQL,
                 uid=USER_SQL, pwd=PASSWORD_SQL):
        self.connection_string = f"DRIVER={driver};" \
                                 f"SERVER={server};" \
                                 f"DATABASE={database};" \
                                 f"UID={uid};" \
                                 f"PWD={pwd}"
        self.connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": self.connection_string})
        self.conn = create_engine(self.connection_url)

    def get_conn(self):
        return self.conn

class SqlConnAWS:
    """
    A class for creating a connection to AWS Redshift using psycopg2.

    How to use:
        # Instantiate SqlConnAWS to get a connection
        from your_module_name import SqlConnAWS
        sql_conn_aws = SqlConnAWS(host='your_host', dbname='your_dbname', user='your_user', password='your_password', port=your_port)
        conn = sql_conn_aws.get_conn()

        # Use the connection with pandas to execute a query
        df = pd.read_sql("SELECT * FROM your_table;", conn)
    """

    def __init__(self, host=SERVER_SQL_AWS, dbname=DATABASE_SQL_AWS, user=USER_SQL_AWS, password=PASSWORD_SQL_AWS, port=5439):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password
        self.port = port
        self.conn = None

    def get_conn(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(
                host=self.host,
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                port=self.port
            )
        return self.conn


class SqlConnCAT:
    def __init__(self):
        self.driver = 'ODBC Driver 17 for SQL Server'
        self.server = SERVER_CAT
        self.database = DATABASE_CAT
        self.username = USER_CAT
        self.password = PASSWORD_CAT  # Replace with the actual password

    def get_connection(self):
        connection_string = f"""DRIVER={{{self.driver}}};
                                SERVER={self.server};
                                DATABASE={self.database};
                                UID={self.username};
                                PWD={self.password}"""
        try:
            # Attempt to establish a connection
            connection = pyodbc.connect(connection_string)
            print("Connection successful!")
            return connection
        except Exception as e:
            # Handle connection errors
            print("An error occurred:", e)
            return None


class SqlConnCAT2:
    def __init__(self):
        self.driver = 'ODBC Driver 17 for SQL Server'
        self.server = SERVER_CAT2
        self.database = DATABASE_CAT2
        self.username = USER_CAT2
        self.password = PASSWORD_CAT2  # Replace with the actual password

    def get_connection(self):
        connection_string = f"""DRIVER={{{self.driver}}};
                                SERVER={self.server};
                                DATABASE={self.database};
                                UID={self.username};
                                PWD={self.password}"""
        try:
            # Attempt to establish a connection
            connection = pyodbc.connect(connection_string)
            print("Connection successful!")
            return connection
        except Exception as e:
            # Handle connection errors
            print("An error occurred:", e)
            return None


class SqlConnCAT3:
    def __init__(self):
        self.driver = 'ODBC Driver 17 for SQL Server'
        self.server = SERVER_CAT3
        self.database = DATABASE_CAT3
        self.username = USER_CAT3
        self.password = PASSWORD_CAT3  # Replace with the actual password

    def get_connection(self):
        connection_string = f"""DRIVER={{{self.driver}}};
                                SERVER={self.server};
                                DATABASE={self.database};
                                UID={self.username};
                                PWD={self.password}"""
        try:
            # Attempt to establish a connection
            connection = pyodbc.connect(connection_string)
            print("Connection successful!")
            return connection
        except Exception as e:
            # Handle connection errors
            print("An error occurred:", e)
            return None


class CheckIfRun:
    """


        ############ how to call it: ############
         # instantiate CheckIfRun to check if script has run

     from sabagclass import SqlConn, CheckIfRun
    check_if_run = CheckIfRun(conn)
    check_if_run.check_if_run('my_script.py')


    """

    def __init__(self, conn):
        self.conn = conn

    def check_if_run(self, escript):
        dfexc = pd.read_sql(f"""
            select top 1 [start timestamp], started, finished from all_tables
            where Script = '{escript}'
            order by [start timestamp] desc
        """, self.conn)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        dfexc['ts'] = now
        dfexc['start timestamp'] = pd.to_datetime(dfexc['start timestamp'])
        dfexc['ts'] = pd.to_datetime(dfexc['ts'])
        dfexc['diff_hours'] = dfexc['start timestamp'] - dfexc['ts']
        dfexc['diff_hours'] = abs(dfexc['diff_hours'] / np.timedelta64(1, 'h'))  # difference in hours

        dfexc1 = dfexc.iat[0, 1]
        dfexc2 = dfexc.iat[0, 2]
        time = dfexc.iat[0, 4]

        if dfexc1 == 'Yes' and dfexc2 == 'Yes' and time <= 30:
            print('success')
            pass
        else:
            print('fallo check_if_run()')
            exit()


class CheckIfRunV2:
    """
    Class to check if a script has run successfully within a specific timeframe.

    How to call it:
        # instantiate CheckIfRun to check if script has run
        from yourmodule import SqlUtils, CheckIfRunV2

        # Check if another script has run successfully
        check_if_run = CheckIfRunV2(conn, SqlUtils)
        # check_if_run.check_if_run('ERROR MESSAGE', 'WHICH SCRIPT CHECK IF RUN', script, starttimestamp, 'TIME TO CHECK LAST RUN')
        check_if_run.check_if_run('ALL_MB51_prod.py didnt run', 'ALL_MB51_prod.py', script, starttimestamp, 29)
    """

    def __init__(self, conn, SqlUtils):
        self.conn = conn
        self.SqlUtils = SqlUtils

    def check_if_run(self, error_msg, escript, script, starttimestamp, time_threshold=30):
        dfexc = pd.read_sql(f"""
            select top 1 [start timestamp], started, finished from all_tables
            where Script = '{escript}'
            order by [start timestamp] desc
        """, self.conn)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        dfexc['ts'] = now
        dfexc['start timestamp'] = pd.to_datetime(dfexc['start timestamp'])
        dfexc['ts'] = pd.to_datetime(dfexc['ts'])
        dfexc['diff_hours'] = (dfexc['start timestamp'] - dfexc['ts']).abs() / np.timedelta64(1, 'h')

        dfexc1 = dfexc.iat[0, 1]
        dfexc2 = dfexc.iat[0, 2]
        time = dfexc.iat[0, 4]

        if dfexc1 == 'Yes' and dfexc2 == 'Yes' and time <= time_threshold:
            print('success')
        else:
            print('fallo check_if_run()')
            # starttimestamp = dfexc['start timestamp'].iat[0]
            self.SqlUtils.insert_error_record(error_msg,script, starttimestamp)
            exit()

class SqlUtils:
    """

            ############ how to call it: ############

        #this is always
    from my_library.sql_utils import SqlUtils

         # Create an instance of SqlUtils
    sql_utils = SqlUtils()

         # Call the get_timestamp() method to retrieve starttimestamp and startfecha
    starttimestamp, startfecha = sql_utils.get_timestamp()

    script = 'yulinhmo.py'
    serialno = 99
    types = 'ETL'

    SqlUtils.insert_start_record(starttimestamp, startfecha ,script, serialno, types)
    SqlUtils.insert_finish_record(script, starttimestamp)
    SqlUtils.insert_error_record(errorblock,script, starttimestamp)



            ############ how to call ENVIARMAIL: ############
        from my_library.SqlUtils import SqlUtils

        # create an instance of SqlUtils
        sql_utils = SqlUtils()

        # call the enviarmail() function
        subject = "Test email"
        body = "This is a test email"
        sql_utils.enviarmail(subject, body)

    """

    @staticmethod
    def get_conn():
        connection_string = "DRIVER={ODBC Driver 17 for SQL Server};" \
                            f"SERVER={SERVER_SQL};" \
                            f"DATABASE={DATABASE_SQL};" \
                            f"UID={USER_SQL};" \
                            f"PWD={PASSWORD_SQL}"
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        conn = create_engine(connection_url)
        return conn

    @staticmethod
    def get_timestamp():
        now = datetime.now()
        starttimestamp = now.strftime("%Y-%m-%d %H:%M:%S")
        startfecha = now.strftime("%Y-%m-%d")

        print('get_timestamp()')
        print(starttimestamp)
        return starttimestamp, startfecha

    @staticmethod
    def insert_start_record(starttimestamp, startfecha, script, serialno, types):
        conn = SqlUtils.get_conn().connect()
        conn.execute(
            """
            insert into all_tables (fecha, [start timestamp] , script, serialno, type, started)
            values (?, ?, ?, ?, ?, ?)
            """,
            (startfecha, starttimestamp, script, serialno, types, 'Yes')
        )
        conn.close()

    @staticmethod
    def insert_error_record(errorblock, script, starttimestamp):
        conn = SqlUtils.get_conn().connect()
        now2 = datetime.now()
        finishedtimestamp = now2.strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            """
            update all_tables 
            set [Finished timestamp] = ?,
            Finished = 'No',
            [error block] = ?
            WHERE script = ? 
            AND [start timestamp] = ?
            """,
            (finishedtimestamp, errorblock, script, starttimestamp)
        )
        conn.close()

    @staticmethod
    def insert_finish_record(script, starttimestamp):
        conn = SqlUtils.get_conn().connect()
        now2 = datetime.now()
        finishedtimestamp = now2.strftime("%Y-%m-%d %H:%M:%S")
        try:
            conn.execute(
                """
                update all_tables 
                set [Finished timestamp] = ?,
                Finished = 'Yes',
                [error block] = null
                WHERE script = ? 
                AND [start timestamp] = ?
                """,
                (finishedtimestamp, script, starttimestamp)
            )
            conn.close()
            print('saved success')
        except Exception as e:
            print(e)

class SqlConnEdge:
    """
    ############ how to call it: ############

         # instantiate SqlConnEdge to get connection

    from sabagclass import SqlConnEdge

    SqlConnEdge = SqlConnEdge()
    connEdge = SqlConnEdge.get_conn()


    df = pd.read_sql(
    '''
    SELECT table_schema, table_name
    FROM information_schema.tables

    ''', connEdge)



    """

    def __init__(self, dsn='Redshift 64 bit SS Prod 2', uid=EDGE_USR, pwd=EDGE_PASS):
        self.dsn = dsn
        self.uid = uid
        self.pwd = pwd
        self.conn = pyodbc.connect(f'DSN={dsn};UID={uid};PWD={pwd}')


    def get_conn(self):
        return self.conn