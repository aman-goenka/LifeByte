import psycopg2 as pg
import pandas as pd
import snowflake.connector
from sqlalchemy import create_engine

# Postgres con details
pg_host = '<host_name>'
pg_port = '5432'
pg_database = '<database_name>'
pg_user = '<user_name>'
pg_password = '<password>'

# SnowFlake con details
sf_account_identifier = '<account_identifier>'
sf_user = '<user_login_name>'
sf_password = '<password>'
sf_database_name = '<database_name>'
sf_schema_name = '<schema_name>'
sf_table_name = '<table_name>'
conn_string = f"snowflake://{sf_user}:{sf_password}@{sf_account_identifier}/{sf_database_name}/{sf_schema_name}"
if_exists = 'replace'
sql_path = 'usecase.sql'

with open(sql_path, 'r') as fp:
    sql_query = fp.read()

def lambda_handler():
    # Connect to Postgres
    postgres_conn = pg.connect(
        host=pg_host,
        port=pg_port,
        user=pg_user,
        password=pg_password,
        database=pg_database
    )
    
    # Query Postgres into Dataframe
    dataframepg = pd.read_sql(sql_query, postgres_conn)

    # Write data to Snowflake
    engine = create_engine(conn_string)
    with engine.connect() as con:
            dataframepg.to_sql(name=sf_table_name.lower(), con=con, if_exists=if_exists, method=pd_writer)

    # Close connections
    postgres_conn.close()