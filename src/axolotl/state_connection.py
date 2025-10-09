import sqlite3
#from .snapshot_snowflake import scan_database
import snowflake.connector
from pydantic import BaseModel
from snowflake.connector import DictCursor


class SnowflakeOptions(BaseModel):
    user: str
    password: str
    account: str
    database: str
    warehouse: str
    table_schema: str

    class Config:
        extra = "allow"


def get_conn(options=None):
    # TODO: update this to take options
    conn = sqlite3.connect("local.db")
    return conn


## TODO
# 1. Take a db and scan it for tables
# 2. foreach table, take its metadata
# 3. Depending on the metadata of each col, take its stats
# 4. write to thing
def get_snowflake_conn(
    options: SnowflakeOptions,
) -> snowflake.connector.SnowflakeConnection:
    conn = snowflake.connector.connect(
        **options.model_dump(),
    )

    scan_database(conn, options)
    return

#    //with conn.cursor(DictCursor) as cur:
#    //    scan_database(cur, options)
#
#    //return conn
#
def scan_database(conn: snowflake.connector.SnowflakeConnection, options: SnowflakeOptions):
    with conn.cursor(DictCursor) as cur:
        cur.execute(f"SELECT TABLE_NAME, CREATED, LAST_ALTERED FROM information_schema.tables WHERE table_schema = '{options.table_schema}'")

        for table_rec in cur:
            scan_table(conn, options, table_rec["TABLE_NAME"])

def scan_table(conn: snowflake.connector.SnowflakeConnection, options: SnowflakeOptions, table_name: str): 
    with conn.cursor() as cur:
        cur.execute(f"show columns in {options.database}.{options.table_schema}.{table_name}")

        results = cur.fetchall()
        for rec in results: 
            print(f'{rec}')




