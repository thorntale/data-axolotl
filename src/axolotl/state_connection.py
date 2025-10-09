import sqlite3
from .snapshot_snowflake import scan_database
import snowflake.connector
from pydantic import BaseModel


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
        **options,
    )

    with conn.cursor() as cur:
        scan_database(cur )

    return conn
