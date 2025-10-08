def get_conn(options = None):
    # TODO: update this to take options
    conn = sqlite3.connect("local.db")
    return conn

def get_snowflake_conn(options = None):
    # TODO
    return None
