def pretty_table_name(table: str) -> str:
    return table.split('.')[-1].title()

def maybe_float(n: str):
    try:
        return float(n)
    except ValueError:
        return n
