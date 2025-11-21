from .connectors.identifiers import FqTable


def pretty_table_name(table: FqTable) -> str:
    return table.table.title()

def maybe_float(n: str):
    try:
        return float(n)
    except ValueError:
        return n
