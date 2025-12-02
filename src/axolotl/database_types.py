from .connectors.identifiers import FqTable
from enum import Enum
from typing import (
    NamedTuple,
)


class SimpleDataType(Enum):
    """Simple categorization of Snowflake data types"""

    BOOLEAN = "boolean"
    NUMERIC = "numeric"
    STRING = "string"
    DATETIME = "datetime"
    STRUCTURED = "structured"
    UNSTRUCTURED = "unstructured"
    VECTOR = "vector"
    OTHER = "other"


class ColumnInfo(NamedTuple):
    table: FqTable
    column_name: str
    data_type: str
    data_type_simple: SimpleDataType
