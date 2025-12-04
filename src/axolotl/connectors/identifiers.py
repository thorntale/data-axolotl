import re
from typing import NamedTuple


ident = r'([^":.]+|"(""|[^"])+")'
metric = r'([^.:]+)'
fq_table_regex = (
    rf'^(?P<database>{ident})\.(?P<schema>{ident})\.(?P<table>{ident})$'
)
include_derective_regexs = [
    rf'^(?P<database>{ident})$',
    rf'^(?P<database>{ident})\.(?P<schema>{ident})$',
    rf'^(?P<database>{ident})\.(?P<schema>{ident})\.(?P<table>{ident})$',
    rf'^(?P<database>{ident})\.(?P<schema>{ident})\.(?P<table>{ident}):(?P<metric>{metric})$',
    rf'^(?P<database>{ident})\.(?P<schema>{ident})\.(?P<table>{ident})\.(?P<column>{ident})$',
    rf'^(?P<database>{ident})\.(?P<schema>{ident})\.(?P<table>{ident})\.(?P<column>{ident}):(?P<metric>{metric})$',
]

def parse_ident(sql_ident: str) -> str:
    if sql_ident[0] == '"' and sql_ident[-1] == '"':
        return sql_ident[1:-1].replace('""', '"')
    elif '"' in sql_ident:
        raise TypeError(f'Unparsable sql identifier {sql_ident!r}')
    else:
        return sql_ident

def parse_maybe_ident(sql_ident: str | None) -> str | None:
    if sql_ident is None:
        return None
    else:
        return parse_ident(sql_ident)

class FqTable(NamedTuple):
    database: str
    schema: str
    table: str

    def __str__(self):
        return '.'.join(
            FqTable.escape(p)
            for p in [
                self.database,
                self.schema,
                self.table,
            ]
        )

    @staticmethod
    def escape(ident: str) -> str:
        if re.match(r'[a-zA-Z_][a-zA-Z0-9_]*', ident):
            return ident
        else:
            return '"' + ident.replace('"', '""') + '"'

    @staticmethod
    def from_string(v: str) -> FqTable:
        m = re.match(fq_table_regex, v)
        if not m:
            raise ValueError(f'Cannot parse fully qualified table name from {v}')
        return FqTable(
            parse_ident(m.group('database')),
            parse_ident(m.group('schema')),
            parse_ident(m.group('table')),
        )

class IncludeDirective(NamedTuple):
    database: str
    schema: str | None
    table: str | None
    column: str | None
    metric: str | None

    @staticmethod
    def from_string(v: str) -> IncludeDirective:
        for regex in include_derective_regexs:
            m = re.match(regex, v)
            if m:
                groups = m.groupdict()
                return IncludeDirective(
                    parse_ident(groups['database']),
                    parse_maybe_ident(groups.get('schema')),
                    parse_maybe_ident(groups.get('table')),
                    parse_maybe_ident(groups.get('column')),
                    parse_maybe_ident(groups.get('metric')),
                )
        raise ValueError(f'Cannot parse include/exclude directive from {v}')

    @property
    def database_lower(self):
        return None if self.database is None else self.database.lower()
    @property
    def schema_lower(self):
        return None if self.schema is None else self.schema.lower()
    @property
    def table_lower(self):
        return None if self.table is None else self.table.lower()
    @property
    def column_lower(self):
        return None if self.column is None else self.column.lower()
    @property
    def metric_lower(self):
        return None if self.metric is None else self.metric.lower()

