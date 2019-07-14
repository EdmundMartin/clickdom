from typing import Tuple
from enum import Enum

from clickdom.core.exceptions import QueryError


class QueryType(Enum):
    RETRIEVE = 0
    INSERT = 1
    OTHER = 2


def query_type(query_str: str) -> QueryType:
    stmt = query_str.lstrip()[:8].upper()
    if any([stmt.startswith('SELECT'),
            stmt.startswith('SHOW'),
            stmt.startswith('DESCRIBE'),
            stmt.startswith('EXIST')]):
        return QueryType.RETRIEVE
    if stmt.startswith('INSERT'):
        return QueryType.INSERT
    return QueryType.OTHER


def build_query(query_str: str, q_type: QueryType, *args) -> str:

    if q_type == QueryType.RETRIEVE and 'format' not in query_str.lower():
        query_str += ' FORMAT TSVWithNamesAndTypes'

    if args and q_type != QueryType.INSERT:
        raise QueryError('Only INSERT query may have arguments passed to them')

    return query_str
