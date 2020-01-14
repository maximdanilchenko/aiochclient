import re

import sqlparse.keywords
from sqlparse import tokens
from sqlparse.keywords import KEYWORDS_COMMON


class Where(sqlparse.sql.TokenList):
    M_OPEN = sqlparse.tokens.Keyword, 'WHERE'
    M_CLOSE = (
        sqlparse.tokens.Keyword,
        (
            'ORDER BY',
            'GROUP BY',
            'LIMIT',
            'UNION',
            'UNION ALL',
            'EXCEPT',
            'HAVING',
            'RETURNING',
            'INTO',
            'FORMAT',
        ),
    )


sqlparse.sql.Where = Where

KEYWORDS_COMMON['FORMAT'] = tokens.Keyword.DML
SQL_REGEX = {'clickhouse-ext': [('FORMAT', sqlparse.tokens.Keyword)]}

FLAGS = re.IGNORECASE | re.UNICODE
SQL_REGEX = [
    (re.compile(rx, FLAGS).match, tt) for rx, tt in SQL_REGEX['clickhouse-ext']
]

sqlparse.keywords.SQL_REGEX = SQL_REGEX + sqlparse.keywords.SQL_REGEX
sqlparse.lexer.SQL_REGEX = sqlparse.keywords.SQL_REGEX
