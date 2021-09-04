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

KEYWORDS_COMMON['FORMAT'] = tokens.Keyword
KEYWORDS_COMMON['EXISTS'] = tokens.Keyword.DML
KEYWORDS_COMMON['DESCRIBE'] = tokens.Keyword.DML
KEYWORDS_COMMON['SHOW'] = tokens.Keyword.DML
SQL_REGEX = {
    'clickhouse-ext': [
        ('(FORMAT|EXISTS)\b', sqlparse.tokens.Keyword),
        ('(DESCRIBE|SHOW)\b', sqlparse.tokens.Keyword.DML),
    ]
}

FLAGS = re.IGNORECASE | re.UNICODE
SQL_REGEX = [
    (re.compile(rx, FLAGS).match, tt) for rx, tt in SQL_REGEX['clickhouse-ext']
]

sqlparse.keywords.SQL_REGEX = SQL_REGEX + sqlparse.keywords.SQL_REGEX
sqlparse.lexer.SQL_REGEX = sqlparse.keywords.SQL_REGEX
