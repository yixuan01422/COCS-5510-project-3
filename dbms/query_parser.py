import sqlparse

def parse_query(query):
    """Parse the SQL query into an intermediate representation."""
    parsed = sqlparse.parse(query)[0]
    return parsed