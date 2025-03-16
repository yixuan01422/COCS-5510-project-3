import sqlparse

def parse_query(query):
    """Parse the SQL query into an intermediate representation."""
    parsed = sqlparse.parse(query)[0]
    return parsed

def parse_single_condition(condition: str):
    """
    Parse a single condition string (e.g. 'id >= 2')
    Returns (condition_column, condition_value, condition_type).
    """
    condition = condition.strip()
    if ">=" in condition:
        condition_type = ">="
        parts = condition.split(">=")
    elif "<=" in condition:
        condition_type = "<="
        parts = condition.split("<=")
    elif ">" in condition:
        condition_type = ">"
        parts = condition.split(">")
    elif "<" in condition:
        condition_type = "<"
        parts = condition.split("<")
    elif "=" in condition:
        condition_type = "="
        parts = condition.split("=")
    else:
        raise ValueError("Unsupported condition operator in WHERE clause.")

    condition_column = parts[0].strip()
    condition_value = parts[1].strip()
    return condition_column, condition_value, condition_type