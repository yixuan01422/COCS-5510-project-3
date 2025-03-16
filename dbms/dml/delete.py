from ..database import Database
import sqlparse

class DeleteHandler:
    def __init__(self, database: Database):
        self.database = database

    def handle(self, parsed):
        """Handle DELETE FROM statements."""
        table_name = None
        condition_column = None
        condition_value = None
        condition_type = None
        # Extract table name and condition
        for token in parsed.tokens:
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
                table_name_token = parsed.token_next(parsed.token_index(token))[1]
                if table_name_token:
                    table_name = table_name_token.value
            elif token.value.startswith('WHERE'):
                print(token)
                condition = token.value.replace("WHERE", "").strip()
                condition = condition.replace(";", "")
                print(condition)
                if "=" in condition:
                    condition_type = "="
                    parts = condition.split("=")
                elif ">" in condition:
                    condition_type = ">"
                    parts = condition.split(">")
                elif "<" in condition:
                    condition_type = "<"
                    parts = condition.split("<")
                else:
                    raise ValueError("Unsupported condition type in WHERE clause")

                # Extract column and value
                condition_column = parts[0].strip()
                condition_value = parts[1].strip()
                print(condition_column, condition_value, condition_type)
        message = None
        if table_name is None:
            message = "Table name not found in DELETE FROM statement"
        if condition_column is None or condition_value is None:
            message = "Condition not found in DELETE FROM statement"
        # Delete rows from the table based on the condition
        success, message = self.database.delete_rows(table_name, condition_column, condition_value)
        print(message)