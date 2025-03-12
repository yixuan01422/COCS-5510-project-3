from ..database import Database
import sqlparse

class CreateTableHandler:
    def __init__(self, database: Database):
        self.database = database

    def handle(self, parsed):
        """Handle CREATE TABLE statements."""
        table_name = None
        columns = []

        for token in parsed.tokens:
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'TABLE':
                table_name_token = parsed.token_next(parsed.token_index(token))[1]
                if table_name_token:
                    table_name = table_name_token.value
            elif isinstance(token, sqlparse.sql.Parenthesis):
                for item in token.tokens:
                    if isinstance(item, sqlparse.sql.IdentifierList):
                        for col in item.get_identifiers():
                            columns.append(col.value)
                    elif isinstance(item, sqlparse.sql.Identifier):
                        columns.append(item.value)

        if table_name is None:
            raise ValueError("Table name not found in CREATE TABLE statement")

        # Create the table
        self.database.create_table(table_name, columns)
        print(f"Table '{table_name}' created with columns: {columns}")