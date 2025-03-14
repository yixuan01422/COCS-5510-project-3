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
                column_tokens = token.tokens
                current_column_name = None
                current_column_type = None
                for column_token in column_tokens:
                    if isinstance(column_token, sqlparse.sql.Identifier):
                        current_column_name = column_token.get_real_name()
                    elif column_token.ttype in (sqlparse.tokens.Keyword, sqlparse.tokens.Name.Builtin):
                        current_column_type = column_token.value.upper()
                    elif column_token.ttype is sqlparse.tokens.Punctuation and column_token.value == ',':
                        if current_column_name and current_column_type:
                            columns.append((current_column_name, current_column_type))
                        current_column_name = None
                        current_column_type = None

                if current_column_name and current_column_type:
                    columns.append((current_column_name, current_column_type))
        if table_name is None:
            raise ValueError("Table name not found in CREATE TABLE statement")

        # Create the table
        result = self.database.create_table(table_name, columns)
        if result:
            print(f"Table '{table_name}' created with columns: {columns}")
        else:
            print(f"ERROR: Table '{table_name}' already exists")

