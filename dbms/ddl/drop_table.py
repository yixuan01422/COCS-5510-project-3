from ..database import Database
import sqlparse

class DropTableHandler:
    def __init__(self, database: Database):
        self.database = database

    def handle(self, parsed):
        """Handle DROP TABLE statements."""
        table_name = None

        # Extract table name
        for token in parsed.tokens:
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'TABLE':
                table_name_token = parsed.token_next(parsed.token_index(token))[1]
                if table_name_token:
                    table_name = table_name_token.value

        if table_name is None:
            raise ValueError("Table name not found in DROP TABLE statement")

        # Drop the table
        success, message = self.database.drop_table(table_name)
        print(message)