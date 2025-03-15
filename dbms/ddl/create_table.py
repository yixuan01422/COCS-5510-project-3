from ..database import Database
import sqlparse

class CreateTableHandler:
    def __init__(self, database: Database):
        self.database = database

    def handle(self, parsed):
        table_name = None  # Stores the table name
        columns = []  # Stores column definitions
        primary_key = None  # Stores the primary key column name

        # Iterate through the parsed SQL tokens
        for token in parsed.tokens:
            # Identify the TABLE keyword and extract the table name
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'TABLE':
                table_name_token = parsed.token_next(parsed.token_index(token))[1]
                if table_name_token:
                    table_name = table_name_token.value  # Extract table name
            
            # Identify the parenthesis section containing column definitions    
            elif isinstance(token, sqlparse.sql.Parenthesis):
                column_definitions = token.value.strip("()")  # Extract content inside parentheses
                column_lines = column_definitions.split(",")  # Split by comma

                for line in column_lines:
                    parts = line.strip().split()
                    
                    # Ensure at least column name and type exist
                    if len(parts) >= 2:
                        column_name, column_type = parts[0], parts[1].upper()
                        columns.append((column_name, column_type))

                        # Check if it's a primary key
                        if len(parts) > 2 and "PRIMARY" in parts and "KEY" in parts:
                            primary_key = column_name  # Assign primary key

        # Debugging output to confirm extracted columns
        print(f"Extracted Columns: {columns}, PRIMARY KEY: {primary_key}")

        # Validate that a table name was provided
        if table_name is None:
            raise ValueError("Table name not found in CREATE TABLE statement")

        # Create the table with primary key support
        result = self.database.create_table(table_name, columns, primary_key)
        if result:
            print(f"Table '{table_name}' created with columns: {columns}, PRIMARY KEY: {primary_key}")
        else:
            print(f"ERROR: Table '{table_name}' already exists")
