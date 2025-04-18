from ..database import Database
import sqlparse

class CreateTableHandler:
    def __init__(self, database: Database):
        self.database = database

    def handle(self, parsed):
        table_name = None  # Stores the table name
        columns = []  # Stores column definitions
        primary_key = None  # Stores the primary key column name
        foreign_keys = []  # Stores foreign key definitions

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
                    print(line)
                    parts = line.strip().split()
                    
                    # Ensure at least column name and type exist
                    if len(parts) >= 2 and parts[0].upper() != "FOREIGN":
                        column_name, column_type = parts[0], parts[1].upper()
                        columns.append((column_name, column_type))
                        # Check if it's a primary key
                        if len(parts) > 2 and "PRIMARY" in parts and "KEY" in parts:
                            primary_key = column_name  # Assign primary key
                    
                    elif parts[0].upper() == "FOREIGN":
                        # Example: FOREIGN KEY(department_id) REFERENCES departments(department_id) ON DELETE CASCADE
                        fk_column = parts[2].strip("()")
                        ref_table, ref_column = parts[4].split("(")
                        ref_column = ref_column.strip(")")
                        on_delete = None
                        if "ON" in parts and "DELETE" in parts:
                            on_index = parts.index("ON")
                            if on_index + 2 < len(parts):
                                if parts[on_index + 2].upper() == "SET" and parts[on_index + 3].upper() == "NULL": #for token which was only getting SET
                                    on_delete = "SET NULL"
                                else:
                                    on_delete = parts[on_index + 2]
                        foreign_keys.append({
                            'column': fk_column,
                            'ref_table': ref_table,
                            'ref_column': ref_column,
                            'on_delete': on_delete.upper() if on_delete else 'NO ACTION'
                        })


        # Debugging output to confirm extracted columns
        print(f"Extracted Columns: {columns}, PRIMARY KEY: {primary_key}, FOREIGN KEYS: {foreign_keys}" )

        # Validate that a table name was provided
        if table_name is None:
            raise ValueError("Table name not found in CREATE TABLE statement")

        # Create the table with primary key support
        result, msg = self.database.create_table(table_name, columns, primary_key, foreign_keys)
        if result:
            print(f"Table '{table_name}' created with columns: {columns}, PRIMARY KEY: {primary_key}, foreign keys: {foreign_keys}")
        else:
            print(msg)