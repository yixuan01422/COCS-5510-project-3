from ..database import Database
import sqlparse

class SelectHandler:
    def __init__(self, database: Database):
        """
        Initializes the SelectHandler with a reference to the database.
        
        Parameters:
        - database (Database): The database instance where queries will be executed.
        """
        self.database = database

    def handle(self, parsed):
        """
        Handles SELECT statements by extracting table name and selected columns.
        
        Parameters:
        - parsed (sqlparse.sql.Statement): The parsed SQL statement.
        
        Returns:
        - List of rows matching the SELECT query.
        """
        table_name = None  # Stores the table name
        selected_columns = []  # Stores selected column names

        # Debugging output to check parsed query
        print(f"DEBUG: Parsed SQL query: {parsed}")

        # Extract table name and selected columns
        for token in parsed.tokens:
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
                table_name_token = parsed.token_next(parsed.token_index(token))[1]
                if table_name_token:
                    table_name = table_name_token.get_real_name()  # Extract table name
            elif isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    selected_columns.append(identifier.get_real_name())
            elif isinstance(token, sqlparse.sql.Identifier):
                selected_columns.append(token.get_real_name())
            elif token.value == '*':
                selected_columns = ['*']  # Mark for all columns

        # Debugging output to check extracted table name and columns
        print(f"DEBUG: Extracted table name: {table_name}")
        print(f"DEBUG: Extracted columns: {selected_columns}")
        print(f"DEBUG: Available tables: {self.database.tables.keys()}")

        if table_name is None:
            raise ValueError("Table name not found in SELECT statement")

        # Ensure table exists
        if table_name not in self.database.tables:
            raise ValueError(f"Table '{table_name}' does not exist in the database")

        # Fetch data from the table
        result = self.database.get_table_data(table_name)
        print(f"DEBUG: Fetching data for table '{table_name}':", result)  # Debugging output
        if not result:
            return []  # Return empty if table has no data

        # Ensure column names exist in the table schema
        table_columns = [col[0] for col in self.database.columns[table_name]]
        print(f"DEBUG: Table columns: {table_columns}")  # Debugging output

        # Handle "SELECT *"
        if '*' in selected_columns or not selected_columns:
            selected_columns = table_columns  # Select all columns
        else:
            # Remove the table name if it was mistakenly included in selected_columns
            selected_columns = [col for col in selected_columns if col in table_columns]

            # Validate column existence
            invalid_columns = [col for col in selected_columns if col not in table_columns]
            if invalid_columns:
                raise ValueError(f"Invalid columns {invalid_columns} in table '{table_name}'")

        # Extract relevant column indices
        column_indices = [table_columns.index(col) for col in selected_columns]

        # Filter result based on selected columns
        filtered_result = [[row[i] for i in column_indices] for row in result]

        return filtered_result
