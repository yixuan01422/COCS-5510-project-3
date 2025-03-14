class Database:
    def __init__(self):
        self.tables = {}  
        self.columns = {}  

    def create_table(self, table_name, columns):
        """Create a new table."""
        if table_name in self.tables:
            return False
        self.columns[table_name] = columns
        self.tables[table_name] = []
        return True

    def drop_table(self, table_name):
        """Drop an existing table."""
        if table_name in self.tables:
            del self.tables[table_name]
            del self.columns[table_name]
            return True
        return False

    def insert_row(self, table_name, row):
        """Insert a row into a table."""
        if table_name not in self.tables:
            return False, f"Table '{table_name}' does not exist"

        column_definitions = self.columns[table_name]
        if len(row) != len(column_definitions):
            return False, f"Expected {len(column_definitions)} values, got {len(row)}"

        # Type checking
        for (col_name, col_type), value in zip(column_definitions, row):
            if col_type == 'INT' and not isinstance(value, int):
                return False, f"Expected INT for column '{col_name}', got {type(value).__name__}"
            elif col_type == 'STRING' and not isinstance(value, str):
                return False, f"Expected STRING for column '{col_name}', got {type(value).__name__}"

        self.tables[table_name].append(row)
        return True, f"Inserted {row} into '{table_name}'"

    def get_table_data(self, table_name):
        """Get all rows from a table."""
        return self.tables.get(table_name, [])