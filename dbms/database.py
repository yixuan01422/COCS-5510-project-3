class Database:
    def __init__(self):
        self.tables = {} # Stores table data
        self.columns = {}  # Stores table schemas
        self.primary_keys = {}  # Stores primary keys for each table

    def create_table(self, table_name, columns, primary_key=None):  # Added primary_key argument
        """Create a new table."""
        if table_name in self.tables:
            return False, f"ERROR: Table '{table_name}' already exists"
        self.columns[table_name] = columns
        self.tables[table_name] = [] # Ensure each table gets its own list
        self.primary_keys[table_name] = primary_key  # Store the primary key

        return True, f"Table '{table_name}' created with PRIMARY KEY: {primary_key}"

    def drop_table(self, table_name):
        """Drop an existing table."""
        if table_name in self.tables:
            del self.tables[table_name]
            del self.columns[table_name]
            if table_name in self.primary_keys:
                del self.primary_keys[table_name]  # Remove primary key reference
            return True, f"Table '{table_name}' dropped successfully."
        return False, f"ERROR: Table '{table_name}' does not exist."

    def insert_row(self, table_name, row):
        """Insert a row into a table."""
        if table_name not in self.tables:
            return False, f"Table '{table_name}' does not exist"

        column_definitions = self.columns[table_name]
        if len(row) != len(column_definitions):
            return False, f"Expected {len(column_definitions)} values, got {len(row)}"
        
        # Check Primary Key constraint
        primary_key = self.primary_keys.get(table_name)
        if primary_key:
            column_names = [col[0] for col in column_definitions]
            if primary_key not in column_names:
                return False, f"ERROR: Primary key column '{primary_key}' not found in table schema"
            primary_index = column_names.index(primary_key)
            for existing_row in self.tables[table_name]:
                if existing_row[primary_index] == row[primary_index]:
                    return False, f"ERROR: Duplicate entry for PRIMARY KEY '{primary_key}'"

        # Type checking
        for (col_name, col_type), value in zip(column_definitions, row):
            if col_type == 'INT' and not isinstance(value, int):
                return False, f"Expected INT for column '{col_name}', got {type(value).__name__}"
            elif col_type == 'STRING' and not isinstance(value, str):
                return False, f"Expected STRING for column '{col_name}', got {type(value).__name__}"

        self.tables[table_name].append(row) # Append row to the table
        return True, f"Inserted {row} into '{table_name}'"

    def delete_rows(self, table_name, condition_column, condition_value, condition_type):
        """Delete rows from a table based on a condition."""
        if table_name not in self.tables:
            return False, f"Table '{table_name}' does not exist"

        column_definitions = self.columns[table_name]
        column_names = [col[0] for col in column_definitions]
        if condition_column not in column_names:
            return False, f"Column '{condition_column}' not found in table '{table_name}'"

        condition_index = column_names.index(condition_column)
        deleted_count = 0
        col_type = self.columns[table_name][condition_index][1]
        if col_type == 'INT':
            condition_value = int(condition_value)
        if condition_type == '=':
            condition_func = lambda row_value: row_value == condition_value
        elif condition_type == '>':
            condition_func = lambda row_value: row_value > condition_value
        elif condition_type == '<':
            condition_func = lambda row_value: row_value < condition_value
        elif condition_type == '>=':
            condition_func = lambda row_value: row_value >= condition_value
        elif condition_type == '<=':
            condition_func = lambda row_value: row_value <= condition_value
        else:
            return False, f"Invalid condition type: {condition_type}"

        i = 0
        while i < len(self.tables[table_name]):
            row = self.tables[table_name][i]
            row_value = row[condition_index]
            if condition_func(row_value):
                self.tables[table_name].pop(i)
                deleted_count += 1
            else:
                i += 1

        return True, f"Deleted {deleted_count} rows from '{table_name}' where {condition_column} {condition_type} {condition_value}"


    def get_table_data(self, table_name):
        """Get all rows from a table."""
        return self.tables.get(table_name, [])