class Database:
    def __init__(self):
        self.tables = {} 
        self.columns = {}  
        self.primary_keys = {}  

    def create_table(self, table_name, columns, primary_key=None): 
        """Create a new table."""
        if table_name in self.tables:
            return False, f"ERROR: Table '{table_name}' already exists"
        self.columns[table_name] = columns
        self.tables[table_name] = [] 
        self.primary_keys[table_name] = primary_key  

        return True, f"Table '{table_name}' created with PRIMARY KEY: {primary_key}"

    def drop_table(self, table_name):
        """Drop an existing table."""
        if table_name in self.tables:
            del self.tables[table_name]
            del self.columns[table_name]
            if table_name in self.primary_keys:
                del self.primary_keys[table_name]  
            return True, f"Table '{table_name}' dropped successfully."
        return False, f"ERROR: Table '{table_name}' does not exist."

    def insert_row(self, table_name, row):
        """Insert a row into a table."""
        if table_name not in self.tables:
            return False, f"Table '{table_name}' does not exist"

        column_definitions = self.columns[table_name]
        if len(row) != len(column_definitions):
            return False, f"Expected {len(column_definitions)} values, got {len(row)}"
        
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

        self.tables[table_name].append(row) 
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


    def select_rows(self, table_name, selected_columns, condition_column=None, condition_value=None, condition_type=None):
        rows = self.tables[table_name]
        column_definitions = self.columns[table_name]
        column_names = [col[0] for col in column_definitions]

        filtered_rows = []
        if condition_column and condition_value and condition_type:
            if condition_column not in column_names:
                return False, f"Column '{condition_column}' not found in table '{table_name}'"

            condition_index = column_names.index(condition_column)
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

            for row in rows:
                if condition_func(row[condition_index]):
                    filtered_rows.append(row)
        else:
            filtered_rows = rows[:]

        if len(selected_columns) == 1 and selected_columns[0] == '*':
            return True, filtered_rows

        result_rows = []
        for row in filtered_rows:
            selected_data = []
            for col in selected_columns:
                if col in column_names:
                    col_idx = column_names.index(col)
                    selected_data.append(row[col_idx])
            result_rows.append(selected_data)

        return True, result_rows