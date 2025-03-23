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

    def delete_rows(self, table_name, condition_columns, condition_values, condition_types, logical_operator=None):
        """Delete rows from a table based on a condition."""
        if table_name not in self.tables:
            return False, f"Table '{table_name}' does not exist"

        column_definitions = self.columns[table_name]
        col_names = [col[0] for col in column_definitions]

        deleted_count = 0
        i = 0
        if len(condition_columns) == 1:
            condition_index, condition_func = self.build_condition_func(table_name, col_names, condition_columns[0], condition_types[0], condition_values[0])
            while i < len(self.tables[table_name]):
                row = self.tables[table_name][i]
                row_value = row[condition_index]
                if condition_func(row_value):
                    self.tables[table_name].pop(i)
                    deleted_count += 1
                else:
                    i += 1
        elif len(condition_columns) == 2:
            condition_index1, condition_func1 = self.build_condition_func(table_name, col_names, condition_columns[0], condition_types[0], condition_values[0])
            condition_index2, condition_func2 = self.build_condition_func(table_name, col_names, condition_columns[1], condition_types[1], condition_values[1])
            while i < len(self.tables[table_name]):
                row = self.tables[table_name][i]
                row_value1 = row[condition_index1]
                row_value2 = row[condition_index2]
                if logical_operator == 'AND':
                    if condition_func1(row_value1) and condition_func2(row[row_value2]):
                        self.tables[table_name].pop(i)
                        deleted_count += 1
                        print(row)
                    else:
                        i += 1
                elif logical_operator == 'OR':
                    if condition_func1(row_value1) or condition_func2(row_value2):
                        self.tables[table_name].pop(i)
                        deleted_count += 1
                        print(row)
                    else:
                        i += 1

        
        

        return True, f"Deleted {deleted_count} rows from '{table_name}'"


    def select_rows(
        self, table_name, selected_columns, condition_columns=None, 
        condition_values=None, condition_types=None, logical_operator=None, aggregation_operator=None, aggregation_column=None, order_column=None, ascending=True, group_by_column=None
    ):
        """Select rows from a table based on columns and optional condition(s)."""
        if table_name not in self.tables:
            return False, f"Table '{table_name}' does not exist."
        rows = self.tables[table_name]
        col_definitions = self.columns[table_name]
        col_names = [col[0] for col in col_definitions]
        filtered_rows = []
      
        if len(condition_columns) == 0:
            filtered_rows = rows
        elif len(condition_columns) == 1:
            condition_index, condition_func = self.build_condition_func(table_name, col_names, condition_columns[0], condition_types[0], condition_values[0])
            for row in rows:
                if condition_func(row[condition_index]):
                    filtered_rows.append(row)
        elif len(condition_columns) == 2:
            condition_index1, condition_func1 = self.build_condition_func(table_name, col_names, condition_columns[0], condition_types[0], condition_values[0])
            condition_index2, condition_func2 = self.build_condition_func(table_name, col_names, condition_columns[1], condition_types[1], condition_values[1])

            for row in rows:
                if logical_operator == 'AND':
                    if condition_func1(row[condition_index1]) and condition_func2(row[condition_index2]):
                        filtered_rows.append(row)
                elif logical_operator == 'OR':
                    if condition_func1(row[condition_index1]) or condition_func2(row[condition_index2]):
                        filtered_rows.append(row)
             
        if order_column:
        
            if order_column not in col_names:
                raise ValueError(f"ERROR: Order by column '{order_column}' is not in the table columns {col_names}")


            order_col_idx = col_names.index(order_column)  

            filtered_rows = sorted(
                    filtered_rows,
                    key=lambda x: (x[order_col_idx]),
                    reverse= not ascending  
            )
        print(filtered_rows)
        if not (len(selected_columns) == 1 and selected_columns[0] == '*'):
            selected_indices = [col_names.index(col) for col in selected_columns if col in col_names]
            for i in range(len(filtered_rows)):
                filtered_rows[i] = [filtered_rows[i][idx] for idx in selected_indices]
      
        
        if group_by_column:
            group_col_idx = selected_columns.index(group_by_column)
            grouped_data = {}
            for row in filtered_rows:
                group_key = row[group_col_idx]
                if group_key not in grouped_data:
                    grouped_data[group_key] = []
                grouped_data[group_key].append(row)
            
            final_results = []
            for group_key, group_rows in grouped_data.items():
                group_result = []
                group_result.append(group_key)
                
                if aggregation_operator:
                    if not aggregation_column == '*':
                        agg_col_idx = col_names.index(aggregation_column)
                        group_values = [row[agg_col_idx] for row in group_rows]
                    if aggregation_operator == 'COUNT':
                        agg_value = len(group_rows)
                    elif aggregation_operator == 'SUM':
                        agg_value = sum(group_values)
                    elif aggregation_operator == 'AVG':
                        agg_value = sum(group_values) / len(group_values)
                    elif aggregation_operator == 'MIN':
                        agg_value = min(group_values)
                    elif aggregation_operator == 'MAX':
                        agg_value = max(group_values)
                    
                    group_result.append(agg_value)
                
                final_results.append(group_result)
            
            filtered_rows = final_results
            
        elif aggregation_operator:
            if aggregation_operator == 'MIN':
                agg_col_idx = selected_columns.index(aggregation_column)
                min_value = min(row[agg_col_idx] for row in filtered_rows)
                filtered_rows = [[min_value]]
            elif aggregation_operator == 'MAX':
                agg_col_idx = selected_columns.index(aggregation_column)
                max_value = max(row[agg_col_idx] for row in filtered_rows)
                filtered_rows = [[max_value]]
            elif aggregation_operator == 'AVG':
                agg_col_idx = selected_columns.index(aggregation_column)
                avg_value = sum(row[agg_col_idx] for row in filtered_rows) / len(filtered_rows)
                filtered_rows = [[avg_value]]
            elif aggregation_operator == 'SUM':
                agg_col_idx = selected_columns.index(aggregation_column)
                sum_value = sum(row[agg_col_idx] for row in filtered_rows)
                filtered_rows = [[sum_value]]
            elif aggregation_operator == 'COUNT':
                count_value = len(filtered_rows)
                filtered_rows = [[count_value]]
                
        return True, filtered_rows
        
        
    def build_condition_func(self, table_name, col_names, condition_column, condition_type, condition_value):
        condition_index = col_names.index(condition_column)
        col_type = self.columns[table_name][condition_index][1]
        if col_type == 'INT':
            condition_value = int(condition_value)
        if  condition_type == '=':
            condition_func = lambda row_value: row_value == condition_value
        elif condition_type == '>':
            condition_func = lambda row_value: row_value > condition_value
        elif condition_type == '<':
            condition_func = lambda row_value: row_value < condition_value
        elif condition_type == '>=':
            condition_func = lambda row_value: row_value >= condition_value
        elif condition_type == '<=':
            condition_func = lambda row_value: row_value <= condition_value   

        return condition_index, condition_func
