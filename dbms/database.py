import copy
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

        for i, col in enumerate(condition_columns):
            col_idx = col_names.index(col)
            if self.columns[table_name][col_idx][1] == 'INT':
                condition_values[i] = int(condition_values[i])

        while i < len(self.tables[table_name]):
            row = self.tables[table_name][i]
            results = [
                self.compare_values(
                    row[col_names.index(col)], 
                    condition_values[j], 
                    condition_types[j]
                )
                for j, col in enumerate(condition_columns)
            ]

            should_delete = (len(results) == 1 and results[0]) or \
                          (logical_operator == 'AND' and all(results)) or \
                          (logical_operator == 'OR' and any(results))
            
            if should_delete:
                self.tables[table_name].pop(i)
                deleted_count += 1
            else:
                i += 1

        return True, f"Deleted {deleted_count} rows from '{table_name}'"
    
    def update_rows(self, table_name, condition_columns, condition_values, condition_types, set_columns, set_values, logical_operator=None):

        col_names = [col[0] for col in self.columns[table_name]]
        primary_key = self.primary_keys.get(table_name)
        cnt = 0
        if primary_key in set_columns:
            backup_rows = copy.deepcopy(self.tables[table_name])
        for i in range(len(self.tables[table_name])):
            row = self.tables[table_name][i]
            match_results = [
                self.compare_values(
                    row[col_names.index(condition_columns[i])],
                    int(condition_values[i]) if self.columns[table_name][col_names.index(condition_columns[i])][1] == 'INT' else condition_values[i],
                    condition_types[i]
                ) for i in range(len(condition_columns))
            ]
            
            should_update = (len(match_results) == 1 and match_results[0]) or \
                            (logical_operator == 'AND' and all(match_results)) or \
                            (logical_operator == 'OR' and any(match_results))

            if should_update:
                cnt+=1
                pk_values = []
                if primary_key in set_columns:
                    pk_index = col_names.index(primary_key)
                    pk_values = set(row[pk_index] for row in self.tables[table_name])
                for i, col in enumerate(set_columns):
                    if col == primary_key:
                        if set_values[i] in pk_values:
                            self.tables[table_name] = backup_rows
                            return False, f"ERROR: Duplicate entry for PRIMARY KEY '{primary_key}' in UPDATE"
                    idx = col_names.index(col)
                    row[idx] = set_values[i]

        return True, f"Updated {cnt} rows in '{table_name}'"



    def select_rows(
        self, table_name, selected_columns, condition_columns=None, 
        condition_values=None, condition_types=None, logical_operator=None, aggregation_operator=None, aggregation_column=None, order_column=None, ascending=True, group_by_column=None, 
        having_condition_columns=None, having_condition_values=None, having_condition_types=None, having_aggregation_operator=None, having_logical_operator=None
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
        else:
            for i, col in enumerate(condition_columns):
                col_idx = col_names.index(col)
                if self.columns[table_name][col_idx][1] == 'INT':
                    condition_values[i] = int(condition_values[i])

            for row in rows:
                results = [
                    self.compare_values(
                        row[col_names.index(col)], 
                        condition_values[i], 
                        condition_types[i]
                    )
                    for i, col in enumerate(condition_columns)
                ]

                if (len(results) == 1 and results[0]) or (logical_operator == 'AND' and all(results)) or (logical_operator == 'OR' and any(results)):
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

        
        if group_by_column:
            group_col_idx = col_names.index(group_by_column)
            grouped_data = {}
            for row in filtered_rows:
                group_key = row[group_col_idx]
                if group_key not in grouped_data:
                    grouped_data[group_key] = []
                grouped_data[group_key].append(row)

            final_results = []
            for group_key, group_rows in grouped_data.items():
            
                having_values = []
                for i, agg_op in enumerate(having_aggregation_operator):
                    agg_value = self.cal_aggregation(agg_op, having_condition_columns[i], group_rows, col_names)
                    having_values.append(agg_value)
                
                include_group = True
                if len(having_values) == 1:
                    condition_value = float(having_condition_values[0])
                    include_group = self.compare_values(having_values[0], condition_value, having_condition_types[0])
                elif len(having_values) == 2:
                    condition_value1 = float(having_condition_values[0])
                    condition_value2 = float(having_condition_values[1])
                    
                    result1 = self.compare_values(having_values[0], condition_value1, having_condition_types[0])
                    result2 = self.compare_values(having_values[1], condition_value2, having_condition_types[1])

                    if having_logical_operator == 'AND':
                        include_group = result1 and result2
                    elif having_logical_operator == 'OR':
                        include_group = result1 or result2

                if include_group:
                    group_result = []
                    group_result.append(group_key)
                    if aggregation_operator:
                        for i in range(len(aggregation_operator)):
                            agg_value = self.cal_aggregation(
                                aggregation_operator[i], 
                                aggregation_column[i], 
                                group_rows, 
                                col_names
                            )
                            group_result.append(agg_value)
                    final_results.append(group_result)

            return True, final_results
        elif aggregation_operator:
            result_rows = [[]]
            for i in range(len(aggregation_operator)):
                            agg_value = self.cal_aggregation(
                                aggregation_operator[i], 
                                aggregation_column[i], 
                                filtered_rows, 
                                col_names
                            )
                            result_rows[0].append(agg_value)
            return True, result_rows
        
        
        if not (len(selected_columns) == 1 and selected_columns[0] == '*'):
            selected_indices = [col_names.index(col) for col in selected_columns if col in col_names]
            for i in range(len(filtered_rows)):
                filtered_rows[i] = [filtered_rows[i][idx] for idx in selected_indices]

        return True, filtered_rows
        
        
    def cal_aggregation(self, aggregation_operator, aggregation_column, group_rows, col_names):
        """Calculate aggregation value for a group of rows."""
        if not aggregation_column == '*':
            agg_col_idx = col_names.index(aggregation_column)
            group_values = [row[agg_col_idx] for row in group_rows]
            
        if aggregation_operator == 'COUNT':
            return len(group_rows)
        elif aggregation_operator == 'SUM':
            return sum(group_values)
        elif aggregation_operator == 'AVG':
            return sum(group_values) / len(group_values)
        elif aggregation_operator == 'MIN':
            return min(group_values)
        elif aggregation_operator == 'MAX':
            return max(group_values)

    def compare_values(self, value1, value2, operator):
        """Compare two values using the specified operator.
        """
        if operator == '>':
            return value1 > value2
        elif operator == '>=':
            return value1 >= value2
        elif operator == '<':
            return value1 < value2
        elif operator == '<=':
            return value1 <= value2
        elif operator == '=':
            return value1 == value2
        else:
            raise ValueError(f"Unsupported comparison operator: {operator}")
