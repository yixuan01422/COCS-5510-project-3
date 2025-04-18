import copy
class Database:
    def __init__(self):
        self.tables = {} 
        self.columns = {}  
        self.primary_keys = {}
        self.foreign_keys = {}   #added 
        self.indexes = {}  # Store indexes: {table_name: {column_name: {value: [row_indices]}}}
        self.use_sort_merge = False  # Flag to control join method
        
    def create_table(self, table_name, columns, primary_key=None, foreign_keys=None): 
        """Create a new table."""
        if table_name in self.tables:
            return False, f"ERROR: Table '{table_name}' already exists"
        self.columns[table_name] = columns
        self.tables[table_name] = [] 
        self.primary_keys[table_name] = primary_key
        self.foreign_keys[table_name] = foreign_keys or [] #added  

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

        # Foreign key checks
        for fk in self.foreign_keys.get(table_name, []):
            fk_col = fk['column']
            ref_table = fk['ref_table']
            ref_col = fk['ref_column']

            fk_index = [col[0] for col in self.columns[table_name]].index(fk_col)
            fk_value = row[fk_index]

            if ref_table not in self.tables:
                return False, f"ERROR: Referenced table '{ref_table}' does not exist"

            ref_col_index = [col[0] for col in self.columns[ref_table]].index(ref_col)
            if all(r[ref_col_index] != fk_value for r in self.tables[ref_table]):
                return False, f"ERROR: Foreign key constraint fails on '{fk_col}' referencing '{ref_table}({ref_col})'"


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
                # BEFORE self.tables[table_name].pop(i)
                # Check foreign key dependencies
                for dependent_table, fks in self.foreign_keys.items():
                    for fk in fks:
                        if fk['ref_table'] == table_name:
                            ref_col = fk['ref_column']
                            on_delete = fk['on_delete']
                            fk_col = fk['column']
                            ref_index = [col[0] for col in self.columns[table_name]].index(ref_col)
                            ref_value = row[ref_index]

                            if on_delete == 'CASCADE':
                                # Delete matching rows from dependent table
                                self.tables[dependent_table] = [
                                    r for r in self.tables[dependent_table]
                                    if r[[col[0] for col in self.columns[dependent_table]].index(fk_col)] != ref_value
                                ]
                            elif on_delete == 'SET NULL':
                                print(f"SET NULL triggered for FK {dependent_table}.{fk_col} where value = {ref_value}")
                                # Set FK column to None
                                fk_index = [col[0] for col in self.columns[dependent_table]].index(fk_col)
                                for r in self.tables[dependent_table]:
                                    #if r[[col[0] for col in self.columns[dependent_table]].index(fk_col)] == ref_value:
                                    #    r[[col[0] for col in self.columns[dependent_table]].index(fk_col)] = None
                                    if r[fk_index] == ref_value:
                                        print(f" - Setting NULL for row: {r}")
                                        r[fk_index] = None

                            elif on_delete == 'NO ACTION':
                                for r in self.tables[dependent_table]:
                                    if r[[col[0] for col in self.columns[dependent_table]].index(fk_col)] == ref_value:
                                        return False, f"ERROR: Cannot delete from '{table_name}' due to FOREIGN KEY constraint in '{dependent_table}'"
                print(f"[DEBUG] Foreign key map: {self.foreign_keys}")

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
        self, table_name,  expanded_selected_columns, selected_columns, condition_columns=None, 
        condition_values=None, condition_types=None, logical_operator=None, aggregation_operator=None, aggregation_column=None, order_column=None, ascending=True, group_by_column=None, 
        having_condition_columns=None, having_condition_values=None, having_condition_types=None, having_aggregation_operator=None, having_logical_operator=None, condition_value_types=None, table_alias_map=None
    ):
        """Select rows from a table based on columns and optional condition(s)."""
        if not all(tbl in self.tables for tbl in table_name):
            return False, f"Table '{table_name}' does not exist."

        filtered_rows = []
        
        if len(table_name) == 1:
            t1 = table_name[0]
            col_names = [col[0] for col in self.columns[t1]]
            full_col_names = col_names[:]
            
            # Check if we can use an index for the condition (single table, single equality condition)
            if (condition_columns and len(condition_columns) == 1 and 
                condition_types[0] == '=' and 
                condition_value_types and condition_value_types[0] is None and  # Not a column comparison
                t1 in self.indexes and 
                condition_columns[0] in self.indexes[t1] and 
                condition_values[0] in self.indexes[t1][condition_columns[0]]):
                
                print(f"Using index on {t1}.{condition_columns[0]} for lookup")
                # Use index for direct lookup
                index = self.indexes[t1][condition_columns[0]]
                row_indices = index[condition_values[0]]
                filtered_rows = [self.tables[t1][idx] for idx in row_indices]
            else:
                # Fallback to normal processing
                rows = self.tables[t1]
                if not condition_columns:
                    filtered_rows = rows
                else:
                    for row in rows:
                        results = [
                            self.compare_values(
                                row[full_col_names.index(condition_columns[i])],
                                row[full_col_names.index(condition_values[i])] if (condition_value_types and condition_value_types[i] == 'COLUMN') else condition_values[i],
                                operator=condition_types[i]
                            )
                            for i, col in enumerate(condition_columns)
                        ]
                        
                        if (len(results) == 1 and results[0]) or (logical_operator == 'AND' and all(results)) or (logical_operator == 'OR' and any(results)):
                            filtered_rows.append(row)
                    
        elif len(table_name) == 2:
            t1, t2 = table_name
            col_names = [f"{t1}.{col[0]}" for col in self.columns[t1]] + [f"{t2}.{col[0]}" for col in self.columns[t2]]
            full_col_names = col_names[:]
            
            if self.use_sort_merge:
                print("Using sort-merge join algorithm")
                
                # Extract join conditions and filter conditions
                join_conditions = []
                filter_conditions = []
                
                if condition_columns and condition_types and condition_values and condition_value_types:
                    for i, col in enumerate(condition_columns):
                        if condition_value_types[i] == 'COLUMN':
                            # This is a join condition
                            join_conditions.append((col, condition_values[i], condition_types[i]))
                        else:
                            # This is a filter condition
                            filter_conditions.append((col, condition_values[i], condition_types[i]))
                
                # Apply filters to individual tables first
                t1_filters = []
                t2_filters = []
                for col, val, op in filter_conditions:
                    if col.startswith(f"{t1}."):
                        t1_filters.append((col.split('.')[1], val, op))
                    elif col.startswith(f"{t2}."):
                        t2_filters.append((col.split('.')[1], val, op))
                
                # Filter t1 rows
                t1_rows = self.tables[t1]
                if t1_filters:
                    filtered_t1 = []
                    for row in t1_rows:
                        include = True
                        for col, val, op in t1_filters:
                            col_idx = [idx for idx, c in enumerate(self.columns[t1]) if c[0] == col][0]
                            if not self.compare_values(row[col_idx], val, op):
                                include = False
                                break
                        if include:
                            filtered_t1.append(row)
                    t1_rows = filtered_t1
                
                # Filter t2 rows
                t2_rows = self.tables[t2]
                if t2_filters:
                    filtered_t2 = []
                    for row in t2_rows:
                        include = True
                        for col, val, op in t2_filters:
                            col_idx = [idx for idx, c in enumerate(self.columns[t2]) if c[0] == col][0]
                            if not self.compare_values(row[col_idx], val, op):
                                include = False
                                break
                        if include:
                            filtered_t2.append(row)
                    t2_rows = filtered_t2
                
                # Extract join columns
                join_cols = []
                for left_col, right_col, op in join_conditions:
                    if op == '=' and left_col.startswith(f"{t1}.") and right_col.startswith(f"{t2}."):
                        t1_col = left_col.split('.')[1]
                        t2_col = right_col.split('.')[1]
                        join_cols.append((t1_col, t2_col))
                    elif op == '=' and left_col.startswith(f"{t2}.") and right_col.startswith(f"{t1}."):
                        t2_col = left_col.split('.')[1]
                        t1_col = right_col.split('.')[1]
                        join_cols.append((t1_col, t2_col))
                
                # If we have at least one equijoin condition, we can do the sort-merge join
                if join_cols:
                    # Get the indices for the join columns
                    t1_col, t2_col = join_cols[0]  # Use the first join condition
                    t1_col_idx = [idx for idx, c in enumerate(self.columns[t1]) if c[0] == t1_col][0]
                    t2_col_idx = [idx for idx, c in enumerate(self.columns[t2]) if c[0] == t2_col][0]
                    
                    # Sort both tables on the join columns
                    sorted_t1 = sorted(t1_rows, key=lambda row: row[t1_col_idx])
                    sorted_t2 = sorted(t2_rows, key=lambda row: row[t2_col_idx])
                    
                    # Perform the merge
                    i = 0  # Index for t1
                    j = 0  # Index for t2
                    
                    while i < len(sorted_t1) and j < len(sorted_t2):
                        t1_val = sorted_t1[i][t1_col_idx]
                        t2_val = sorted_t2[j][t2_col_idx]
                        
                        if t1_val < t2_val:
                            # t1's value is smaller, advance t1 pointer
                            i += 1
                        elif t1_val > t2_val:
                            # t2's value is smaller, advance t2 pointer
                            j += 1
                        else:
                            # Match found - collect all rows with this join value
                            match_val = t1_val
                            
                            # Find all t1 rows with this value
                            matching_t1 = []
                            while i < len(sorted_t1) and sorted_t1[i][t1_col_idx] == match_val:
                                matching_t1.append(sorted_t1[i])
                                i += 1
                            
                            # Find all t2 rows with this value
                            matching_t2 = []
                            while j < len(sorted_t2) and sorted_t2[j][t2_col_idx] == match_val:
                                matching_t2.append(sorted_t2[j])
                                j += 1
                            
                            # Create result rows from matched rows
                            for t1_row in matching_t1:
                                for t2_row in matching_t2:
                                    filtered_rows.append(t1_row + t2_row)
                    
                    # Verify all other join conditions if any
                    if len(join_cols) > 1:
                        verified_rows = []
                        for row in filtered_rows:
                            valid = True
                            for t1_join_col, t2_join_col in join_cols[1:]:  # Skip the first one we already used
                                t1_idx = [idx for idx, c in enumerate(self.columns[t1]) if c[0] == t1_join_col][0]
                                t2_idx = [idx for idx, c in enumerate(self.columns[t2]) if c[0] == t2_join_col][0]
                                t2_offset = len(self.columns[t1])  # Offset to find t2 columns in combined row
                                
                                if row[t1_idx] != row[t2_offset + t2_idx]:
                                    valid = False
                                    break
                            
                            if valid:
                                verified_rows.append(row)
                        
                        filtered_rows = verified_rows
                else:
                    # No equijoin condition, fallback to Cartesian product
                    print("No equijoin condition found, falling back to Cartesian product")
                    rows = [r1 + r2 for r1 in t1_rows for r2 in t2_rows]
                    
                    if condition_columns:
                        for row in rows:
                            results = [
                                self.compare_values(
                                    row[full_col_names.index(condition_columns[i])],
                                    row[full_col_names.index(condition_values[i])] if condition_value_types[i] == 'COLUMN' else condition_values[i],
                                    operator=condition_types[i]
                                )
                                for i in range(len(condition_columns))
                            ]
                            
                            if (len(results) == 1 and results[0]) or (logical_operator == 'AND' and all(results)) or (logical_operator == 'OR' and any(results)):
                                filtered_rows.append(row)
                    else:
                        filtered_rows = rows
            else:
                # Current implementation - Cartesian product
                print("Using Cartesian product join algorithm")
                rows = [r1 + r2 for r1 in self.tables[t1] for r2 in self.tables[t2]]
                
                if not condition_columns:
                    filtered_rows = rows
                else:          
                    for row in rows:
                        results = [
                            self.compare_values(
                                row[full_col_names.index(condition_columns[i])],
                                row[full_col_names.index(condition_values[i])] if (condition_value_types and condition_value_types[i] == 'COLUMN') else condition_values[i],
                                operator=condition_types[i]
                            )
                            for i, col in enumerate(condition_columns)
                        ]
    
                        if (len(results) == 1 and results[0]) or (logical_operator == 'AND' and all(results)) or (logical_operator == 'OR' and any(results)):
                            filtered_rows.append(row)
        else:
            return False, "Only support up to 2-table SELECT."

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
            #selected_indices = [col_names.index(col) for col in selected_columns if col in col_names]
            selected_indices = [col_names.index(col) for col in expanded_selected_columns if col in col_names]
            for i in range(len(filtered_rows)):
                filtered_rows[i] = [filtered_rows[i][idx] for idx in selected_indices]
            # print("[DEBUG] Expanded Selected Columns:", expanded_selected_columns)
            # print("[DEBUG] Selected Indices:", selected_indices)
            # if filtered_rows:
            #     print("[DEBUG] Sample Selected Row:", filtered_rows[0])



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

    def create_index(self, table_name, column_name):
        """Create an index on a specific column of a table."""
        if table_name not in self.tables:
            return False, f"ERROR: Table '{table_name}' does not exist"
            
        column_names = [col[0] for col in self.columns[table_name]]
        if column_name not in column_names:
            return False, f"ERROR: Column '{column_name}' does not exist in table '{table_name}'"
            
        # Initialize the index structure if it doesn't exist
        if table_name not in self.indexes:
            self.indexes[table_name] = {}
            
        # Create the index
        index = {}
        col_idx = column_names.index(column_name)
        
        for row_idx, row in enumerate(self.tables[table_name]):
            value = row[col_idx]
            if value not in index:
                index[value] = []
            index[value].append(row_idx)
            
        self.indexes[table_name][column_name] = index
        return True, f"Index created on {table_name}.{column_name}"
        
    def drop_index(self, table_name, column_name):
        """Drop an existing index on a specific column of a table."""
        if table_name not in self.tables:
            return False, f"ERROR: Table '{table_name}' does not exist"
            
        if table_name not in self.indexes:
            return False, f"ERROR: No indexes exist on table '{table_name}'"
            
        if column_name not in self.indexes[table_name]:
            return False, f"ERROR: No index exists on column '{column_name}' in table '{table_name}'"
            
        # Remove the index
        del self.indexes[table_name][column_name]
        
        # If no more indexes on this table, remove the table entry from indexes
        if not self.indexes[table_name]:
            del self.indexes[table_name]
            
        return True, f"Index dropped on {table_name}.{column_name}"
    
    def save_database(self, filename):
        """
        Save the entire database state to a file for persistence.
        
        Args:
            filename: Path to the file where database will be saved
        """
        import json
        import time
        
        start_time = time.time()
        
        # Create a serializable database state
        db_state = {
            "tables": self.tables,
            "columns": self.columns,
            "primary_keys": self.primary_keys,
            "foreign_keys": self.foreign_keys
        }
        
        # Handle indexes specially (convert non-string keys to strings)
        serialized_indexes = {}
        for table_name, table_indexes in self.indexes.items():
            serialized_indexes[table_name] = {}
            for column_name, column_index in table_indexes.items():
                serialized_indexes[table_name][column_name] = {}
                for value, row_indices in column_index.items():
                    # Convert value to string for JSON serialization
                    serialized_indexes[table_name][column_name][str(value)] = row_indices
        
        db_state["indexes"] = serialized_indexes
        
        try:
            with open(filename, 'w') as f:
                json.dump(db_state, f)
            end_time = time.time()
            save_time = end_time - start_time
            return True, f"Database saved to {filename} ({save_time:.4f} seconds)"
        except Exception as e:
            return False, f"ERROR: Failed to save database: {str(e)}"
    
    def load_database(self, filename):
        """
        Load the entire database state from a file.
        
        Args:
            filename: Path to the file containing saved database
        """
        import json
        import time
        import os
        
        if not os.path.exists(filename):
            return False, f"ERROR: Database file {filename} does not exist"
        
        start_time = time.time()
        try:
            with open(filename, 'r') as f:
                db_state = json.load(f)
            
            # Restore basic database structures
            self.tables = db_state["tables"]
            self.columns = db_state["columns"]
            self.primary_keys = db_state["primary_keys"]
            self.foreign_keys = db_state["foreign_keys"]
            
            # Restore indexes with proper type conversion
            self.indexes = {}
            serialized_indexes = db_state["indexes"]
            
            for table_name, table_indexes in serialized_indexes.items():
                self.indexes[table_name] = {}
                for column_name, column_index in table_indexes.items():
                    self.indexes[table_name][column_name] = {}
                    
                    # Get column type to properly convert values
                    column_names = [col[0] for col in self.columns[table_name]]
                    if column_name in column_names:
                        col_idx = column_names.index(column_name)
                        col_type = self.columns[table_name][col_idx][1]
                        
                        for value_str, row_indices in column_index.items():
                            # Convert value back to original type
                            if col_type == 'INT':
                                try:
                                    value = int(value_str)
                                except ValueError:
                                    value = value_str  # Keep as string if can't convert
                            else:
                                value = value_str
                                
                            self.indexes[table_name][column_name][value] = row_indices
            
            end_time = time.time()
            load_time = end_time - start_time
            
            return True, f"Database loaded from {filename} ({load_time:.4f} seconds)"
        except Exception as e:
            return False, f"ERROR: Failed to load database: {str(e)}"
