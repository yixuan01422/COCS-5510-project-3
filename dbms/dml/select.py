from ..database import Database
import sqlparse
from ..query_parser import parse_single_condition

class SelectHandler:
    def __init__(self, database: Database):
        self.database = database

    def handle(self, parsed):

        table_name = [] #make a list
        table_alias_map = {}
        selected_columns = []  
        condition_columns = []
        condition_values = []
        condition_types = []
        condition_value_types = []  # ✅ This was missing

        # Modified to support multiple aggregations
        aggregation_operators = []
        aggregation_columns = []
        logical_operator = None
        column_aliases = {}  
        order_column = None
        ascending = True
        group_by_column = None
        having_condition_columns = []
        having_condition_values = []
        having_condition_types = []
        having_aggregation_operator = []
        having_logical_operator = None

        """
        break the query into two parts
        query:SELECT column1, column2, ... FROM table_name WHERE condition;
        =>
        part1: SELECT column1, column2, ... 
        part2:FROM table_name WHERE condition;
        """
        from_token = None
        # for token in parsed.tokens: #token line
        #     print(token)
        for token in parsed.tokens:
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
                from_token = token
                break

        
        if not from_token:
            raise ValueError("FROM clause not found in SELECT statement")

        from_idx = parsed.token_index(from_token)
        select_tokens = parsed.tokens[:from_idx]
        #handle part1
        for token in select_tokens:
            if isinstance(token, sqlparse.sql.IdentifierList): #handle select more than one column
                for identifier in token.get_identifiers():
                    parts = identifier.value.split(" AS ")#handle rename
                    agg_op = None
                    if '(' in identifier.value and ')' in identifier.value:
                        # Extract aggregation function and column
                        agg_op = identifier.value[:identifier.value.index('(')].upper()
                        column_name = identifier.value[identifier.value.index('(') + 1:identifier.value.index(')')]
                        aggregation_operators.append(agg_op)
                        aggregation_columns.append(column_name)
                        selected_columns.append(f"{agg_op}({column_name})")
                    else:
                        column_name = parts[0].strip()
                        selected_columns.append(column_name)
                    
                    if len(parts) == 2: #handle rename 
                        _, alias = parts[0].strip(), parts[1].strip()
                        column_aliases[column_name] = alias
                    else:
                        if agg_op:
                            #column_aliases[column_name] = identifier.value
                            column_aliases[f"{agg_op}({column_name})"] = identifier.value

                        
            elif isinstance(token, sqlparse.sql.Identifier) or (hasattr(token, 'value') and '(' in token.value and ')' in token.value):
                #handle select one column
                parts = token.value.split(" AS ") #handle rename
                agg_op = None
                if '(' in token.value and ')' in token.value:
                    # Extract aggregation function and column
                    agg_op = token.value[:token.value.index('(')].upper()
                    column_name = token.value[token.value.index('(') + 1:token.value.index(')')]
                    aggregation_operators.append(agg_op)
                    aggregation_columns.append(column_name)
                    #selected_columns.append(f"{agg_op}({column_name})")
                else:
                    column_name = parts[0].strip()
                selected_columns.append(column_name)
                
                if len(parts) == 2: #handle rename 
                        _, alias = parts[0].strip(), parts[1].strip()
                        column_aliases[column_name] = alias
                else:
                    if agg_op:
                        column_aliases[column_name] = token.value
            elif token.value == '*':
                selected_columns = ['*']

        #handle part2
        from_onwards_tokens = parsed.tokens[from_idx:]
        for token in from_onwards_tokens:
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM': #extract table name
                table_name_token = parsed.token_next(parsed.token_index(token))[1]
                if isinstance(table_name_token, sqlparse.sql.IdentifierList):
                    for identifier in table_name_token.get_identifiers():
                        real_name = identifier.get_real_name()
                        alias = identifier.get_alias() or real_name
                        #table_name.append(f"{real_name} {alias}")
                        table_name.append(real_name) 
                        table_alias_map[alias] = real_name
                elif isinstance(table_name_token, sqlparse.sql.Identifier):
                    real_name = table_name_token.get_real_name()
                    alias = table_name_token.get_alias() or real_name
                    #table_name.append(f"{real_name} {alias}")
                    table_name.append(real_name) 
                    table_alias_map[alias] = real_name

            elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'ORDER BY':
                order_column_token = parsed.token_next(parsed.token_index(token))[1]
                if order_column_token:
                    order_column_parts = order_column_token.value.split()
                    order_column = order_column_parts[0]
                    # Handle table aliases in ORDER BY
                    if '.' in order_column:
                        alias, col = order_column.split('.')
                        order_column = f"{table_alias_map.get(alias, alias)}.{col}"
                    
                    if len(order_column_parts) > 1:
                        if order_column_parts[1].upper() == 'DESC':
                            ascending = False
                        elif order_column_parts[1].upper() == 'ASC':
                            ascending = True
            elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'GROUP BY':
                group_by_column_token = parsed.token_next(parsed.token_index(token))[1]
                if group_by_column_token:
                    group_by_column = group_by_column_token.value
                    # Handle table aliases in GROUP BY - same approach as ORDER BY
                    print(group_by_column)
                    if '.' in group_by_column:
                        alias, col = group_by_column.split('.')
                        group_by_column = f"{table_alias_map.get(alias, alias)}.{col}"
                        print(group_by_column)
            elif token.value.startswith('WHERE'):
                print(token)
                condition = token.value.replace("WHERE", "").replace(";", "").strip()
                #condition = token.value[5:].replace(";", "").strip()
                lower_str = condition.lower()
                if ' and ' in lower_str:
                    condition_parts = lower_str.split(' and ')
                    logical_operator = 'AND'
                elif ' or ' in lower_str:
                    condition_parts = lower_str.split(' or ')
                    logical_operator = 'OR'
                else:
                    condition_parts = [condition]
                
                for part in condition_parts:
                    condition_column, condition_value, condition_type = parse_single_condition(part)
                    if '.' in condition_column:
                        alias, col = condition_column.split('.')
                        condition_column = f"{table_alias_map.get(alias, alias)}.{col}"
                    if '.' in condition_value:
                        alias, col = condition_value.split('.')
                        condition_value = f"{table_alias_map.get(alias, alias)}.{col}"
                        #condition_type = 'COLUMN'
                        value2_type = 'COLUMN'
                    else:
                        value2_type = None
                    condition_columns.append(condition_column)
                    if condition_value and condition_value.isdigit():
                        condition_value = int(condition_value)
                    condition_values.append(condition_value)
                    condition_types.append(condition_type)
                    condition_value_types.append(value2_type)

            elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'HAVING':
                
                having_idx = parsed.token_index(token)

                agg_token = parsed.token_next(having_idx)[1]
                if agg_token:
                    agg_str = agg_token.value
                    agg_part = agg_str[:agg_str.index(')')+ 1]  
                    having_aggregation_operator.append(agg_part[:agg_part.index('(')].upper())
                    column = agg_part[agg_part.index('(')+1:agg_part.index(')')]
                    
                    # Handle table aliases in HAVING column references
                    if '.' in column:
                        alias, col = column.split('.')
                        column = f"{table_alias_map.get(alias, alias)}.{col}"
                        
                    having_condition_columns.append(column)
                    
                    remaining = agg_str[agg_str.index(')')+1:].strip()
                    comparison_op = remaining.split()[0]
                    value = remaining.split()[1]
                    having_condition_types.append(comparison_op)
                    having_condition_values.append(value)
                
                condition_token = parsed.token_next(parsed.token_index(agg_token))[1]
                if condition_token.value == 'AND' or condition_token.value == 'OR':
                    having_logical_operator = condition_token.value
                    
                    second_agg_token = parsed.token_next(parsed.token_index(condition_token))[1]
                    if second_agg_token:
                        agg_str = second_agg_token.value
                        agg_part = agg_str[:agg_str.index(')')+ 1]  
                        having_aggregation_operator.append(agg_part[:agg_part.index('(')].upper())
                        column = agg_part[agg_part.index('(')+1:agg_part.index(')')]
                        
                        # Handle table aliases in HAVING column references for second condition
                        if '.' in column:
                            alias, col = column.split('.')
                            column = f"{table_alias_map.get(alias, alias)}.{col}"
                            
                        having_condition_columns.append(column)
                        
                        remaining = agg_str[agg_str.index(')')+1:].strip()
                        comparison_op = remaining.split()[0]
                        value = remaining.split()[1]
                        having_condition_types.append(comparison_op)
                        having_condition_values.append(value)


        if table_name is None:
            raise ValueError("Table name not found in SELECT statement")

        #if table_name not in self.database.tables:
        #    raise ValueError(f"Table '{table_name}' does not exist in the database")
        for tbl in table_name:
            #base = tbl.split()[0]
            if tbl not in self.database.tables:
                raise ValueError(f"Table '{tbl}' does not exist in the database")
            #if base not in self.database.tables:
            #    raise ValueError(f"Table '{base}' does not exist in the database")
        # Expand alias-based selected columns (e.g., u.id -> users.id)
        expanded_selected_columns = []
        for col in selected_columns:
            if '.' in col:
                alias, col_name = col.split('.')
                real_table = table_alias_map.get(alias, alias)
                expanded_selected_columns.append(f"{real_table}.{col_name}")
            else:
                expanded_selected_columns.append(col)

        # print(f"Expanded Selected Columns: {expanded_selected_columns}")
        # print(f"Table Alias Map: {table_alias_map}")
        success, message = self.database.select_rows(
            table_name,
            expanded_selected_columns, 
            selected_columns, 
            condition_columns, 
            condition_values, 
            condition_types, 
            logical_operator, 
            aggregation_operators,  
            aggregation_columns,    
            order_column, 
            ascending, 
            group_by_column,
            having_condition_columns,
            having_condition_values,
            having_condition_types,
            having_aggregation_operator,
            having_logical_operator,
            condition_value_types,
            table_alias_map 
        )
        if success:
            for i in range(len(selected_columns)):
                if selected_columns[i] in column_aliases:
                    selected_columns[i] = column_aliases[selected_columns[i]]

            #if selected_columns[0] == "*":
                #print(self.database.columns[table_name])
            #    columns = [col[0] for col in self.database.columns[table_name]]
            #    print (columns)
            #else:
            #    print(selected_columns)
            #for row in message:
            #    print(row)

            if selected_columns[0] == "*":
                if len(table_name) == 1:
                    #columns = [col[0] for col in self.database.columns[table_name[0]]]
                    base = table_name[0].split()[0]
                    alias = table_name[0].split()[-1]
                    columns = [f"{col[0]}" for col in self.database.columns[base]]
                else:
                    t1, t2 = table_name
                    #t1_real = t1.split()[0]
                    #t2_real = t2.split()[0]
                    #t1_alias = t1.split()[-1]
                    #t2_alias = t2.split()[-1]
                    #columns = [f"{t1}.{col[0]}" for col in self.database.columns[t1]] + \
                    #        [f"{t2}.{col[0]}" for col in self.database.columns[t2]]
                    t1_alias = list(table_alias_map.keys())[list(table_alias_map.values()).index(t1)]
                    t2_alias = list(table_alias_map.keys())[list(table_alias_map.values()).index(t2)]
                    #columns = [f"{t1_alias}.{col[0]}" for col in self.database.columns[t1_real]] + \
                    #        [f"{t2_alias}.{col[0]}" for col in self.database.columns[t2_real]]
                    columns = [f"{t1_alias}.{col[0]}" for col in self.database.columns[t1]] + \
                            [f"{t2_alias}.{col[0]}" for col in self.database.columns[t2]]

            else:
                columns = selected_columns

            print(columns)
            for row in message:
                print(row)
        else:
            #print(result)
            print(message)

            

