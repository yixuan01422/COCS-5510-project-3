from ..database import Database
import sqlparse
from ..query_parser import parse_single_condition

class SelectHandler:
    def __init__(self, database: Database):
        self.database = database

    def handle(self, parsed):

        table_name = None 
        selected_columns = []  
        condition_columns = []
        condition_values = []
        condition_types = []
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
                    else:
                        column_name = parts[0].strip()
                    selected_columns.append(column_name)
                    
                    if len(parts) == 2: #handle rename 
                        _, alias = parts[0].strip(), parts[1].strip()
                        column_aliases[column_name] = alias
                    else:
                        if agg_op:
                            column_aliases[column_name] = identifier.value
                        
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
                if table_name_token:
                    table_name = table_name_token.get_real_name()
            elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'ORDER BY':
                order_column_token = parsed.token_next(parsed.token_index(token))[1]
                if order_column_token:
                    order_column = order_column_token.get_real_name()
                    order_column_parts = order_column_token.value.split()
                    if len(order_column_parts) > 1:
                        if order_column_parts[1].upper() == 'DESC':
                            ascending = False
                        elif order_column_parts[1].upper() == 'ASC':
                            ascending = True
            elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'GROUP BY':
                group_by_column_token = parsed.token_next(parsed.token_index(token))[1]
                if group_by_column_token:
                    group_by_column = group_by_column_token.get_real_name()
            elif token.value.startswith('WHERE'):
                condition = token.value.replace("WHERE", "").replace(";", "").strip()
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
                    condition_columns.append(condition_column)
                    condition_values.append(condition_value)
                    condition_types.append(condition_type)
            elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'HAVING':
                
                having_idx = parsed.token_index(token)

                agg_token = parsed.token_next(having_idx)[1]
                if agg_token:
                    agg_str = agg_token.value
                    agg_part = agg_str[:agg_str.index(')')+ 1]  
                    having_aggregation_operator.append(agg_part[:agg_part.index('(')].upper())
                    column = agg_part[agg_part.index('(')+1:agg_part.index(')')]
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
                        having_condition_columns.append(column)
                        
                        remaining = agg_str[agg_str.index(')')+1:].strip()
                        comparison_op = remaining.split()[0]
                        value = remaining.split()[1]
                        having_condition_types.append(comparison_op)
                        having_condition_values.append(value)

        if table_name is None:
            raise ValueError("Table name not found in SELECT statement")

        if table_name not in self.database.tables:
            raise ValueError(f"Table '{table_name}' does not exist in the database")

        success, message = self.database.select_rows(
            table_name, 
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
            having_logical_operator
        )
        if success:
            for i in range(len(selected_columns)):
                if selected_columns[i] in column_aliases:
                    selected_columns[i] = column_aliases[selected_columns[i]]

            if selected_columns[0] == "*":
                #print(self.database.columns[table_name])
                columns = [col[0] for col in self.database.columns[table_name]]
                print (columns)
            else:
                print(selected_columns)
            for row in message:
                print(row)
            

