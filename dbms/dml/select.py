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
        aggregation_operator = None
        aggregation_column = None
        logical_operator = None
        column_aliases = {}  
        order_column = None
        ascending = True
        group_by_column = None

        
        from_token = None
        for token in parsed.tokens:
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
                from_token = token
                break
        
        if not from_token:
            raise ValueError("FROM clause not found in SELECT statement")

       
        from_idx = parsed.token_index(from_token)
        select_tokens = parsed.tokens[:from_idx]

        for token in select_tokens:
            if isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    parts = identifier.value.split(" AS ")
                    if '(' in identifier.value and ')' in identifier.value:
                        aggregation_operator = identifier.value[:identifier.value.index('(')].upper()
                        column_name = identifier.value[identifier.value.index('(') + 1:identifier.value.index(')')]
                        aggregation_column = column_name
                    else:
                        column_name = parts[0].strip()
                    
                    if len(parts) == 2:  
                        _, alias = parts[0].strip(), parts[1].strip()
                        column_aliases[column_name] = alias
                        selected_columns.append(column_name)
                    else:
                        if aggregation_operator:
                            column_aliases[column_name] = identifier.value
                        selected_columns.append(column_name)
            elif isinstance(token, sqlparse.sql.Identifier) or (hasattr(token, 'value') and '(' in token.value and ')' in token.value):
                parts = token.value.split(" AS ")
                if '(' in token.value and ')' in token.value:
                    aggregation_operator = token.value[:token.value.index('(')].upper()
                    column_name = token.value[token.value.index('(') + 1:token.value.index(')')]
                    aggregation_column = column_name
                else:
                    column_name = parts[0].strip()
                
                if len(parts) == 2:
                    _, alias = parts[0].strip(), parts[1].strip()
                    column_aliases[column_name] = alias
                    selected_columns.append(column_name)
                else:
                    if aggregation_operator:
                        column_aliases[column_name] = token.value
                    selected_columns.append(column_name)
                
            elif token.value == '*':
                selected_columns = ['*']

        
        from_onwards_tokens = parsed.tokens[from_idx:]
        for token in from_onwards_tokens:
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
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
                    condition_parts = condition.split(' and ')
                    logical_operator = 'AND'
                elif ' or ' in lower_str:
                    condition_parts = condition.split(' or ')
                    logical_operator = 'OR'
                else:
                    condition_parts = [condition]

                for part in condition_parts:
                    condition_column, condition_value, condition_type = parse_single_condition(part)
                    condition_columns.append(condition_column)
                    condition_values.append(condition_value)
                    condition_types.append(condition_type)


        if table_name is None:
            raise ValueError("Table name not found in SELECT statement")

        if table_name not in self.database.tables:
            raise ValueError(f"Table '{table_name}' does not exist in the database")
        
        success, message = self.database.select_rows(table_name, selected_columns, condition_columns, condition_values, condition_types, logical_operator, aggregation_operator, aggregation_column, order_column, ascending, group_by_column)
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
            

