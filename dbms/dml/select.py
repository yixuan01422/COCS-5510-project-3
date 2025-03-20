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
        column_aliases = {}  # Stores renamed column aliases
        order_column = None
        ascending = True

        for token in parsed.tokens:
            #Support rename select
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

            elif isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    if table_name and identifier.get_real_name().upper() == table_name.upper():
                        continue
                    if order_column and identifier.get_real_name().upper() == order_column.upper():
                        continue
                    parts = identifier.value.split(" AS ")
                    if len(parts) == 2:  
                        column_name, alias = parts[0].strip(), parts[1].strip()
                        if '(' in column_name and ')' in column_name:
                            aggregation_operator = column_name[:column_name.index('(')].upper()
                            inner_column = column_name[column_name.index('(') + 1:column_name.index(')')]
                            column_name = inner_column
                            aggregation_column = column_name
                        column_aliases[column_name] = alias
                        selected_columns.append(column_name)
                    else:
                        selected_columns.append(identifier.get_real_name())

            elif isinstance(token, sqlparse.sql.Identifier):
                if table_name and token.get_real_name().upper() == table_name.upper():
                    continue
                if order_column and token.get_real_name().upper() == order_column.upper():
                    continue
                parts = token.value.split(" AS ")
                if len(parts) == 2:
                    column_name, alias = parts[0].strip(), parts[1].strip()
                    if '(' in column_name and ')' in column_name:
                        aggregation_operator = column_name[:column_name.index('(')].upper()
                        inner_column = column_name[column_name.index('(') + 1:column_name.index(')')]
                        column_name = inner_column
                        aggregation_column = column_name
                    column_aliases[column_name] = alias
                    selected_columns.append(column_name)
                else:
                    selected_columns.append(token.get_real_name())
                    print(token)
                
    
            elif token.value == '*':
                selected_columns = ['*']
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
                    # Only one condition
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

        success, message = self.database.select_rows(table_name, selected_columns, condition_columns, condition_values, condition_types, logical_operator, aggregation_operator, aggregation_column, order_column, ascending)
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
            

