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
        logical_operator = None
        column_aliases = {}  # Stores renamed column aliases

        for token in parsed.tokens:
            #Support rename select
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
                table_name_token = parsed.token_next(parsed.token_index(token))[1]
                if table_name_token:
                    table_name = table_name_token.get_real_name()
            #mulitple 
            elif isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    if table_name and identifier.get_real_name().upper() == table_name.upper():
                        continue
                    parts = identifier.value.split(" AS ")
                    if len(parts) == 2:  # Handles column renaming
                        column_name, alias = parts[0].strip(), parts[1].strip()
                        column_aliases[column_name] = alias
                        selected_columns.append(column_name)
                    else:
                        selected_columns.append(identifier.get_real_name())
            #Single
            elif isinstance(token, sqlparse.sql.Identifier):
                if table_name and token.get_real_name().upper() == table_name.upper():
                    continue
                parts = token.value.split(" AS ")
                if len(parts) == 2:
                    column_name, alias = parts[0].strip(), parts[1].strip()
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

        success, message = self.database.select_rows(table_name, selected_columns, condition_columns, condition_values, condition_types, logical_operator)
        if success:
            for i in range(len(selected_columns)):
                if selected_columns[i] in column_aliases:
                    selected_columns[i] = column_aliases[selected_columns[i]]

            print(selected_columns)
            for row in message:
                print(row)

