#Update table_name
#Set column1= value1, column1= value1 ...
#where conditon (optional)

from ..database import Database
import sqlparse
from ..query_parser import parse_single_condition

class UpdateHandler:
    def __init__(self, database: Database):
        self.database = database

    def handle(self, parsed):
        """Handle UPDATE FROM statements."""
        table_name = None
        condition_columns = []
        condition_values = []
        condition_types = []
        logical_operator = None
        set_columns = []
        set_values = []

      
        for idx, token in enumerate(parsed.tokens):
            # print(token)
            if token.value.upper() == 'UPDATE':
                next_token_tuple = parsed.token_next(idx, skip_ws=True, skip_cm=True)
                if next_token_tuple:
                    next_token = next_token_tuple[1]
                    if isinstance(next_token, sqlparse.sql.Identifier):
                        table_name = next_token.get_real_name()
                    elif next_token.ttype in (sqlparse.tokens.Name, sqlparse.tokens.Keyword):
                        table_name = next_token.value

            elif token.match(sqlparse.tokens.Keyword, 'SET'):
                next_token = parsed.token_next(idx, skip_ws=True)[1]
                if isinstance(next_token, sqlparse.sql.IdentifierList):
                    value_tokens = next_token.get_identifiers()
                else:
                    value_tokens = [next_token]

                for value_token in value_tokens:
                    if '=' in str(value_token):
                        col, val = str(value_token).split('=')
                        col = col.strip()
                        val = val.strip()
                        set_columns.append(col)
                        if val.isdigit():
                            set_values.append(int(val))
                        elif val.startswith("'") and val.endswith("'"):
                            set_values.append(val.strip("'"))
                        else:
                            raise ValueError(f"Invalid value format in SET clause: {val}")
                        
                
            elif token.value.upper().startswith('WHERE'):
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
                    condition_values.append(condition_value.strip("'"))
                    condition_types.append(condition_type)

                    

        message = None
        if table_name is None:
            message = "Table name not found in UPDATE FROM statement"
        if condition_column is None or condition_value is None:
            message = "Condition not found in UPDATE FROM statement"

        success, message = self.database.update_rows(table_name, condition_columns, condition_values, condition_types, set_columns, 
        set_values, logical_operator)
        print(message)