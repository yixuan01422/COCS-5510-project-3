from ..database import Database
import sqlparse
from ..query_parser import parse_single_condition

class DeleteHandler:
    def __init__(self, database: Database):
        self.database = database

    def handle(self, parsed):
        """Handle DELETE FROM statements."""
        table_name = None
        condition_columns = []
        condition_values = []
        condition_types = []
        logical_operator = None

        for token in parsed.tokens:
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
                table_name_token = parsed.token_next(parsed.token_index(token))[1]
                if table_name_token:
                    table_name = table_name_token.value
            elif token.value.startswith('WHERE'):
                condition = token.value.replace("WHERE", "").replace(";", "").strip()
                #lower_str = condition.lower()
                #if ' and ' in lower_str:
                if ' and ' in condition.lower():
                    #condition_parts = condition.split(' and ')
                    logical_operator = 'AND'
                    condition_parts = [part.strip() for part in condition.lower().split(' and ')]
                #elif ' or ' in lower_str:
                elif ' or ' in condition.lower():
                    #condition_parts = condition.split(' or ')
                    logical_operator = 'OR'
                    condition_parts = [part.strip() for part in condition.lower().split(' or ')]
                    #print("Parsed condition parts:", condition_parts)

                else:
                    # Only one condition
                    #condition_parts = [condition]
                    condition_parts = [condition.strip()]
                for part in condition_parts:
                    condition_column, condition_value, condition_type = parse_single_condition(part)
                    condition_columns.append(condition_column)
                    condition_values.append(condition_value)
                    condition_types.append(condition_type)

        message = None
        if table_name is None:
            message = "Table name not found in DELETE FROM statement"
        if condition_column is None or condition_value is None:
            message = "Condition not found in DELETE FROM statement"

        success, message = self.database.delete_rows(table_name, condition_columns, condition_values, condition_types, logical_operator)
        print(message)