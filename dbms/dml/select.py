from ..database import Database
import sqlparse

class SelectHandler:
    def __init__(self, database: Database):
        self.database = database

    def handle(self, parsed):

        table_name = None 
        selected_columns = []  
        condition_column = None
        condition_value = None
        condition_type = None

        for token in parsed.tokens:
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
                table_name_token = parsed.token_next(parsed.token_index(token))[1]
                if table_name_token:
                    table_name = table_name_token.get_real_name()
            elif isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    if table_name and identifier.get_real_name().upper() == table_name.upper():
                        continue
                    selected_columns.append(identifier.get_real_name())
            elif isinstance(token, sqlparse.sql.Identifier):
                if table_name and token.get_real_name().upper() == table_name.upper():
                    continue
                selected_columns.append(token.get_real_name())
            elif token.value == '*':
                selected_columns = ['*']
            elif token.value.startswith('WHERE'):

                condition = token.value.replace("WHERE", "").replace(";", "").strip()
                
                if ">=" in condition:
                    condition_type = ">="
                    parts = condition.split(">=")
                elif "<=" in condition:
                    condition_type = "<="
                    parts = condition.split("<=")
                elif ">" in condition:
                    condition_type = ">"
                    parts = condition.split(">")
                elif "<" in condition:
                    condition_type = "<"
                    parts = condition.split("<")
                elif "=" in condition:
                    condition_type = "="
                    parts = condition.split("=")
                else:
                    raise ValueError("Unsupported condition type in WHERE clause")
                condition_column = parts[0].strip()
                condition_value = parts[1].strip()


        if table_name is None:
            raise ValueError("Table name not found in SELECT statement")

        if table_name not in self.database.tables:
            raise ValueError(f"Table '{table_name}' does not exist in the database")

        success, message = self.database.select_rows(table_name, selected_columns, condition_column, condition_value, condition_type)
        if success:
            print(message)

