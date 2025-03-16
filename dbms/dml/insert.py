from ..database import Database
import sqlparse

class InsertHandler:
    def __init__(self, database: Database):
        self.database = database

    def handle(self, parsed):
        """Handle INSERT INTO statements."""
        table_name = None
        values = [] 


        for token in parsed.tokens:
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'INTO':
                table_name_token = parsed.token_next(parsed.token_index(token))[1]
                if table_name_token:
                    table_name = table_name_token.value 
            elif token.value.startswith('VALUES'):

                value_string = token.value[token.value.index('(') + 1:token.value.rindex(')')]  
                value_tokens = value_string.split(',')
                for value_token in value_tokens:
                    value_token = value_token.strip()
                    if value_token.isdigit():
                        values.append(int(value_token)) 
                    elif value_token.startswith("'") and value_token.endswith("'"):
                        values.append(value_token.strip("'")) 
                    else:
                        raise ValueError(f"Invalid value: {value_token}")

        if table_name is None:
            raise ValueError("Table name not found in INSERT INTO statement")
        
         # Insert data into the table while enforcing primary key constraints
        success, message = self.database.insert_row(table_name, values)
        print(message)