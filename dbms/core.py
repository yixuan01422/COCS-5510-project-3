from .database import Database
from .query_parser import parse_query
from .ddl.create_table import CreateTableHandler
from .ddl.drop_table import DropTableHandler
from .dml.insert import InsertHandler
from .dml.select import SelectHandler
from .dml.delete import DeleteHandler
from .dml.update import UpdateHandler

class SimpleDBMS:
    def __init__(self):
        self.database = Database()  
        self.create_table_handler = CreateTableHandler(self.database)
        self.drop_table_handler = DropTableHandler(self.database)
        self.insert_handler = InsertHandler(self.database)
        self.select_handler = SelectHandler(self.database)
        self.delete_handler = DeleteHandler(self.database)
        self.update_handler = UpdateHandler(self.database) 

    def execute(self, query):
        """Parse and execute the SQL query."""
        parsed = parse_query(query)
        statement_type = parsed.get_type()

        if statement_type == 'CREATE':
            self.create_table_handler.handle(parsed)
        elif statement_type == 'DROP':
            self.drop_table_handler.handle(parsed)
        elif statement_type == 'INSERT':
            self.insert_handler.handle(parsed)
        elif statement_type == 'SELECT':
            return self.select_handler.handle(parsed)
        elif statement_type == 'DELETE':
            return self.delete_handler.handle(parsed)
        elif statement_type == 'UPDATE':
            return self.update_handler.handle(parsed)
        else:
            raise ValueError(f"Unsupported SQL statement: {statement_type}")
    
    def load(self, num, table_name):
        """
        Generate random rows for a table with incrementing IDs
        
        Args:
            num: Number of rows to generate
            table_name: Name of the table to load data into
        """
        import random
        import string
        
        if table_name not in self.database.tables:
            return False, f"Table '{table_name}' does not exist"
            
        # Get table columns and types
        columns = [col[0] for col in self.database.columns[table_name]]
        types = [col[1] for col in self.database.columns[table_name]]
        
        id_col_index = 0
        id = 1
        for i in range(num):
            row = []
            
            
            for j, col_type in enumerate(types):
                # ID column (first column)
                if j == id_col_index:
                    row.append(id)
                # Generate random data based on column type
                elif col_type == "INT":
                    row.append(random.randint(1, 1000))
                else:  # Default to string for any other type
                    row.append(''.join(random.choices(string.ascii_letters, k=random.randint(5, 10))))
            self.database.tables[table_name].append(row)
            id += 1
        
        print(f"Successfully added {num} random rows to table '{table_name}'")
