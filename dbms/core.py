from .database import Database
from .query_parser import parse_query
from .ddl.create_table import CreateTableHandler
from .ddl.drop_table import DropTableHandler
from .dml.insert import InsertHandler
from .dml.select import SelectHandler
from .dml.delete import DeleteHandler
from .dml.update import UpdateHandler
import random
import string
import time

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
        start_time = time.time()
        parsed = parse_query(query)
        statement_type = parsed.get_type()

        if statement_type == 'CREATE':
            self.create_table_handler.handle(parsed)
        elif statement_type == 'DROP':
            self.drop_table_handler.handle(parsed)
        elif statement_type == 'INSERT':
            self.insert_handler.handle(parsed)
        elif statement_type == 'SELECT':
            self.select_handler.handle(parsed)
        elif statement_type == 'DELETE':
            self.delete_handler.handle(parsed)
        elif statement_type == 'UPDATE':
            self.update_handler.handle(parsed)
        else:
            raise ValueError(f"Unsupported SQL statement: {statement_type}")
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Operation took {execution_time:.2f} seconds")

    def load(self, num, table_name):
        """
        Generate random rows for a table with incrementing IDs
        
        Args:
            num: Number of rows to generate
            table_name: Name of the table to load data into
        """
        
        
        if table_name not in self.database.tables:
            return False, f"Table '{table_name}' does not exist"
            
        # Get table columns and types
        columns = [col[0] for col in self.database.columns[table_name]]
        types = [col[1] for col in self.database.columns[table_name]]
        
        id_col_index = 0
        id = 1
        
        start_time = time.time()
        
        for i in range(num):
            row = []
            
            if i % (num/10) == 0:
                print(f"Loaded {i} rows")
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
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        print(f"Successfully added {num} random rows to table '{table_name}'")
        print(f"Operation took {execution_time:.2f} seconds")

    def create_index(self, table_name, column_name):
        """
        Create an index on a column to improve query performance.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column to index
        """
        start_time = time.time()
        success, message = self.database.create_index(table_name, column_name)
        end_time = time.time()
        execution_time = end_time - start_time
        
        print(message)
        print(f"Index creation took {execution_time:.2f} seconds")
        return success
