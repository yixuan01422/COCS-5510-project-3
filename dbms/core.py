from .database import Database
from .query_parser import parse_query
from .ddl.create_table import CreateTableHandler
from .ddl.drop_table import DropTableHandler
from .dml.insert import InsertHandler
from .dml.select import SelectHandler

class SimpleDBMS:
    def __init__(self):
        self.database = Database()  # Centralized data store
        self.create_table_handler = CreateTableHandler(self.database)
        self.drop_table_handler = DropTableHandler(self.database)
        self.insert_handler = InsertHandler(self.database)
        self.select_handler = SelectHandler(self.database)

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
        else:
            raise ValueError(f"Unsupported SQL statement: {statement_type}")