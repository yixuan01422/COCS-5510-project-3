from ..database import Database
import sqlparse

class SelectHandler:
    def __init__(self, database: Database):
        self.database = database

    def handle(self, parsed):
        """Handle SELECT statements."""
        columns = []
        table_name = None

        pass