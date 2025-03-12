from ..database import Database
import sqlparse

class InsertHandler:
    def __init__(self, database: Database):
        self.database = database

    def handle(self, parsed):
        """Handle INSERT INTO statements."""
        table_name = None
        values = []

        pass