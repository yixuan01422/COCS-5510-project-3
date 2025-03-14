class Database:
    def __init__(self):
        self.tables = {}  
        self.columns = {}  

    def create_table(self, table_name, columns):
        """Create a new table."""
        if table_name in self.tables:
            return False
        self.columns[table_name] = columns
        self.tables[table_name] = []
        return True

    def drop_table(self, table_name):
        """Drop an existing table."""
        if table_name in self.tables:
            del self.tables[table_name]
            del self.columns[table_name]
            return True
        return False
        #todo: handle error if table does not exist

    def insert_row(self, table_name, row):
        """Insert a row into a table."""
        if table_name in self.tables:
            self.tables[table_name].append(row)

    def get_table_data(self, table_name):
        """Get all rows from a table."""
        return self.tables.get(table_name, [])