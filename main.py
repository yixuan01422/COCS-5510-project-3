from dbms.core import SimpleDBMS
'''
SELECT ...
FROM ...
WHERE ...
GROUP BY ...
HAVING ...
ORDER BY ...
'''
def main():
    db = SimpleDBMS()
    
    # Create basic tables for testing
    db.execute("CREATE TABLE table1 ( id INT PRIMARY KEY, col INT);")
    
    db.load(1000000, "table1")  # Load 1 million rows
    
    print("\nQuerying WITHOUT index:")
    db.execute("SELECT * FROM table1 WHERE id = 500000;")
    
    print("\nCreating index on id column...")
    db.create_index("table1", "id")
    
    print("\nQuerying WITH index:")
    db.execute("SELECT * FROM table1 WHERE id = 500000;")
    
    # # Also create an index on a non-primary key column
    # print("\nCreating index on non-primary key column (col)...")
    # db.create_index("table1", "col")
    
    # # Query using the non-primary key index
    # print("\nQuerying using non-primary key index:")
    # db.execute("SELECT * FROM table1 WHERE col = 500;")

if __name__ == "__main__":
    main()