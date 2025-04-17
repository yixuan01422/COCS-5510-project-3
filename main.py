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
    
    # Create and populate database
    print("Creating and populating database...")
    db.execute("CREATE TABLE table1 ( id INT PRIMARY KEY, col INT);")
    
    # Load fewer rows for faster testing
    db.load(10000, "table1")  # Load 10K rows instead of 1M
    
    # Create an index
    print("\nCreating index on id column...")
    db.create_index("table1", "id")
    
    # Run a query to show it works
    print("\nRunning a query using the index:")
    db.execute("SELECT * FROM table1 WHERE id = 5000;")
    
    # Save the database state
    print("\nSaving database state...")
    db.save_database("database_backup.json")
    
    # Create a new database instance
    print("\nCreating new database instance...")
    new_db = SimpleDBMS()
    
    # Verify the new instance is empty
    try:
        print("\nAttempting to query before loading (should fail):")
        new_db.execute("SELECT * FROM table1 WHERE id = 5000;")
    except Exception as e:
        print(f"Expected error: {str(e)}")
    
    # Load the database state
    print("\nLoading database state...")
    new_db.load_database("database_backup.json")
    
    # Verify the data was loaded by running the same query again
    print("\nRunning the same query after loading:")
    new_db.execute("SELECT * FROM table1 WHERE id = 5000;")
    
    # Show that indexes were loaded too
    print("\nVerify indexes were loaded (query should use index):")
    new_db.execute("SELECT * FROM table1 WHERE id = 7500;")

if __name__ == "__main__":
    main()