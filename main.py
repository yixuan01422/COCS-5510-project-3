from dbms.core import SimpleDBMS

def main():
    db = SimpleDBMS()
    print("example 1:")
    print("create table:")
    db.execute("CREATE TABLE users ( id INT PRIMARY KEY, name STRING, age INT, department_id INT, FOREIGN KEY (department_id) REFERENCES departments(department_id) ON DELETE);")
    db.execute("CREATE TABLE departments ( department_id INT PRIMARY KEY, department_name STRING, location STRING );")
    print("insert data:")
    db.execute("INSERT INTO departments VALUES  ( 1, 'CS', 'Hall');")
    db.execute("INSERT INTO departments VALUES  ( 2, 'Math', 'College');")
    db.execute("INSERT INTO departments VALUES  ( 3, 'Bio', 'Barn');")
    db.execute("INSERT INTO departments VALUES  ( 4, 'History', 'Southwest');")
    

    db.execute("INSERT INTO users VALUES  ( 2, 'Alice', 25, 4);")
    db.execute("INSERT INTO users VALUES  ( 2, 'Tom', 19, 4);") 
    db.execute("INSERT INTO users VALUES (1, 'Bob', 25, 1);")
    db.execute("INSERT INTO users VALUES (4, 'Dog', 19, 2);")
    db.execute("INSERT INTO users VALUES (3, 'Cat', 19, 3);")
    db.execute("INSERT INTO users VALUES (7, 'Man', 27, 1);")
    db.execute("INSERT INTO users VALUES (5, 'Kobe', 31, 3);")
    db.execute("INSERT INTO users VALUES (6, 'James', 27, 2);")
    print("query:")
    db.execute("SELECT * FROM users WHERE id > 5 or id<2;")
    db.execute("SELECT name AS NAME, id FROM users WHERE age > 30 or age < 20 ORDER BY id;")
    db.execute("SELECT COUNT(*), SUM(id) FROM users WHERE age > 25;")
    db.execute("SELECT age, SUM(age) FROM users GROUP BY age;")
    db.execute("SELECT age, AVG(id), COUNT(*) FROM users GROUP BY age HAVING COUNT(*) > 1 AND AVG(age) >= 25;")
    db.execute("SELECT u.id, u.name, u.department_id, d.department_name FROM users u, departments d WHERE u.department_id = d.department_id AND u.id > 3;")
    
    print("example 2:")
    db = SimpleDBMS()
    db.execute("CREATE TABLE table1 ( id INT PRIMARY KEY, col INT);")
    
    db.load(10000, "table1")
    
    print("\nCreating index on id column...")
    db.create_index("table1", "id")
    
    print("\nRunning a query using the index:")
    db.execute("SELECT * FROM table1 WHERE id = 5000;")
    
    print("\nSaving database state...")
    db.save_database("database_backup.json")
    
    print("\nCreating new database instance...")
    new_db = SimpleDBMS()
    
    try:
        print("\nAttempting to query before loading (should fail):")
        new_db.execute("SELECT * FROM table1 WHERE id = 5000;")
    except Exception as e:
        print(f"Expected error: {str(e)}")
    
    print("\nLoading database state...")
    new_db.load_database("database_backup.json")
    
    print("\nRunning the same query after loading:")
    new_db.execute("SELECT * FROM table1 WHERE id = 5000;")
    
    print("\nVerify indexes were loaded (query should use index):")
    new_db.execute("SELECT * FROM table1 WHERE id = 7500;")

if __name__ == "__main__":
    main()