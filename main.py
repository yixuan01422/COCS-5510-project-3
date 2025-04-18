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
    db.execute("INSERT INTO departments VALUES  ( 5, 'Physics', 'Science Building');")
    db.execute("INSERT INTO departments VALUES  ( 6, 'Chemistry', 'Lab Center');")
    db.execute("INSERT INTO departments VALUES  ( 7, 'English', 'Liberal Arts');")
    db.execute("INSERT INTO departments VALUES  ( 8, 'Economics', 'Business Hall');")
    

    db.execute("INSERT INTO users VALUES  ( 2, 'Alice', 25, 4);")
    db.execute("INSERT INTO users VALUES  ( 2, 'Tom', 19, 4);") 
    db.execute("INSERT INTO users VALUES (1, 'Bob', 25, 1);")
    db.execute("INSERT INTO users VALUES (4, 'Dog', 19, 2);")
    db.execute("INSERT INTO users VALUES (3, 'Cat', 19, 3);")
    db.execute("INSERT INTO users VALUES (7, 'Man', 27, 1);")
    db.execute("INSERT INTO users VALUES (5, 'Kobe', 31, 3);")
    db.execute("INSERT INTO users VALUES (6, 'James', 27, 2);")
    db.execute("INSERT INTO users VALUES (8, 'Sarah', 22, 5);")
    db.execute("INSERT INTO users VALUES (9, 'Michael', 35, 6);")
    db.execute("INSERT INTO users VALUES (10, 'Emily', 24, 7);")
    db.execute("INSERT INTO users VALUES (11, 'David', 29, 8);")
    db.execute("INSERT INTO users VALUES (12, 'Jessica', 21, 5);")
    db.execute("INSERT INTO users VALUES (13, 'Alex', 26, 1);")
    db.execute("INSERT INTO users VALUES (14, 'Lisa', 30, 3);")
    db.execute("INSERT INTO users VALUES (15, 'Kevin', 23, 2);")
    db.execute("INSERT INTO users VALUES (16, 'Rachel', 27, 7);")
    db.execute("INSERT INTO users VALUES (17, 'Mark', 32, 8);")
    db.execute("INSERT INTO users VALUES (18, 'Emma', 20, 4);")
    db.execute("INSERT INTO users VALUES (19, 'John', 28, 6);")
    db.execute("INSERT INTO users VALUES (20, 'Sophie', 25, 3);")
    db.execute("INSERT INTO users VALUES (21, 'Ryan', 33, 1);")
    db.execute("INSERT INTO users VALUES (22, 'Olivia', 24, 2);")
    db.execute("INSERT INTO users VALUES (23, 'Daniel', 29, 5);")
    db.execute("INSERT INTO users VALUES (24, 'Grace', 26, 8);")
    db.execute("INSERT INTO users VALUES (25, 'Nathan', 31, 7);")
    db.execute("INSERT INTO users VALUES (26, 'Amy', 22, 6);")
    db.execute("INSERT INTO users VALUES (27, 'Thomas', 27, 4);")
    db.execute("INSERT INTO users VALUES (28, 'Zoe', 25, 1);")
    db.execute("INSERT INTO users VALUES (29, 'Chris', 30, 3);")
    db.execute("INSERT INTO users VALUES (30, 'Jennifer', 28, 2);")
    db.execute("INSERT INTO users VALUES (31, 'Jacob', 34, 5);")
    db.execute("INSERT INTO users VALUES (32, 'Rebecca', 23, 6);")
    db.execute("INSERT INTO users VALUES (33, 'Andrew', 29, 8);")
    db.execute("INSERT INTO users VALUES (34, 'Nicole', 26, 7);")
    db.execute("INSERT INTO users VALUES (35, 'Patrick', 31, 1);")
    db.execute("INSERT INTO users VALUES (36, 'Hannah', 25, 4);")
    db.execute("INSERT INTO users VALUES (37, 'Peter', 33, 3);")
    db.execute("INSERT INTO users VALUES (38, 'Laura', 24, 5);")
    db.execute("INSERT INTO users VALUES (39, 'Benjamin', 27, 2);")
    db.execute("INSERT INTO users VALUES (40, 'Samantha', 30, 6);")
    print("query:")
    # db.execute("SELECT * FROM users WHERE id > 5 or id<2;")
    # db.execute("SELECT name AS NAME, id FROM users WHERE age > 30 or age < 20 ORDER BY id;")
    # db.execute("SELECT COUNT(*), SUM(id) FROM users WHERE age > 25;")
    # db.execute("SELECT age, SUM(age) FROM users GROUP BY age;")
    # db.execute("SELECT age, AVG(id), COUNT(*) FROM users GROUP BY age HAVING COUNT(*) > 1 AND AVG(age) >= 25;")
    
    # Demonstrate join with Cartesian product (default)
    print("\nJoin using Cartesian product (default):")
    db.execute("SELECT u.id, u.name, u.department_id, d.department_name FROM users u, departments d WHERE u.department_id = d.department_id AND u.id > 3;")
    
    # Switch to sort-merge join
    print("\nSwitching to sort-merge join method:")
    db.set_join_method(True)
    
    # Demonstrate join with sort-merge
    print("\nJoin using sort-merge algorithm:")
    db.execute("SELECT u.id, u.name, u.department_id, d.department_name FROM users u, departments d WHERE u.department_id = d.department_id AND u.id > 3;")
    
    # # Switch back to Cartesian product
    # print("\nSwitching back to Cartesian product join method:")
    # db.set_join_method(False)
    
    # print("example 2:")
    # db = SimpleDBMS()
    # db.execute("CREATE TABLE table1 ( id INT PRIMARY KEY, col INT);")
    
    # db.load(10000, "table1")
    
    # print("\nCreating index on id column...")
    # db.create_index("table1", "id")
    
    # print("\nRunning a query using the index:")
    # db.execute("SELECT * FROM table1 WHERE id = 5000;")
    
    # print("\nSaving database state...")
    # db.save_database("database_backup.json")
    
    # print("\nCreating new database instance...")
    # new_db = SimpleDBMS()
    
    # try:
    #     print("\nAttempting to query before loading (should fail):")
    #     new_db.execute("SELECT * FROM table1 WHERE id = 5000;")
    # except Exception as e:
    #     print(f"Expected error: {str(e)}")
    
    # print("\nLoading database state...")
    # new_db.load_database("database_backup.json")
    
    # print("\nRunning the same query after loading:")
    # new_db.execute("SELECT * FROM table1 WHERE id = 5000;")
    
    # print("\nVerify indexes were loaded (query should use index):")
    # new_db.execute("SELECT * FROM table1 WHERE id = 7500;")

if __name__ == "__main__":
    main()