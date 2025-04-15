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
    db.execute("CREATE TABLE table1 ( id INT PRIMARY KEY, col INT);")
    db.load(1000000, "table1")
    db.execute("SELECT * FROM table1 WHERE id > 999990;")
    #db.execute("CREATE TABLE users ( id INT PRIMARY KEY, name STRING, age INT, department_id INT, FOREIGN KEY (department_id) REFERENCES departments(department_id) ON DELETE CASCADE );")
    #db.execute("CREATE TABLE users ( id INT PRIMARY KEY, name STRING, age INT, department_id INT, FOREIGN KEY (department_id) REFERENCES departments(department_id) ON DELETE SET NULL );")
    # db.execute("CREATE TABLE users ( id INT PRIMARY KEY, name STRING, age INT, department_id INT, FOREIGN KEY (department_id) REFERENCES departments(department_id) ON DELETE);")
    # db.execute("CREATE TABLE departments ( department_id INT PRIMARY KEY, department_name STRING, location STRING );")
    # #db.execute("CREATE TABLE agents ( id STRING PRIMARY KEY, name STRING, age INT );")
    # # db.execute("CREATE TABLE agents ( id STRING, name STRING PRIMARY KEY, age INT );")
    # # db.execute("DROP TABLE users;")
    # # db.execute("DROP TABLE users;")

    # db.execute("INSERT INTO departments VALUES  ( 1, 'CS', 'Hall');")
    # db.execute("INSERT INTO departments VALUES  ( 2, 'Math', 'College');")
    # db.execute("INSERT INTO departments VALUES  ( 3, 'Bio', 'Barn');")
    # db.execute("INSERT INTO departments VALUES  ( 4, 'History', 'Southwest');")
    

    # db.execute("INSERT INTO users VALUES  ( 2, 'Alice', 25, 4);")
    # db.execute("INSERT INTO users VALUES  ( 2, 'Tom', 19, 4);") # for duplicate primary key testing /Expected: ERROR
    # db.execute("INSERT INTO users VALUES (1, 'Bob', 25, 1);")
    # db.execute("INSERT INTO users VALUES (4, 'Dog', 19, 2);")
    # db.execute("INSERT INTO users VALUES (3, 'Cat', 19, 3);")
    # db.execute("INSERT INTO users VALUES (5, 'Kobe', 31, 3);")
    # db.execute("INSERT INTO users VALUES (6, 'James', 27, 2);")
    # db.execute("INSERT INTO users VALUES (7, 'Man', 27, 1);")
    

    # db.execute("DELETE FROM departments WHERE department_id = 3;")
    #db.execute("SELECT * FROM users WHERE id > 5 or id<2;")
    #db.execute("SELECT id FROM users WHERE id<2 or age>=25;")
    # # db.execute("DELETE FROM users WHERE age=31;")
    # # db.execute("SELECT name, id AS N FROM users WHERE age < 30;")
    
    # # db.execute("SELECT * FROM users WHERE id > 2 AND age <= 25;")

    # # db.execute("SELECT MIN(age) FROM users")
    # #db.execute("SELECT COUNT(*), SUM(id) FROM users WHERE age > 25;")
    # # db.execute("SELECT SUM(id) AS sum_id FROM users")
    
    #db.execute("SELECT AVG(age) AS avg_age FROM users")
    #db.execute("SELECT * FROM users WHERE id > 2 AND age <= 27 ORDER BY age")
   
    # # db.execute("SELECT * FROM users ORDER BY age ASC;")
    #db.execute("SELECT * FROM users ORDER BY name DESC;")
    #db.execute("SELECT name AS NAME, id FROM users WHERE age > 30 or age < 20 ORDER BY id;")
    #db.execute("SELECT age, SUM(age) FROM users GROUP BY age;")
    #db.execute("SELECT age, AVG(id), COUNT(*) FROM users GROUP BY age HAVING COUNT(*) > 1 AND AVG(age) >= 25;")
    # #db.execute("UPDATE users SET age=31 WHERE id=3;")
    # db.execute("SELECT * FROM users;")
    # #db.execute("UPDATE users SET age=28 WHERE name='Alice';")
    # #db.execute("UPDATE users SET age=38, name='Robert' WHERE id=2;")
    # db.execute("UPDATE users SET id=60, name='Rob' WHERE name='Kobe';") # this is the one that's not working
    # #db.execute("UPDATE users SET id=9 WHERE name='Man';")
    #db.execute("SELECT * FROM departments;")
    #db.execute("SELECT * FROM users;")
    #db.execute("SELECT u.id, u.name, u.department_id, d.department_name FROM users u, departments d WHERE u.department_id = d.department_id AND u.id > 3;")
    #db.execute("SELECT users.id, users.name, users.department_id, departments.department_name FROM users, departments WHERE departments.department_id = users.department_id;")

if __name__ == "__main__":
    main()