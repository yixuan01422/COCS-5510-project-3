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

    db.execute("CREATE TABLE users ( id INT PRIMARY KEY, name STRING, age INT,  FOREIGN KEY (department_id) REFERENCES departments(department_id) ON DELETE CASCADE );")
    #db.execute("CREATE TABLE agents ( id STRING PRIMARY KEY, name STRING, age INT );")
    # db.execute("CREATE TABLE agents ( id STRING, name STRING PRIMARY KEY, age INT );")
    # db.execute("DROP TABLE users;")
    # db.execute("DROP TABLE users;")

    # db.execute("INSERT INTO users VALUES  ( 2, 'Alice', 25);")
    # # db.execute("INSERT INTO users VALUES  ( 2, 'Tom', 19);") # for duplicate primary key testing /Expected: ERROR
    # db.execute("INSERT INTO users VALUES (1, 'Bob', 25);")
    # db.execute("INSERT INTO users VALUES (4, 'Dog', 19);")
    # db.execute("INSERT INTO users VALUES (3, 'Cat', 19);")
    # db.execute("INSERT INTO users VALUES (5, 'Kobe', 31);")
    # db.execute("INSERT INTO users VALUES (6, 'James', 27);")
    # db.execute("INSERT INTO users VALUES (7, 'Man', 27);")
    # # db.execute("SELECT * FROM users WHERE id > 3 or age <= 25;")
    # # db.execute("SELECT id FROM users WHERE id<2 or age>=25;")
    # # db.execute("DELETE FROM users WHERE age=31;")
    # # db.execute("SELECT name, id AS N FROM users WHERE age < 30;")
    
    # # db.execute("SELECT * FROM users WHERE id > 2 AND age <= 25;")

    # # db.execute("SELECT MIN(age) FROM users")
    # #db.execute("SELECT COUNT(*), SUM(id) FROM users WHERE age > 25;")
    # # db.execute("SELECT SUM(id) AS sum_id FROM users")
    
    # # db.execute("SELECT AVG(age) AS avg_age FROM users")
    # # db.execute("SELECT * FROM users WHERE id > 2 AND age <= 27 ORDER BY age")
   
    # # db.execute("SELECT * FROM users ORDER BY age ASC;")
    # # db.execute("SELECT * FROM users ORDER BY name DESC;")
    # # db.execute("SELECT name AS NAME, id FROM users WHERE age > 30 or age < 20 ORDER BY id;")
    # # db.execute("SELECT age, SUM(age) FROM users GROUP BY age;")
    # # db.execute("SELECT age, AVG(id), COUNT(*) FROM users GROUP BY age HAVING COUNT(*) > 1 AND AVG(age) >= 25;")
    # #db.execute("UPDATE users SET age=31 WHERE id=3;")
    # db.execute("SELECT * FROM users;")
    # #db.execute("UPDATE users SET age=28 WHERE name='Alice';")
    # #db.execute("UPDATE users SET age=38, name='Robert' WHERE id=2;")
    # db.execute("UPDATE users SET id=60, name='Rob' WHERE name='Kobe';") # this is the one that's not working
    # #db.execute("UPDATE users SET id=9 WHERE name='Man';")
    # db.execute("SELECT * FROM users;")

if __name__ == "__main__":
    main()