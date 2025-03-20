from dbms.core import SimpleDBMS

def main():
    db = SimpleDBMS()

    db.execute("CREATE TABLE users ( id INT PRIMARY KEY, name STRING, age INT );")
    #db.execute("CREATE TABLE agents ( id STRING PRIMARY KEY, name STRING, age INT );")
    db.execute("CREATE TABLE agents ( id STRING, name STRING PRIMARY KEY, age INT );")
    # db.execute("DROP TABLE users;")
    # db.execute("DROP TABLE users;")
    # # Insert data
    db.execute("INSERT INTO users VALUES  ( 1, 'Alice', 25);")
    db.execute("INSERT INTO users VALUES  ( 1, 'Tom', 19);") # for duplicate primary key testing /Expected: ERROR
    db.execute("INSERT INTO users VALUES (2, 'Bob', 30);")
    db.execute("INSERT INTO users VALUES (3, 'Dog', 40);")
    db.execute("INSERT INTO users VALUES (4, 'Cat', 50);")
    # db.execute("SELECT * FROM users;")
    #db.execute("SELECT id FROM users WHERE id>2 or age<=25;")
    #db.execute("DELETE FROM users WHERE id<2 or age>30;")
    #db.execute("SELECT name, id AS N FROM users WHERE age > 30;")
    
    #need to fix
    #db.execute("SELECT id FROM users WHERE id > 2 OR age <= 25;")

    db.execute("SELECT MAX(age) AS min_age FROM users")
    db.execute("SELECT COUNT(name) AS count_name FROM users")
    db.execute("SELECT SUM(id) AS sum_id FROM users")
    db.execute("SELECT AVG(age) AS avg_age FROM users")



    # db.execute("INSERT INTO agents VALUES  ( 'A', 'Ben', 35);")
    # db.execute("INSERT INTO agents VALUES  ( 'A', 'Ben', 45);") # for duplicate primary key testing /Expected: ERROR
    # db.execute("INSERT INTO agents VALUES ('C', 'Tucker', 84);")
    # # # Select data
    # result = db.execute("SELECT name, age FROM users;")
    # print("SELECT result:", result)  # Output: [{'name': 'Alice', 'age': '25'}, {'name': 'Bob', 'age': '30'}]

if __name__ == "__main__":
    main()