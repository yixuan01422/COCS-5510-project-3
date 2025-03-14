from dbms.core import SimpleDBMS

def main():
    db = SimpleDBMS()

    db.execute("CREATE TABLE users ( id INT, name STRING, age INT );")
    db.execute("CREATE TABLE users ( id STRING, name STRING, age INT );")
    # db.execute("DROP TABLE users;")
    # db.execute("DROP TABLE users;")
#     db.execute('''CREATE TABLE products (
#     id STRING PRIMARY KEY,
#     name STRING,
#     price INT,
# );''')
    # # Insert data
    db.execute("INSERT INTO users VALUES  ( 1, 'Alice', 25);")
    # db.execute("INSERT INTO users VALUES (2, 'Bob', 30);")

    # # Select data
    # result = db.execute("SELECT name, age FROM users;")
    # print("SELECT result:", result)  # Output: [{'name': 'Alice', 'age': '25'}, {'name': 'Bob', 'age': '30'}]

if __name__ == "__main__":
    main()