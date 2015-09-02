import sqlman, sqlite3

database_file = "test.db"
working_dir = "."

connection = sqlite3.connect(database_file)
man = sqlman.sqlman(connection, database_file, working_dir)

