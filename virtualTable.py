import sqlite3
from sqlite3 import Error

""" create a database connection to the SQLite database specified by db_file

:param db_file: database file
:return: Connection object or None
""" 
def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
 
    return None


""" create a table from the create_table_sql_statement

:param conn: Connection object
:param create_table_sql_statement: a CREATE TABLE statement
:return:
"""
def create_table(conn, create_virtual_table_query, create_table_query, create_index_query):
    try:
        c = conn.cursor()
        c.execute(create_virtual_table_query)
        print("Virtual table created")
        c.execute(create_table_query)
        print("Search table created")
        c.execute(create_index_query)                
        print("Index table created")
    except Error as e:
        print(e)


""" fetch column data to and return a set to create new tables

:param conn: Connection object
:return table_set: Set of table names
"""
def fetch_table_names(conn):
    
    table_set = set([])
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")

    for row in cursor:
        table = str(row[0])
        table_set.add(table[:5])
    
    return table_set

def main():
    db_path = "< path to db>"

    conn = create_connection(db_path)

    table_names = fetch_table_names(conn)

    for item in table_names:
        create_virtual_table_query = "CREATE VIRTUAL TABLE Search{table_name} USING fts3(Name, content='', columnsize=0, detail=none)".format(table_name= item )
        create_table_query = "CREATE TABLE 'Search{table_name}_config'(k PRIMARY KEY, v) WITHOUT ROWID".format(table_name= item )            
        create_index_query = "CREATE INDEX index_{table_name} ON [{table_name}] ([Number])".format(table_name= item )

        if conn is not None:
            # create projects table
            create_table(conn, create_virtual_table_query, create_table_query, create_index_query)
        else:
            print("Error! cannot create the database connection.")

        print("~~~~~~~~~~~~ CREATED for {table}!~~~~~~~~~~~~~~~~".format(table=item))

if __name__ == '__main__':
    main()