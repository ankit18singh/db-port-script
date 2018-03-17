
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
def create_table(conn, create_table_sql_statement):
    try:
        c = conn.cursor()
        c.execute(create_table_sql_statement)                
    except Error as e:
        print(e)


""" move data from source table to newly created table using the sql_move_data_statement

:param conn: Connection object
:param sql_move_data_queries: statement to move data within two tables
:return:
"""
def move_data(conn, sql_move_data_statement):
    try:
        c = conn.cursor()
        c.execute(sql_move_data_statement)
        conn.commit()
        print("Inserted")                             
    except Error as e:
        print(e)


""" delete data from source table using the delete_from_source_statement

:param conn: Connection object
:param delete_from_source_statement: a DELETE TABLE statement
:return:
"""
def delete_from_source(conn, delete_from_source_statement):
    try:
        c = conn.cursor()  
        c.execute(delete_from_source_statement)
        conn.commit()
        print("Deleted from Source")                
    except Error as e:
        print(e)

""" fetch column data to and return a set to create new tables

:param conn: Connection object
:return table_set: Set of table names
"""
def fetch_table_names(conn):
    
    table_set = set([])
    cursor = conn.execute("SELECT Number from < source database >;")

    for row in cursor:
        table = str(row[0])
        table_set.add(table[:5])
    
    return table_set


"""  
    main method to execute the program.
"""
def main():
    main_db = "< path to .db file >"

    # create a database connection
    conn = sqlite3.connect(main_db)

    table_name_set = fetch_table_names(conn)    

    for item in table_name_set:
        sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS [{table_name}] (
                                        ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                        Number CHAR,
                                        Name CHAR,
                                        Address CHAR,
                                        Date DATE,
                                        Circle INT,
                                        Operator INT
                                    ); """.format(table_name= item)

        sql_move_data_statement = """ INSERT INTO [{table_name}] (Number, Name, Address, Date, Circle, Operator)
                                    SELECT Number, Name, Address, Date, Circle, Operator FROM < source table > 
                                    WHERE substr(Number, 0,6) = '{table_name}';
                                """.format(table_name= item )

        delete_data_statement = """ DELETE FROM < source table >           
                        WHERE substr(Number, 0,6) = '{table_name}';
                        """.format(table_name = item )
    
        if conn is not None:
            # create projects table
            create_table(conn, sql_create_projects_table)
            move_data(conn, sql_move_data_statement)
            delete_from_source(conn, delete_data_statement)
        else:
            print("Error! cannot create the database connection.")
    
        print("~~~~~~~~~~~~ CREATED !~~~~~~~~~~~~~~~~")            

if __name__ == '__main__':
    main()