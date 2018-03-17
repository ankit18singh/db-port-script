
import sqlite3
from sqlite3 import Error

# Method to create db connection. 
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
 
    return None

# Method to create table
def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """

    try:
        c = conn.cursor()
        c.execute(create_table_sql)                
    except Error as e:
        print(e)

def move_data(conn, sql_move_data_queries):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """

    try:
        c = conn.cursor()
        c.execute(sql_move_data_queries)
        conn.commit()
        print("Inserted")                             
    except Error as e:
        print(e)

def delete_from_source(conn, delete_from_source):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """

    try:
        c = conn.cursor()  
        c.execute(delete_from_source)
        conn.commit()
        print("Deleted from Source")                
    except Error as e:
        print(e)

# Method to fetch table names
def fetch_table_names(conn):
    
    table_list = []
    table_set = set([])
    cursor = conn.execute("SELECT Number from ekyc1;")

    for row in cursor:
        table = str(row[0])
        table_set.add(table[:5])
    
    return table_set


# Main method.
def main():
    main_db = "./testdb/testcg.db"

    # create a database connection
    conn = sqlite3.connect(main_db)

    tablelist = fetch_table_names(conn)    

    for table in tablelist:
        sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS [{tabl}] (
                                        ID INTEGER PRIMARY KEY AUTOINCREMENT,
                                        Number CHAR,
                                        Name CHAR,
                                        Address CHAR,
                                        Date DATE,
                                        Circle INT,
                                        Operator INT
                                    ); """.format(tabl= table)

        sql_move_data_queries = """ INSERT INTO [{tabl}] (Number, Name, Address, Date, Circle, Operator)
                                    SELECT Number, Name, Address, Date, Circle, Operator FROM ekyc1 
                                    WHERE substr(Number, 0,6) = '{tabl}';
                                """.format(tabl= table)

        delete_data = """ DELETE FROM ekyc1           
                        WHERE substr(Number, 0,6) = '{tabl}';
                        """.format(tabl = table)
    
        if conn is not None:
            # create projects table
            create_table(conn, sql_create_projects_table)
            move_data(conn, sql_move_data_queries)
            delete_from_source(conn, delete_data)
        else:
            print("Error! cannot create the database connection.")
    
        print("~~~~~~~~~~~~ CREATED !~~~~~~~~~~~~~~~~")            

if __name__ == '__main__':
    main()