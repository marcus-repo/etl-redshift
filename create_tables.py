import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error:
            print("Error: Tables not deleted")
            raise


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error:
            print("Error: Tables not created")
            raise


def main():
    """  
    - Reads configuration file.
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    
    # read config
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # set process status
    success = False
    conn = None
    try:
        # connect database
        conn = psycopg2.connect("host={} dbname={} user={} password={} \
                                port={}".format(*config['CLUSTER'].values()))
        # create cursor
        cur = conn.cursor()
        
        # drop and create tables
        drop_tables(cur, conn)
        create_tables(cur, conn)
        
        # change process status
        success = True
    except psycopg2.Error as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()
        if success:
            print('Process suceeded')
        else:
            print('Process failed')


if __name__ == "__main__":
    main()