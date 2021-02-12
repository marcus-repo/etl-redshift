import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copies data to staging tables using the queries in `copy_table_queries`.
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error:
            print("Error: Copying into staging tables")
            raise        


def insert_tables(cur, conn):
    """
    Inserts data to target tables using the queries in `insert_table_queries`.
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error:
            print("Error: Inserting into target tables")
            raise


def main():
    
    """  
    - Reads configuration file.
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Copies logfiles and songfiles from S3 bucket into sparkify 
    staging tables: stg_events, stg_songs
    
    - Inserts data from staging files into sparkify database target tables.
    
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
        
        # copy data to staging tables 
        load_staging_tables(cur, conn)
        
        # insert data from staging to final tables
        insert_tables(cur, conn)
        
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