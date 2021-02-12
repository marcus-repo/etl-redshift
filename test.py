import configparser
import psycopg2
from prettytable import PrettyTable


def get_test_definition():
    """
    returns list of dictionaries with test definition and sql statements.
    """
    
    ## DATA QUALITY TEST DEFINITION (SQL)
   
    # USERS Count
    user_stg_count = """SELECT COUNT(DISTINCT userId) 
                        FROM stg_events WHERE page='NextSong';"""
    
    user_dim_count = """SELECT COUNT(user_id) FROM users;"""
    
    
    # ARTISTS Count
    artist_stg_count = """SELECT COUNT(DISTINCT artist_id) FROM stg_songs;"""
    
    artist_dim_count = """SELECT COUNT(artist_id) from artists;"""
    
    
    # SONGS Count
    song_stg_count = """SELECT COUNT(DISTINCT song_id) FROM stg_songs;"""
    
    song_dim_count = """SELECT COUNT(song_id) FROM songs;"""
    
    
    # TIME Count
    time_stg_count = """SELECT COUNT(DISTINCT ts) FROM stg_events;"""
    
    time_dim_count = """SELECT COUNT(start_time) FROM time;"""
    
    
    # SONGPLAYS Count
    songplays_stg_count = """SELECT COUNT(1) FROM stg_events
                             WHERE page='NextSong';"""
    
    songplays_fact_count = """SELECT COUNT(1) FROM songplays;"""

    
    ## BUILD QUERY LIST
    user = {'test': 'Count Distinct user_id', 
            'source': user_stg_count, 
            'target': user_dim_count}
    
    artist = {'test': 'Count Distinct artist_id', 
              'source': artist_stg_count, 
              'target': artist_dim_count}
    
    song = {'test': 'Count Distinct song_id', 
            'source': song_stg_count, 
            'target': song_dim_count}
    
    time = {'test': 'Count Distinct time_stamp', 
            'source': time_stg_count, 
            'target': time_dim_count}
    
    songplays = {'test': 'Record Count songplays',
                 'source': songplays_stg_count,
                 'target': songplays_fact_count}
    
    test_definition = [user, artist, song, time, songplays]
    
    return test_definition
    

def run_test(cur, conn, test_definition):
    """
    Runs test definition against database and
    returns list of dictionaries with test results.
    """

    test_results = []
    test_result = {}
    for test in test_definition:
        try:
            # run test against source table
            cur.execute(test['source'])
            source_value = cur.fetchone()[0]
            
            # run test against target table
            cur.execute(test['target'])
            target_value = cur.fetchone()[0]
            
            # calculate differences
            diff = source_value - target_value
            
            # assign result
            test_result = {'test': test['test'], 'source':source_value, 
                           'target': target_value, 'diff': diff}
            
            # build result list
            test_results.append(test_result)
        
        except psycopg2.Error:
            print("Error: Retrieving results from database")
            raise
    
    return test_results

            
def print_test_results(test_results):
    """
    Prints test results to console in user friendly format.
    """
    
    t = PrettyTable(['test', 'source', 'target', 'difference'])
    for row in test_results:
        t.add_row([row['test'], row['source'], row['target'], row['diff']])
    
    print("Test Results - Please investigate differences")
    print(t)
    print("source = staging tables")
    print("target = dimension tables / fact table")
    print(" ")
    
            
def main():
    """  
    - Reads configuration file.
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Retrieves data quality test definition (SQL statements).
    
    - Runs test definition against database.
    
    - Prints test definition in table format.
    
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
        
        # get test definition
        test_definition = get_test_definition()
        
        # run tests against database
        test_results = run_test(cur, conn, test_definition)
        
        # print results
        print_test_results(test_results)
        
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