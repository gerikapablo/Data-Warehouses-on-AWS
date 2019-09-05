import configparser
import psycopg2
from sql_queries import copy_table_queries
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """It function try to load data into the tables staging_events and staging_songs.

    Args:
        cur (str): It is the cursor send as parameter.
        conn (str): It is the conection send as parameter.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """It function try to insert the data into the tables songplays, users, songs, artists, and time.
    
    Args:
        cur (str): It is the cursor send as parameter.
        conn (str): It is the conection send as parameter.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """It is the principal function where we load data into the tables :staging_events,staging_songs,songplays,users,songs,artists,time.ç
    Note:
         First we load the tables staging then we load the others 5 tables.
    """
    #Instance the parameter of the configuration
    config = configparser.ConfigParser()
    #Read the configuration of the cluster
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    #Creation of the cursor
    cur = conn.cursor()
    #Call the function load_staging_tables for load data into the tables staging staging_songs and staging_events
    load_staging_tables(cur, conn)
    #Call the function insert_tables for insert data into the tables Insert into the others tables songplays,users,songs,artists,time
    insert_tables(cur, conn)
    #Close the connection
    conn.close()


if __name__ == "__main__":
    main()