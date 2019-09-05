import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """It function try to drop the tables creates(staging_events,staging_songs,songplays,users,songs,artists,time).
    Args:
        cur (str): It is the cursor send as parameter.
        conn (str): It is the conection send as parameter.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """It function try to create the tables: staging_events,staging_songs,songplays,users,songs,artists,time.
    Args:
        cur (str): It is the cursor send as parameter.
        conn (str): It is the conection send as parameter.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """It is the principal function where we drop and create the tables :staging_events,staging_songs,songplays,users,songs,artists,time.
    Note:
        First we have to drop all of tables for later create the tables
    """
    #Instance the parameter of the configuration
    config = configparser.ConfigParser()
    #Read the configuration of the cluster
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    #Creation of the cursor
    cur = conn.cursor()
    #Call the function drop_tables
    drop_tables(cur, conn)
    #Call the function create_tables
    create_tables(cur, conn)
    #Close the connection
    conn.close()


if __name__ == "__main__":
    main()