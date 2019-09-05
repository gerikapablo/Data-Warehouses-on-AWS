1.-The purpose of this design is to store the information of the Sparkify application so that they can obtain information about the preferences of their users for a certain song, time of duration in which the song was heard, season, schedule, etc.
2.-The star model was used, creating 4 dimensional tables and a fact table that stores the keys and common information of the tables with which it is related

Steps to excute the scripts:
1.-Configure 'dwh.cfg' to put string connection about the cluster
2.-Configure 'sql_queries.py' for drop,create the structure of the tables songplays,users,songs,artists,time,staging_events, and staging_songs and insert the data into the tables before mencioned.
3.-Execute 'create_tables.py' 
4.-Execute 'etl.py' to insert data into the tables ,staging_events, and staging_songs,songplays,users,songs,artists,time with information of the before step (3).