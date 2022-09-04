# Data Lake using Spark

#### Introduction and Motivation
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Sparkify has grown their user base and song database and already has the data into the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. The created architechture should serve the purpose of serving analtyical queries using spark in EMR cluster and load them back to S3

#### Tasks at hand
Load the data from S3 into EMR cluster, process the data into analytics tables using Spark in EMR cluster, and load them back into S3.

#### Available data
Two datasets are available - the song dataset and the log dataset. Both datasets contain multiple files are in JSON file format. The song dataset is a subset of real data from the Million Song Dataset and each file in the dataset contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. The log dataset consists of log files a event simulator based on the songs in the song dataset. 

#### Files in the repository

- etl.py : Reads data from S3 into EMR cluster, processes that data using Spark in EMR cluster, and writes them back to S3 

- dl.cfg : Configuration file that contains the links, access ids for AWS

- README.md : Description of the project and the repository

#### How to run the scripts

1. Create an AWS IAM role with S3 read and write access.
2. Enter the IAM's credentials in the dl.cfg configuration file.
3. Launch the EMR cluster inside AWS with Spark 2.4.0 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.0
4. Then execute this command to start the ETL process to load the data into the database `python3 etl.py`.

#### Database Schema design

We have utilized a star schema design for spark analytical tables that is ideal for analytical query handling but also for data integrity. With star schema, the queries from analytical team can be handled with both high efficiency and performance. The fact table and the dimentsion tables are listed below with their corresponding columns. The first column in the list is the PRIMARY KEY for the corresponding table.

###### Fact Table

- songplays - records in log data associated with song plays i.e. records with page NextSong
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

##### Dimension Tables

- users : users in the app
    - user_id, first_name, last_name, gender, level

- songs : songs in music database
    - song_id, title, artist_id, year, duration

- artists : artists in music database
    - artist_id, name, location, latitude, longitude

- time - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday

#### ETL Pipeline
The ETL pipeline actively segregates tasks into two main functions - process_song_data and process_log_data. In each function the data in the S3 is loaded from the input path, pre-processed and loaded back again to the desired S3 output path. The ETL process in the file etl.py is carried out in the following steps:

1. Connection to the intialized AWS is established.
2. First the song_data is processed using Spark and relevant tables are loaded back to S3
    1. The information pertaining to song metadata is entered into the **songs** table
    2. The information pertaining to artists for each song is entered into the **artists** table
3. Then the log_data is processed using Spark and relevant tables are loaded back to S3  
    1. The timestamp is converted into the unix format and broken down to insert into the **time** table
    2. The information pertaining to the users from the log file is entered into the **users** table
    3. The log information about all songs accessed by users is inserted into **songplays** table


#### Last Steps
1. Remember to delete the EMR clusters so that you don't get charged
2. Remove S3 buckets which you no longer use
