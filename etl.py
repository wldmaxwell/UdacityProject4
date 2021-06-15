import configparser
from datetime import datetime as dt
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as t
from pyspark.sql.functions import monotonically_increasing_id
import time


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
                                           



def create_spark_session():
    """
        Creates Apache Spark Session to Process Data
        
        Keyword Arguments:
        * N/A
        
        Output:
        * spark -- Apache Spark Session
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
        Loads JSON Song Data, Processes the data into 
        Songs and Artists table into parquet files
        
        Keyword Arguments:
        * spark -- Apache Spark Session
        * input_data -- Path to Song Data Files Amazon S3 or Local
        * output_data -- Path to store Parquet Output Files Amazon S3 or Local
        
        Output:
        * Song and Artist Table Parquet files stored in Amazon S3 or Local
    """
    # get filepath to song data file
    song_data = input_data
    
    start_time = time.time()
    start_date = dt.now()
    
    # read song data file
    
    song_data_df = spark.read.json(song_data)
    
    end_time = time.time()
    end_date = dt.now()
    
    time_taken = end_time - start_time
    date_time_taken = end_date - start_date
    
    print(f"------ song data reading complete in {time_taken} Seconds -------")
    

    # extract columns to create songs table
                                           
    song_data_df.createOrReplaceTempView("song_data_DF")
                                           
    songs_table = spark.sql("""
    SELECT  DISTINCT song_id, 
            title, 
            artist_id, 
            year, 
            duration
    FROM song_data_DF
    ORDER BY song_id
    """)
    print()
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "song_table.parquet")
    
    print("------- Done Writing Song Table Parquet File -------")

    # extract columns to create artists table
    
    song_data_df.createOrReplaceTempView("artist_data_DF")
                                           
    artists_table = artist_table = spark.sql("""
        SELECT
        DISTINCT artist_id,
                 artist_name as name,
                 artist_location as location,
                 artist_latitude as latitude,
                 artist_longitude as longitude
        FROM artist_data_DF
        ORDER BY artist_id
                 """)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artist_table.parquet")
    
    print("------- Done Writing Artist Table Parquet File -------")

def process_log_data(spark, song_input_data, log_input_data, output_data):
    
    """
            Load JSON Song and Log Data, Processes the data into 
            User, Time, and Songplay tables into parquet files

            Keyword Arguments:
            * spark -- Apache Spark Session
            * song_input_data -- Path to Song Data Files in Amazon S3 or Local
            * log_input_data -- Path to Log Data Files in Amazon S3 or Local
            * output_data -- Path to store Parquet Output Files Amazon S3 or Local

            Output:
            * User, Time, and Songplay Table Parquet files stored in Amazon S3 or Local
    """
    # get filepath to log data file
    log_data = log_input_data

    # read log data file
    log_data_df = spark.read.json(log_data)
    
    print("------ Done Reading log data file -----")
    
    # filter by actions for song plays
    log_data_df = log_data_df.filter(log_data_df.page == 'NextSong')

    # extract columns for users table 
    log_data_df.createOrReplaceTempView("user_table_DF")
                                           
    user_table = spark.sql("""
        SELECT DISTINCT int(userId) as user_id,
                        firstName as first_name,
                        lastName as last_name,
                        gender,
                        level
        FROM user_table_DF
        ORDER BY user_id
                    """)
    
    # write users table to parquet files
    user_table.write.mode("overwrite").partitionBy("user_id").parquet(output_data + "user_table.parquet")
    
    print("------- Done Writing User Table Parquet File -------")
    # create timestamp column from original timestamp column
    def format_timestamp(ts):
        return dt.fromtimestamp(ts / 1000.0)
                                           
    get_timestamp = udf(lambda x: format_timestamp(x), t.TimestampType())
    log_data_df = log_data_df.withColumn('timestamp', get_timestamp(log_data_df['ts']))
    
    # create datetime column from original timestamp column
                                           
    def format_datetime(ts):
        return dt.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
                                           
    get_datetime = udf(lambda x: format_datetime(x), t.StringType())
                                           
    log_data_df = log_data_df.withColumn('datetime', get_datetime(log_data_df['ts']))
    
    # extract columns to create time table
    log_data_df.createOrReplaceTempView("time_table_DF")
                                           
    time_table = spark.sql("""
        SELECT  DISTINCT datetime as start_time,
                hour(timestamp) as hour,
                day(timestamp) as day,
                weekofyear(timestamp) as week,
                month(timestamp) as month,
                year(timestamp) as year,
                dayofweek(timestamp) as weekday
        FROM time_table_DF
        ORDER BY start_time """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time_table.parquet")
    
    print("------- Done Writing Time Table Parquet File -------")

    # read in song data to use for songplays table
    song_df = song_input_data

    # extract columns from joined song and log datasets to create songplays table 
    song_log_join_df = song_df.join(log_data_df, (log_data_df.artist == song_df.artist_name) & (log_data_df.song == song_df.title))
                                           
    song_log_join_df = song_log_join_df.withColumn("songplay_id", monotonically_increasing_id())
                                           
    song_log_join_df.createOrReplaceTempView('songplay_table_df')
    
    songplays_table = spark.sql("""
        SELECT  songplay_id,
                timestamp as start_time,
                userId as user_id,
                level,
                song_id,
                artist_id,
                sessionId as session_id,
                location,
                userAgent as user_agent,
                year(timestamp) as year,
                month(timestamp) as month
        FROM songplay_table_df
        ORDER BY (user_id, session_id)
            """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy('year', 'month').parquet(output_data + "songplay_table.parquet")
    
    print("---- Done Writing songplay table Parquet File ----- ")


def main():
    
    """
        Loads JSON Log and Song Data, Processes the data into 
        Song, Artist, User, Time, and Songplay tables into parquet files
        
        Keyword Arguments:
        * N/A
        
        Output:
        * Song Table Parquet File
        * Artist Table Parquet File
        * User Table Parquet File
        * Time Table Parquet File
        * Songplay Table Parquet File
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    song_data = config.get('S3_Input', 'Song_Data')
    song_data_test = config.get('S3_Input', 'Song_Data_Test')
    log_data = config.get('S3_Input', 'Log_Data')                                      
    output_data = config.get('S3_Output', 'Output_Data')
    
    process_song_data(spark, song_data_test, output_data)    
    process_log_data(spark, song_data_test, log_data, output_data)


if __name__ == "__main__":
    main()
