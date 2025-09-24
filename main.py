# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Load datasets
listening_logs_df = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs_metadata_df = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Correcting data types
listening_logs_df = listening_logs_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Join the two DataFrames for analysis
joined_df = listening_logs_df.join(songs_metadata_df, "song_id")

# Task 1: Find Each User's Favorite Genre
window_spec = Window.partitionBy("user_id").orderBy(desc("play_count"))
user_favorite_genres = (
    joined_df.groupBy("user_id", "genre")
    .agg(count("*").alias("play_count"))
    .withColumn("rank", row_number().over(window_spec))
    .filter("rank == 1")
    .drop("rank")
)
user_favorite_genres.coalesce(1).write.csv("outputs/user_favorite_genres", mode="overwrite", header=True)

# Task 2: Average Listen Time
avg_listen_time_per_song = (
    joined_df.groupBy("song_id", "title")
    .agg(avg("duration_sec").alias("average_duration_sec"))
)
avg_listen_time_per_song.coalesce(1).write.csv("outputs/avg_listen_time_per_song", mode="overwrite", header=True)

# Task 3: Genre Loyalty Scores
# The correct logic for genre loyalty involves a more direct calculation.
total_plays_per_user = joined_df.groupBy("user_id").count().withColumnRenamed("count", "total_plays")
favorite_genre_plays = (
    joined_df.groupBy("user_id", "genre")
    .agg(count("*").alias("plays_in_genre"))
    .withColumn("rank", rank().over(Window.partitionBy("user_id").orderBy(desc("plays_in_genre"))))
    .filter("rank == 1")
)
genre_loyalty_scores = (
    favorite_genre_plays.join(total_plays_per_user, on="user_id", how="inner")
    .withColumn("loyalty_score", col("plays_in_genre") / col("total_plays"))
    .filter("loyalty_score > 0.8")
    .select("user_id", "loyalty_score")
)

genre_loyalty_scores.coalesce(1).write.csv("outputs/genre_loyalty_scores", mode="overwrite", header=True)

# Task 4: Identify users who listen between 12 AM and 5 AM
night_owl_users = (
    listening_logs_df.filter(hour("timestamp").between(0, 5))
    .select("user_id")
    .distinct()
)
night_owl_users.coalesce(1).write.csv("outputs/night_owl_users", mode="overwrite", header=True)

spark.stop()