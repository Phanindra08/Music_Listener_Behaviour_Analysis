# Music Streaming Analysis Using Spark Structured APIs

## Overview
This project utilizes Spark Structured APIs to perform an in-depth analysis of user listening habits and music trends from a fictional music streaming service. The primary goal is to process and analyze structured data to derive insights into song popularity, user engagement, and genre preferences.

## Dataset Description
The analysis is performed on two datasets, which are generated to simulate user activity and music metadata.

1.  **`listening_logs.csv`**
      * `user_id`: Unique identifier for the user. 
      * `song_id`: Unique identifier for the song.
      * `timestamp`: The date and time the song was played. 
      * `duration_sec`: The duration in seconds for which the song was played. 

2.  **`songs_metadata.csv`**
      * `song_id`: Unique identifier for the song.
      * `title`: The title of the song.
      * `artist`: The name of the artist.
      * `genre`: The genre of the song (e.g., Pop, Rock, Jazz).
      * `mood`: The mood category of the song (e.g., Happy, Sad).

## Repository Structure
The project repository should have the following structure:
```
.
├── outputs/
├── Requirements
├── datagen.py
├── main.py
└── README.md
```

## Output Directory Structure
The results of each analysis task should be saved to individual files within a structured directory as shown below.
```
output/
├── avg_listen_time_per_song/
├── genre_loyalty_scores/
├── night_owl_users/
└── user_favorite_genres/
```

## Tasks and Outputs
The following analytical tasks are performed using Spark Structured APIs, with each output saved to a dedicated CSV file.
1.  **Find each user's favorite genre:** Identifies the most frequently listened-to genre for every user by counting their plays for each genre. <br >
   **Output Path**: `outputs/user_favorite_genres/`
2.  **Calculate the average listen time per song:** Computes the average listening duration in seconds for every song across all user listening events. <br >
   **Output Path:** outputs/avg_listen_time_per_song/
3.  **Compute the genre loyalty score for each user:** Calculates the proportion of a user's listening activity that belongs to their favorite genre. The output is filtered to include only users with a loyalty score greater than 0.8. <br >
   **Output Path:** outputs/genre_loyalty_scores/
4.  **Identify "Night Owl" users:** Extracts a list of users who frequently listen to music during the late-night hours, specifically between 12 AM and 5 AM. <br >
   **Output Path:** outputs/night_owl_users/

## Execution Instructions
## *Prerequisites*

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. *Python 3.x*:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. *PySpark*:
   - Install using pip:
     ```bash
     pip install pyspark
     ```

3. *Apache Spark*:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

### *2. Running the Analysis Tasks*

####  *Running Locally*

1. *Generate the Input*:
  ```bash
   python3 input_generator.py
   ```

2. **Execute Each Task Using spark-submit**:
   ```bash
     spark-submit main.py
   ```

3. *Verify the Outputs*:
   Check the outputs/ directory for the resulting files:
   ```bash
   ls outputs/
   ```

## Errors and Resolutions

1. **AnalysisException:** cannot resolve 'timestamp' given input columns. <br >
   **Resolution:** Corrected the column name references in the script to ensure they matched the DataFrame schema at each stage of the analysis. It is crucial to print the schema (df.printSchema()) after joins to verify column names.
2. **Incorrect Data Type for Timestamp Filtering** <br >
   **Resolution:** Explicitly cast the timestamp column to a TimestampType. This was done by adding the line listening_logs_df = listening_logs_df.withColumn("timestamp", col("timestamp").cast("timestamp")) after loading the data, enabling proper time-based filtering.
3. **Flawed Genre Loyalty Score Calculation** <br >
   **Resolution:** The logic was re-implemented using window functions to rank genres for each user. First, play counts per genre were calculated for each user. Then, a window function rank().over(Window.partitionBy("user_id").orderBy(col("genre_count").desc())) was used to identify the top genre. This was then joined with total user play counts to accurately calculate the loyalty score.
