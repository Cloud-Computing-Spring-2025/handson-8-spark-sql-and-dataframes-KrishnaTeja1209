from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, trim, desc

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# Split Hashtags column into an array, explode into individual hashtags, and trim whitespace
hashtags_df = posts_df.select(explode(split(col("Hashtags"), ",")).alias("Hashtag"))
hashtags_df = hashtags_df.select(trim(col("Hashtag")).alias("Hashtag"))

# Count frequency of each hashtag and sort in descending order
hashtag_counts = hashtags_df.groupBy("Hashtag").count().orderBy(desc("count"))

# Take top 10 hashtags
top_hashtags = hashtag_counts.limit(10)

# Save result
top_hashtags.coalesce(1).write.mode("overwrite").csv("outputs/hashtag_trends.csv", header=True)
