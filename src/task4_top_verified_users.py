from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# Initialize Spark session
spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# Join posts with users on UserID
joined_df = posts_df.join(users_df, on="UserID")

# Filter for verified users
verified_df = joined_df.filter(col("Verified") == True)

# Calculate Reach = Likes + Retweets
verified_df = verified_df.withColumn("Reach", col("Likes") + col("Retweets"))

# Aggregate total reach by user
top_verified = verified_df.groupBy("Username").agg(
    _sum("Reach").alias("Total_Reach")
).orderBy(col("Total_Reach").desc()).limit(5)

# Save result
top_verified.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)
