from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
import time

# Initializing SparkSession
spark = SparkSession.builder \
    .appName("Problem 3 - Titanic Analysis") \
    .getOrCreate()

# Loading the titanic.csv dataset
data_path = "./titanic.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Part (a): Computing Median Age
start_time_a = time.time()
def compute_median_age(df):
    df_with_default = df.withColumn("Age", when(col("Age").isNull(), 23).otherwise(col("Age")))
    median_age = df_with_default.approxQuantile("Age", [0.5], 0.01)[0]
    return median_age

median_age_value = compute_median_age(df)
end_time_a = time.time()
runtime_a = end_time_a - start_time_a
print(f"Part (a) - Median Age (with nulls replaced by 23): {median_age_value}")
print(f"Part (a) - Runtime: {runtime_a} seconds")

# Part (b): Query for passengers with age > median age
start_time_b = time.time()
query_result = df.filter(col("Age") > median_age_value) \
                .select("Name", "Fare") \
                .orderBy("Name")
result_count = query_result.count()  # Counting the number of results
end_time_b = time.time()
runtime_b = end_time_b - start_time_b
print(f"Part (b) - Passengers with age greater than median age: {result_count})")
query_result.show(n=query_result.count(), truncate=False)  # Show first
print(f"Part (b) - Runtime: {runtime_b} seconds")

# Part (c): Verifying the hypothesis
start_time_c = time.time()
avg_fare_by_survival = df.groupBy("Survived") \
                        .agg(F.avg("Fare").alias("AvgFare")) \
                        .orderBy("Survived")
avg_fare_survived = avg_fare_by_survival.filter(col("Survived") == 1).collect()[0]["AvgFare"]
avg_fare_not_survived = avg_fare_by_survival.filter(col("Survived") == 0).collect()[0]["AvgFare"]
difference = abs(avg_fare_survived - avg_fare_not_survived)

print(f"Part (c) - Average fare by survival status:")
avg_fare_by_survival.show()
print(f"Part (c) - Average fare for survivors: {avg_fare_survived}")
print(f"Part (c) - Average fare for non-survivors: {avg_fare_not_survived}")
print(f"Part (c) - Difference in average fares: {difference}")
if difference < 10:  # Arbitrary threshold
    print("Part (c) - Conclusion: The average fare prices are not very different.")
else:
    print("Part (c) - Conclusion: The average fare prices are noticeably different.")
end_time_c = time.time()
runtime_c = end_time_c - start_time_c
print(f"Part (c) - Runtime: {runtime_c} seconds")

# Stopping the SparkSession
spark.stop()
