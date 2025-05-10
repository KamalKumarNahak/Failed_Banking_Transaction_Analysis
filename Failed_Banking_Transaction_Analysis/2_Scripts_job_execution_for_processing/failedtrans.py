from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("FailedTransactionAnalysis").getOrCreate()

# Read the cleaned combined CSV file from GCS
df_clean = spark.read.option("header", "true").csv("gs://kamalkumar-bucket/cleaned/all_transactions.csv")

# Filter for failed transactions
failed_df = df_clean.filter(col("transaction_status").isin("FAIL", "FAILED", "PENDING"))

# Write failed transactions to Cloud SQL MySQL
failed_df.write.format("jdbc").options(
    url="jdbc:mysql://34.30.149.196:3306/banking_db",
    driver="com.mysql.cj.jdbc.Driver",
    dbtable="failed_transactions",
    user="kamal",
    password="123"
).mode("overwrite").save()

spark.stop()

