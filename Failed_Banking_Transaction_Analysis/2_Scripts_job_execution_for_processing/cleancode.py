from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, regexp_replace, to_date

# Initialize Spark session
spark = SparkSession.builder.appName("FailedTransactionAnalysis").getOrCreate()

# Load all CSVs
df = spark.read.option("header", "true").csv("gs://kamalkumar-bucket/transactions/*.csv")

# Drop fully empty rows
df = df.dropna(how="all")

# Replace empty strings with None
for c in df.columns:
    df = df.withColumn(c, when(trim(col(c)) == "", None).otherwise(col(c)))

# Drop rows missing essential fields
df = df.dropna(subset=["transaction_id", "account_no", "transaction_status"])

# === Additional Transformations ===

# Clean and cast amount columns
df = df.withColumn("deposit_amt", regexp_replace(regexp_replace("deposit_amt", "∅", "0"), "[$]", "").cast("float"))
df = df.withColumn("withdrawal_amt", regexp_replace(regexp_replace("withdrawal_amt", "∅", "0"), "[$]", "").cast("float"))
df = df.withColumn("balance_amt", regexp_replace("balance_amt", "[$]", "").cast("float"))

# Clean non-numeric or identifier columns
df = df.withColumn("chq_no", when(col("chq_no") == "∅", None).otherwise(col("chq_no")))
df = df.withColumn("bank_id", col("bank_id").cast("int"))
df = df.withColumn("transaction_details", trim(col("transaction_details")))

# Handle special NULL strings and date conversion
df = df.withColumn("value_date", when(col("value_date") == "#NULL!", None).otherwise(col("value_date")))
df = df.withColumn("value_date", to_date("value_date", "yyyy-MM-dd"))

# Save cleaned data
df.write.mode("overwrite").csv("gs://kamalkumar-bucket/cleaned/all_transactions.csv", header=True)
