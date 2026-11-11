import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when
from pyspark.sql.types import FloatType, DoubleType

# Project directories
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "results" / "cleaned_data"
OUTPUT_DIR = BASE_DIR / "results" / "parquet"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Initialize Spark
spark = (
    SparkSession.builder
    .appName("CybersecurityThreatAnalytics")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.parquet.enableDictionary", "false")
    .config("spark.sql.parquet.columnarWriterBatchSize", "256")
    .config("spark.sql.files.maxRecordsPerFile", "100000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n===== CYBERSECURITY SPARK PROCESSING =====")

# Locate cleaned CSV files
csv_files = list(DATA_DIR.rglob("*.csv"))

if not csv_files:
    raise FileNotFoundError(
        f"No CSV files found in {DATA_DIR}. "
        "Run your preprocessing script first."
    )

print(f"\nFound {len(csv_files)} CSV files.")

# Load data into Spark
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "DROPMALFORMED")
    .csv([str(file) for file in csv_files])
)

print("\n===== DATASET SCHEMA =====")
df.printSchema()

print("\n===== DATASET STATISTICS =====")
print(f"Total Records: {df.count():,}")
print(f"Total Columns: {len(df.columns)}")

# Remove duplicate records
df = df.dropDuplicates()

print(f"\nRecords after removing duplicates: {df.count():,}")

# Display sample records
print("\n===== SAMPLE RECORDS =====")
df.show(5, truncate=False)


# Analyze missing values
print("\n===== MISSING VALUE ANALYSIS =====")

missing_expressions = []

for field in df.schema.fields:

    column_name = field.name

    # Escape special characters and spaces in column names
    safe_column = col(f"`{column_name.replace('`', '``')}`")

    if isinstance(field.dataType, (FloatType, DoubleType)):

        condition = (
            safe_column.isNull() |
            isnan(safe_column)
        )

    else:
        condition = safe_column.isNull()

    missing_expressions.append(
        count(
            when(condition, 1)
        ).alias(column_name)
    )

missing_counts = df.select(*missing_expressions)

missing_counts.show(truncate=False)

# Repartition for memory-efficient processing
df = df.repartition(8)

print("\n===== PARTITION INFORMATION =====")
print(f"Number of partitions: {df.rdd.getNumPartitions()}")

# Write Parquet dataset
print("\n===== WRITING PARQUET DATA =====")

df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(str(OUTPUT_DIR))

print("\nParquet files successfully generated!")
print("\n===== PROCESSING COMPLETED =====")
print(f"Parquet output: {OUTPUT_DIR}")

spark.stop()
