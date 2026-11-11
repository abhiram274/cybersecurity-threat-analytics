from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper

# --------------------------------------------------
# PROJECT CONFIGURATION
# --------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent

PARQUET_DIR = BASE_DIR / "results" / "parquet"
REPORT_DIR = BASE_DIR / "results" / "analytics"

REPORT_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("CybersecuritySparkSQLAnalytics")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# LOAD PROCESSED DATA
# --------------------------------------------------

print("\n===== LOADING PARQUET DATA =====")

df = spark.read.parquet(str(PARQUET_DIR))

df.cache()

print("Total Records:", f"{df.count():,}")
print("Total Columns:", len(df.columns))

print("\nAvailable Columns:")
print(df.columns)


# --------------------------------------------------
# IDENTIFY IMPORTANT COLUMNS
# --------------------------------------------------

def find_column(name):
    for column_name in df.columns:
        if column_name.strip().lower() == name.lower():
            return column_name
    return None


label_column = find_column("Label")
protocol_column = find_column("Protocol")

if not label_column:
    raise ValueError(
        "Label column not found. Check your dataset schema."
    )

print("\nLabel Column:", label_column)
print("Protocol Column:", protocol_column)


# --------------------------------------------------
# CREATE SPARK SQL VIEW
# --------------------------------------------------

# Escape column names containing spaces or special characters.
safe_label = col(
    f"`{label_column.replace('`', '``')}`"
)

df.createOrReplaceTempView("cybersecurity_logs")

df.createOrReplaceTempView("security_events")


# --------------------------------------------------
# 1. ATTACK CATEGORY DISTRIBUTION
# --------------------------------------------------

print("\n===== ATTACK CATEGORY DISTRIBUTION =====")

attack_distribution = spark.sql("""
    SELECT
        TRIM(CAST(`""" + label_column.replace("`", "``") + """` AS STRING))
        AS attack_type,
        COUNT(*) AS total_events
    FROM cybersecurity_logs
    GROUP BY TRIM(CAST(`""" + label_column.replace("`", "``") + """` AS STRING))
    ORDER BY total_events DESC
""")

attack_distribution.show(50, truncate=False)

attack_distribution.coalesce(1).write.mode(
    "overwrite"
).option("header", True).csv(
    str(REPORT_DIR / "attack_distribution")
)


# --------------------------------------------------
# 2. BENIGN VS MALICIOUS TRAFFIC
# --------------------------------------------------

print("\n===== BENIGN VS MALICIOUS TRAFFIC =====")

traffic_summary = spark.sql("""
    SELECT
        CASE
            WHEN UPPER(TRIM(CAST(`""" +
            label_column.replace("`", "``") + """` AS STRING)))
            = 'BENIGN'
            THEN 'NORMAL'
            ELSE 'MALICIOUS'
        END AS traffic_type,
        COUNT(*) AS total_events
    FROM cybersecurity_logs
    GROUP BY traffic_type
    ORDER BY total_events DESC
""")

traffic_summary.show()

traffic_summary.coalesce(1).write.mode(
    "overwrite"
).option("header", True).csv(
    str(REPORT_DIR / "traffic_summary")
)


# --------------------------------------------------
# 3. PROTOCOL-WISE ANALYSIS
# --------------------------------------------------

if protocol_column:

    print("\n===== PROTOCOL-WISE ANALYSIS =====")

    protocol_sql = (
        "SELECT `" + protocol_column.replace("`", "``") + """` AS protocol,
                COUNT(*) AS total_events,
                SUM(CASE WHEN UPPER(TRIM(CAST(`""" +
        label_column.replace("`", "``") + """` AS STRING)))
                != 'BENIGN' THEN 1 ELSE 0 END) AS malicious_events
        FROM cybersecurity_logs
        GROUP BY `""" + protocol_column.replace("`", "``") + """`
        ORDER BY total_events DESC
        """
    )

    protocol_analysis = spark.sql(protocol_sql)

    protocol_analysis.show(50, truncate=False)

    protocol_analysis.coalesce(1).write.mode(
        "overwrite"
    ).option("header", True).csv(
        str(REPORT_DIR / "protocol_analysis")
    )

else:
    print("Protocol column not available. Skipping protocol analysis.")


# --------------------------------------------------
# 4. TOP ATTACK CATEGORIES
# --------------------------------------------------

print("\n===== TOP 10 ATTACK CATEGORIES =====")

top_attacks = spark.sql("""
    SELECT
        TRIM(CAST(`""" + label_column.replace("`", "``") + """` AS STRING))
        AS attack_type,
        COUNT(*) AS attack_count
    FROM cybersecurity_logs
    WHERE UPPER(TRIM(CAST(`""" +
    label_column.replace("`", "``") + """` AS STRING)))
    != 'BENIGN'
    GROUP BY TRIM(CAST(`""" +
    label_column.replace("`", "``") + """` AS STRING))
    ORDER BY attack_count DESC
    LIMIT 10
""")

top_attacks.show(truncate=False)

top_attacks.coalesce(1).write.mode(
    "overwrite"
).option("header", True).csv(
    str(REPORT_DIR / "top_attacks")
)


# --------------------------------------------------
# 5. NETWORK TRAFFIC STATISTICS
# --------------------------------------------------

print("\n===== NETWORK TRAFFIC STATISTICS =====")

numeric_columns = [
    field.name
    for field in df.schema.fields
    if field.dataType.typeName() in ["integer", "long", "float", "double"]
]

if numeric_columns:

    selected_columns = [
        col(f"`{name.replace('`', '``')}`")
        for name in numeric_columns[:10]
    ]

    statistics = df.select(*selected_columns).describe()

    statistics.show(truncate=False)

    statistics.coalesce(1).write.mode(
        "overwrite"
    ).option("header", True).csv(
        str(REPORT_DIR / "traffic_statistics")
    )


# --------------------------------------------------
# FINAL OUTPUT
# --------------------------------------------------

print("\n========================================")
print("SPARK SQL ANALYTICS COMPLETED")
print("========================================")

print("Reports saved at:", REPORT_DIR)

spark.stop()
