from pathlib import Path
import re
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan
from pyspark.sql.types import NumericType

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# ==========================================
# 1. PROJECT CONFIGURATION
# ==========================================

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_PATH = BASE_DIR / "results" / "parquet"
MODEL_PATH = BASE_DIR / "models" / "random_forest"
RESULT_PATH = BASE_DIR / "results" / "ml"
MODELS_DIR = BASE_DIR / "models"

RESULT_PATH.mkdir(parents=True, exist_ok=True)
MODELS_DIR.mkdir(parents=True, exist_ok=True)


# ==========================================
# 2. INITIALIZE SPARK
# ==========================================

spark = (
    SparkSession.builder
    .appName("CybersecurityThreatClassification")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n===== CYBERSECURITY ML TRAINING =====")


# ==========================================
# 3. LOAD DATASET
# ==========================================

df = spark.read.parquet(str(DATA_PATH))

print("\nTotal Records:", f"{df.count():,}")
print("Total Columns:", len(df.columns))


# ==========================================
# 4. IDENTIFY LABEL COLUMN
# ==========================================

label_column = next(
    (c for c in df.columns if c.strip().lower() == "label"),
    None
)

if label_column is None:
    raise ValueError("Label column not found!")

print("\nTarget Column:", label_column)


# ==========================================
# 5. SELECT NUMERIC FEATURES
# ==========================================

numeric_columns = [
    field.name
    for field in df.schema.fields
    if isinstance(field.dataType, NumericType)
    and field.name != label_column
]

if not numeric_columns:
    raise ValueError("No numeric features found!")

print("\nTotal Numeric Features:", len(numeric_columns))


# ==========================================
# 6. CLEAN ML DATA
# ==========================================

selected_columns = numeric_columns + [label_column]

# Select and normalize column names
ml_df = df.select(*[
    col(f"`{c.replace('`', '``')}`").alias(
        re.sub(r"[^a-zA-Z0-9_]", "_", c)
    )
    for c in selected_columns
])

# Normalize feature names
feature_names = [
    re.sub(r"[^a-zA-Z0-9_]", "_", c)
    for c in numeric_columns
]

# Rename target
target_name = re.sub(
    r"[^a-zA-Z0-9_]", "_", label_column
)

ml_df = ml_df.withColumnRenamed(
    target_name, "label"
)

# Remove invalid target and feature values
valid_condition = col("label").isNotNull()

for feature in feature_names:

    valid_condition = (
        valid_condition
        & col(feature).isNotNull()
        & ~isnan(col(feature).cast("double"))
        & (col(feature).cast("double") < 1e308)
        & (col(feature).cast("double") > -1e308)
    )

ml_df = ml_df.filter(valid_condition)

# Remove duplicate records
ml_df = ml_df.dropDuplicates()

print("\nClean ML Records:", f"{ml_df.count():,}")

print("\n===== CLASS DISTRIBUTION =====")

ml_df.groupBy("label").count().orderBy(
    col("count").desc()
).show(50, truncate=False)


# ==========================================
# 7. ENCODE TARGET LABELS
# ==========================================

indexer = StringIndexer(
    inputCol="label",
    outputCol="label_index",
    handleInvalid="error"
)

# Fit the indexer ONCE
indexer_model = indexer.fit(ml_df)

# Transform dataset using fitted model
indexed_df = indexer_model.transform(ml_df)

print("\n===== LABEL ENCODING =====")

for index, label in enumerate(indexer_model.labels):
    print(f"{index}: {label}")


# ==========================================
# 8. FEATURE VECTORIZATION
# ==========================================

assembler = VectorAssembler(
    inputCols=feature_names,
    outputCol="features",
    handleInvalid="error"
)


# ==========================================
# 9. TRAIN TEST SPLIT
# ==========================================

train_df, test_df = indexed_df.randomSplit(
    [0.8, 0.2],
    seed=42
)

print("\nTraining Records:", f"{train_df.count():,}")
print("Testing Records:", f"{test_df.count():,}")


# ==========================================
# 10. RANDOM FOREST MODEL
# ==========================================

rf = RandomForestClassifier(
    labelCol="label_index",
    featuresCol="features",
    numTrees=30,
    maxDepth=10,
    maxBins=32,
    seed=42
)

pipeline = Pipeline(
    stages=[assembler, rf]
)

print("\n===== TRAINING RANDOM FOREST =====")

model = pipeline.fit(train_df)

print("\nTraining Completed!")


# ==========================================
# 11. MODEL PREDICTION
# ==========================================

predictions = model.transform(test_df)

print("\n===== SAMPLE PREDICTIONS =====")

predictions.select(
    "label",
    "label_index",
    "prediction",
    "probability"
).show(10, truncate=False)


# ==========================================
# 12. MODEL EVALUATION
# ==========================================

print("\n===== MODEL EVALUATION =====")

evaluator = MulticlassClassificationEvaluator(
    labelCol="label_index",
    predictionCol="prediction"
)

accuracy = evaluator.evaluate(
    predictions,
    {evaluator.metricName: "accuracy"}
)

precision = evaluator.evaluate(
    predictions,
    {evaluator.metricName: "weightedPrecision"}
)

recall = evaluator.evaluate(
    predictions,
    {evaluator.metricName: "weightedRecall"}
)

f1 = evaluator.evaluate(
    predictions,
    {evaluator.metricName: "f1"}
)

print(f"Accuracy  : {accuracy:.4f}")
print(f"Precision : {precision:.4f}")
print(f"Recall    : {recall:.4f}")
print(f"F1 Score  : {f1:.4f}")


# ==========================================
# 13. CONFUSION MATRIX
# ==========================================

print("\n===== CONFUSION MATRIX =====")

confusion_matrix = predictions.groupBy(
    "label_index",
    "prediction"
).count().orderBy(
    "label_index",
    "prediction"
)

confusion_matrix.show(100, truncate=False)

confusion_matrix.coalesce(1).write.mode(
    "overwrite"
).option("header", True).csv(
    str(RESULT_PATH / "confusion_matrix")
)


# ==========================================
# 14. FEATURE IMPORTANCE
# ==========================================

rf_model = model.stages[-1]

importance = rf_model.featureImportances.toArray()

importance_data = sorted(
    zip(feature_names, importance),
    key=lambda x: x[1],
    reverse=True
)

print("\n===== TOP 15 IMPORTANT FEATURES =====")

for name, score in importance_data[:15]:
    print(f"{name}: {score:.6f}")


# ==========================================
# 15. SAVE MODEL AND METRICS
# ==========================================

# Save trained Random Forest pipeline
model.write().overwrite().save(str(MODEL_PATH))

print("\nRandom Forest model saved successfully!")


# ------------------------------------------
# Save label mapping
# ------------------------------------------

label_mapping = {
    str(index): label
    for index, label in enumerate(indexer_model.labels)
}

with open(
    MODELS_DIR / "label_mapping.json",
    "w"
) as file:
    json.dump(label_mapping, file, indent=4)

print("Label mapping saved successfully!")


# ------------------------------------------
# Save feature names
# ------------------------------------------

with open(
    MODELS_DIR / "feature_names.json",
    "w"
) as file:
    json.dump(feature_names, file, indent=4)

print("Feature names saved successfully!")


# ------------------------------------------
# Save classification metrics
# ------------------------------------------

metrics = {
    "algorithm": "Random Forest",
    "training_records": train_df.count(),
    "testing_records": test_df.count(),
    "features": len(feature_names),
    "accuracy": accuracy,
    "precision": precision,
    "recall": recall,
    "f1_score": f1
}

with open(
    RESULT_PATH / "classification_metrics.json",
    "w"
) as file:
    json.dump(metrics, file, indent=4)

print("\n===== MODEL SAVED =====")

print("Model:", MODEL_PATH)
print("Label Mapping:", MODELS_DIR / "label_mapping.json")
print("Feature Names:", MODELS_DIR / "feature_names.json")
print("Metrics:", RESULT_PATH / "classification_metrics.json")


# ==========================================
# 16. COMPLETION
# ==========================================

print("\n===== THREAT CLASSIFICATION COMPLETED =====")

spark.stop()
