from pathlib import Path
import json

import joblib
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.types import NumericType

from sklearn.ensemble import IsolationForest
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
    classification_report
)


# ==========================================
# 1. PROJECT CONFIGURATION
# ==========================================

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_PATH = BASE_DIR / "results" / "parquet"
MODEL_PATH = BASE_DIR / "models" / "isolation_forest.joblib"
RESULT_PATH = BASE_DIR / "results" / "anomalies"

RESULT_PATH.mkdir(parents=True, exist_ok=True)

SAMPLE_LIMIT = 150000


# ==========================================
# 2. INITIALIZE SPARK
# ==========================================

spark = (
    SparkSession.builder
    .appName("CybersecurityAnomalyDetection")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n===== ISOLATION FOREST ANOMALY DETECTION =====")


try:

    # ==========================================
    # 3. LOAD PARQUET DATA
    # ==========================================

    df = spark.read.parquet(str(DATA_PATH))

    print("\nTotal Dataset Records:", f"{df.count():,}")

    # Identify label column
    label_column = next(
        (c for c in df.columns if c.strip().lower() == "label"),
        None
    )

    if label_column is None:
        raise ValueError("Label column not found!")

    # Identify numeric features
    numeric_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, NumericType)
        and field.name != label_column
    ]

    if not numeric_columns:
        raise ValueError("No numeric features found!")

    print("Numeric Features:", len(numeric_columns))


    # ==========================================
    # 4. EXTRACT BOUNDED SAMPLE
    # ==========================================

    print("\n===== EXTRACTING DATA SAMPLE =====")

    selected_columns = [
        f"`{c.replace('`', '``')}`"
        for c in numeric_columns
    ]

    selected_columns.append(
        f"`{label_column.replace('`', '``')}`"
    )

    sample_df = (
        df.selectExpr(*selected_columns)
        .sample(False, 0.20, seed=42)
        .limit(SAMPLE_LIMIT)
    )

    pdf = sample_df.toPandas()

    print("Sample Records:", f"{len(pdf):,}")


    # ==========================================
    # 5. DATA CLEANING
    # ==========================================

    print("\n===== DATA CLEANING =====")

    pdf[label_column] = (
        pdf[label_column]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    pdf = pdf.replace(
        [np.inf, -np.inf],
        np.nan
    )

    pdf = pdf.dropna(subset=[label_column])

    # Remove entirely empty feature columns
    features = [
        c for c in numeric_columns
        if pdf[c].notna().any()
    ]

    if not features:
        raise ValueError("No usable numeric features!")

    X = pdf[features].copy()

    # Convert numeric columns safely
    for feature in features:
        X[feature] = pd.to_numeric(
            X[feature], errors="coerce"
        )

    # Remove constant features
    features = [
        c for c in features
        if X[c].nunique(dropna=True) > 1
    ]

    X = X[features]

    # Identify normal traffic
    benign_mask = pdf[label_column] == "BENIGN"

    X_benign = X.loc[benign_mask]

    if len(X_benign) < 100:
        raise ValueError(
            "Insufficient BENIGN records for training."
        )

    print("Benign Records:", f"{len(X_benign):,}")
    print("Features Selected:", len(features))


    # ==========================================
    # 6. HANDLE MISSING VALUES
    # ==========================================

    imputer = SimpleImputer(strategy="median")

    X_benign_clean = imputer.fit_transform(X_benign)

    X_all_clean = imputer.transform(X)


    # ==========================================
    # 7. TRAIN ISOLATION FOREST
    # ==========================================

    print("\n===== TRAINING ISOLATION FOREST =====")

    isolation_forest = IsolationForest(
        n_estimators=150,
        max_samples="auto",
        contamination=0.02,
        random_state=42,
        n_jobs=2
    )

    isolation_forest.fit(X_benign_clean)

    print("Training Completed!")


    # ==========================================
    # 8. DETECT ANOMALIES
    # ==========================================

    print("\n===== DETECTING ANOMALIES =====")

    predictions = isolation_forest.predict(
        X_all_clean
    )

    anomaly_scores = isolation_forest.decision_function(
        X_all_clean
    )

    # Isolation Forest:
    # 1 = Normal
    # -1 = Anomaly

    pdf["anomaly_prediction"] = predictions

    pdf["anomaly_status"] = np.where(
        predictions == -1,
        "ANOMALY",
        "NORMAL"
    )

    pdf["anomaly_score"] = anomaly_scores

    print("\n===== ANOMALY DISTRIBUTION =====")

    print(
        pdf["anomaly_status"].value_counts()
    )


    # ==========================================
    # 9. EVALUATION AGAINST DATASET LABELS
    # ==========================================

    print("\n===== ANOMALY DETECTION EVALUATION =====")

    actual = (
        pdf[label_column] != "BENIGN"
    ).astype(int)

    predicted = (
        predictions == -1
    ).astype(int)

    accuracy = accuracy_score(actual, predicted)

    precision = precision_score(
        actual, predicted, zero_division=0
    )

    recall = recall_score(
        actual, predicted, zero_division=0
    )

    f1 = f1_score(
        actual, predicted, zero_division=0
    )

    print(f"Accuracy  : {accuracy:.4f}")
    print(f"Precision : {precision:.4f}")
    print(f"Recall    : {recall:.4f}")
    print(f"F1 Score  : {f1:.4f}")

    print("\nConfusion Matrix:")
    print(confusion_matrix(actual, predicted))

    print("\nClassification Report:")

    print(
        classification_report(
            actual,
            predicted,
            target_names=["BENIGN", "MALICIOUS"],
            zero_division=0
        )
    )


    #==========================================
    # 10. SAVE ANOMALY RESULTS
    # ==========================================

    print("\n===== SAVING ANOMALY RESULTS =====")

    #Save only detected anomalies instead of the entire dataset
    anomaly_records = pdf[
    pdf["anomaly_status"] == "ANOMALY"
    ].copy()

    # Keep only useful columns
    output_columns = (
     [label_column, "anomaly_status", "anomaly_score"]
     + features[:10]
    )

    anomaly_records = anomaly_records[output_columns]

    # Save compressed CSV
    anomaly_records.to_csv(
       RESULT_PATH / "detected_anomalies.csv.gz",
       index=False,
       compression="gzip"
    )

    print(
      "Anomalies Saved:",
      f"{len(anomaly_records):,}"
    )

    print("Compressed anomaly report saved!")

    # ==========================================
    # 11. SAVE MODEL
    # ==========================================

    artifact = {
        "model": isolation_forest,
        "imputer": imputer,
        "features": features,
        "threshold": "contamination=0.02"
    }

    joblib.dump(
        artifact,
        MODEL_PATH
    )

    # ==========================================
    # 12. SAVE METRICS
    # ==========================================

    metrics = {
        "algorithm": "Isolation Forest",
        "sample_records": int(len(pdf)),
        "benign_training_records": int(len(X_benign)),
        "features": len(features),
        "anomalies_detected": int((predictions == -1).sum()),
        "accuracy": float(accuracy),
        "precision": float(precision),
        "recall": float(recall),
        "f1_score": float(f1)
    }

    with open(
        RESULT_PATH / "anomaly_metrics.json",
        "w"
    ) as file:

        json.dump(metrics, file, indent=4)

    print("\n===== ANOMALY DETECTION COMPLETED =====")

    print("Model:", MODEL_PATH)

    print("Results:", RESULT_PATH)


finally:
    spark.stop()
