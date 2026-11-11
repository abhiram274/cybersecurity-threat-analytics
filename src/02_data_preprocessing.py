import os
from pathlib import Path
import pandas as pd
import numpy as np

BASE_DIR = Path(__file__).resolve().parent.parent

# Check local project data directory first, fallback to user Downloads directory
INPUT_DIR = BASE_DIR / "data" / "MachineLearningCVE"
if not INPUT_DIR.exists():
    fallback = Path.home() / "Downloads" / "MachineLearningCSV" / "MachineLearningCVE"
    if fallback.exists():
        INPUT_DIR = fallback

if not INPUT_DIR.exists():
    print(f"Dataset directory not found at {INPUT_DIR} or {Path.home() / 'Downloads' / 'MachineLearningCSV' / 'MachineLearningCVE'}")
    print("Please unzip 'MachineLearningCSV.zip' into 'data/MachineLearningCVE/' or your Downloads folder.")
    exit(1)

OUTPUT_DIR = BASE_DIR / "results" / "cleaned_data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

files = sorted(f for f in os.listdir(INPUT_DIR) if f.endswith(".csv"))

print("=" * 60)
print("CYBERSECURITY DATA PREPROCESSING")
print("=" * 60)

for file in files:

    input_path = os.path.join(INPUT_DIR, file)
    output_path = os.path.join(OUTPUT_DIR, file)

    print(f"\nProcessing: {file}")

    total_rows = 0

    if os.path.exists(output_path):
        os.remove(output_path)

    for chunk in pd.read_csv(input_path, chunksize=100000, low_memory=False):

        # Remove unnecessary spaces from column names
        chunk.columns = chunk.columns.str.strip()

        # Remove duplicate column names, if any
        chunk = chunk.loc[:, ~chunk.columns.duplicated()]

        # Identify target column
        label_col = next(
            (col for col in chunk.columns if col.lower() == "label"),
            None
        )

        if label_col is None:
            raise ValueError(f"Label column missing in {file}")

        # Clean attack labels
        chunk[label_col] = chunk[label_col].astype(str).str.strip()

        # Convert infinite values into missing values
        chunk.replace([np.inf, -np.inf], np.nan, inplace=True)

        # Convert feature columns into numeric values
        feature_cols = [
            col for col in chunk.columns if col != label_col
        ]

        for col in feature_cols:
            chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

        # Replace missing numerical values with zero
        chunk[feature_cols] = chunk[feature_cols].fillna(0)

        # Remove records without valid labels
        chunk = chunk[
            chunk[label_col].notna() &
            (chunk[label_col] != "nan") &
            (chunk[label_col] != "")
        ]

        # Save cleaned data
        chunk.to_csv(
            output_path,
            mode="a",
            header=not os.path.exists(output_path) or os.path.getsize(output_path) == 0,
            index=False
        )

        total_rows += len(chunk)

    print("Cleaned records:", total_rows)
    print("Saved:", output_path)

print("\nAll datasets processed successfully!")
