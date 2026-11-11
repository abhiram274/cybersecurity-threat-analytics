import os
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent

# Check local project data directory first, fallback to user Downloads directory
DATASET_PATH = BASE_DIR / "data" / "MachineLearningCVE"
if not DATASET_PATH.exists():
    fallback = Path.home() / "Downloads" / "MachineLearningCSV" / "MachineLearningCVE"
    if fallback.exists():
        DATASET_PATH = fallback

if not DATASET_PATH.exists():
    print(f"Dataset directory not found at {DATASET_PATH} or {Path.home() / 'Downloads' / 'MachineLearningCSV' / 'MachineLearningCVE'}")
    print("Please unzip 'MachineLearningCSV.zip' into 'data/MachineLearningCVE/' or your Downloads folder.")
    exit(1)

files = [f for f in os.listdir(DATASET_PATH) if f.endswith(".csv")]

print("=" * 60)
print("CYBERSECURITY DATASET EXPLORATION")
print("=" * 60)

total_files = 0

for file in sorted(files):

    path = os.path.join(DATASET_PATH, file)

    print("\nFile:", file)

    df = pd.read_csv(path, nrows=5)

    df.columns = df.columns.str.strip()

    print("Columns:", len(df.columns))
    print("Sample records:")
    print(df.head(2).to_string(index=False))

    total_files += 1

print("\nTotal CSV files:", total_files)
