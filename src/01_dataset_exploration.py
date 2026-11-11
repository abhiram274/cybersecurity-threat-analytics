import pandas as pd
import os

DATASET_PATH = os.path.expanduser(
    "~/Downloads/MachineLearningCSV/MachineLearningCVE"
)

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
