import pandas as pd
import os

# Define paths
base_path = '/usr/share/data'
csv_file_path = os.path.join(base_path, 'ValidICD10-Jan2024.csv')
parquet_file_path = os.path.join(base_path, 'ValidICD10-Jan2024.parquet')

# Convert CSV to Parquet
try:
    # Load the CSV file
    df = pd.read_csv(csv_file_path)
    
    # Save it as a Parquet file
    df.to_parquet(parquet_file_path, engine='pyarrow', index=False)
    print(f"Converted CSV to Parquet: {parquet_file_path}")
except Exception as e:
    print(f"Error converting CSV to Parquet: {e}")

