import pandas as pd
import time

file_path = '/usr/share/data/ValidICD10-Jan2024.parquet'

def load_icd10_parquet_optimized():
    """Load specific columns from the Parquet file."""
    start_time = time.time()
    # Specify only the required columns
    icd10_df = pd.read_parquet(file_path, columns=['CODE', 'SHORT DESCRIPTION (VALID ICD-10 FY2024)', 'LONG DESCRIPTION (VALID ICD-10 FY2024)'])
    end_time = time.time()
    print(f"Time taken to read Parquet (optimized): {end_time - start_time:.4f} seconds")
    return icd10_df

# Call the function
icd10_data = load_icd10_parquet_optimized()
