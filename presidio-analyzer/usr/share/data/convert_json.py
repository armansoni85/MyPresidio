import pandas as pd
import os
import json

# Define the file paths
base_path = '/usr/share/data'  # Update this to your actual directory path
excel_file = os.path.join(base_path, 'ValidICD10-Jan2024.xlsx')
json_file = os.path.join(base_path, 'ValidICD10-Jan2024.json')

def convert_excel_to_json(excel_file, json_file):
    """Convert the Excel file to a JSON file."""
    try:
        # Read the Excel file
        df = pd.read_excel(excel_file)
        
        # Fill missing values with empty strings (if any)
        df.fillna("", inplace=True)
        
        # Convert the DataFrame to a list of dictionaries
        data = df.to_dict(orient='records')
        
        # Write the JSON data to a file
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        
        print(f"Conversion complete. JSON file saved at: {json_file}")
    except Exception as e:
        print(f"Error during conversion: {e}")

# Call the function to convert the Excel file to JSON
convert_excel_to_json(excel_file, json_file)

