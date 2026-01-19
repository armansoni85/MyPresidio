import pandas as pd
import sqlite3
import os
import re
from presidio_analyzer import Pattern, PatternRecognizer
from typing import List

base_path = '/usr/share/data'  # Update this path to the actual location of the database file

def load_icd9_data() -> List[Pattern]:
    """Load ICD-9 data from the database and create regex patterns."""
    patterns = []
    conn = None
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(os.path.join(base_path, 'icd9.db'))
        cursor = conn.cursor()

        # Query ICD-9 data from the database
        cursor.execute("SELECT code, long_desc FROM icd9_data")
        rows = cursor.fetchall()

        # Generate patterns for each ICD-9 code
        patterns = [
            Pattern(
                name=f"ICD-9 Code: {row[0]}",
                regex=rf"\b{re.escape(str(row[0]))}\b",  # Ensure the code is treated as a literal in regex
                score=0.9
            )
            for row in rows
        ]
    except Exception as e:
        print(f"Error loading ICD-9 data: {e}")
    finally:
        if conn:
            conn.close()
    return patterns

# Custom ICD-9 Recognizer
class ICD9Recognizer(PatternRecognizer):
    """Custom recognizer for ICD-9 codes and descriptions."""

    def __init__(self, supported_language: str = "en"):
        # Load ICD-9 patterns
        patterns = load_icd9_data()

        # Define the context words that may increase confidence
        context = [
            "icd-9", "code", "diagnosis", "disease", "condition", "medical"
        ]

        # Initialize the recognizer with patterns and context
        super().__init__(
            supported_entity="ICD9_CODE",
            patterns=patterns,
            context=context,
            supported_language=supported_language
        )

