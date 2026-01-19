import re
import sqlite3
from presidio_analyzer import Pattern, PatternRecognizer
from typing import List

def load_icd10_data_from_db() -> List[Pattern]:
    """Load ICD-10 data from the database and create regex patterns."""
    patterns = []
    try:
        # Connect to the database
        conn = sqlite3.connect('/usr/share/data/icd10.db')
        cursor = conn.cursor()
        
        # Query ICD-10 data
        cursor.execute("SELECT code, short_desc, long_desc FROM icd10_data")
        rows = cursor.fetchall()
        
        for row in rows:
            code, short_desc, long_desc = row
            
            # Add patterns for each type of description
            patterns.append(Pattern(f"ICD-10 Code: {code}", rf"\b{code}\b", 1.0))
            patterns.append(Pattern(f"ICD-10 Short Desc: {short_desc}", re.escape(short_desc), 0.7))
            patterns.append(Pattern(f"ICD-10 Long Desc: {long_desc}", re.escape(long_desc), 0.7))
    except Exception as e:
        print(f"Error loading ICD-10 data from DB: {e}")
    finally:
        conn.close()
    
    return patterns

class ICD10Recognizer(PatternRecognizer):
    """Recognizer for identifying ICD-10 codes and descriptions."""
    
    def __init__(
        self,
        supported_language: str = "en",
        supported_entity: str = "ICD10_CODE",
    ):
        # Load patterns
        patterns = load_icd10_data_from_db()
        
        context = ["icd-10", "code", "diagnosis", "disease", "condition", "medical"]
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

