import json
import pandas as pd
import re
import os
from presidio_analyzer import Pattern, PatternRecognizer
from typing import List, Optional
from concurrent.futures import ProcessPoolExecutor

base_path = '/usr/share/data'

def load_data_lazy(filename, loader_function):
    """Load data lazily, only when needed."""
    if not hasattr(load_data_lazy, "cache"):
        load_data_lazy.cache = {}
    if filename not in load_data_lazy.cache:
        load_data_lazy.cache[filename] = loader_function()
    return load_data_lazy.cache[filename]

# Data loader functions
def load_drugs():
    with open(os.path.join(base_path, 'Drugs.json')) as f:
        return set(json.load(f))

def load_ingredients():
    with open(os.path.join(base_path, 'Ingredients.json')) as f:
        return set(json.load(f))

def load_icd9():
    try:
        icd9_df = pd.read_csv(os.path.join(base_path, 'ValidICD9-Jan2024.csv'))  # Use CSV format for faster reading
        patterns = [Pattern(f"ICD-9 Code: {row['CODE']}", rf"\b{row['CODE']}\b", 0.9) for _, row in icd9_df.iterrows()]
        descriptions = {row['CODE']: row['LONG DESCRIPTION (VALID ICD-9 FY2024)'] for _, row in icd9_df.iterrows()}
    except Exception as e:
        print(f"Error loading ICD-9 data: {e}")
        patterns, descriptions = [], {}
    return patterns, descriptions

def load_icd10():
    try:
        icd10_df = pd.read_csv(os.path.join(base_path, 'ValidICD10-Jan2024.csv'))  # Use CSV format for faster reading
        codes = set(icd10_df['CODE'].str.upper())
        short_descriptions = set(icd10_df['SHORT DESCRIPTION (VALID ICD-10 FY2024)'].dropna().str.upper())
        long_descriptions = set(icd10_df['LONG DESCRIPTION (VALID ICD-10 FY2024)'].dropna().str.upper())
        
        patterns = [
            Pattern(f"ICD-10 Code: {code}", re.escape(code), 1.0) for code in codes
        ] + [
            Pattern(f"ICD-10 Short Desc: {desc}", re.escape(desc), 0.7) for desc in short_descriptions
        ] + [
            Pattern(f"ICD-10 Long Desc: {desc}", re.escape(desc), 0.7) for desc in long_descriptions
        ]
    except Exception as e:
        print(f"Error loading ICD-10 data: {e}")
        patterns, codes, short_descriptions, long_descriptions = [], set(), set(), set()
    return patterns, codes, short_descriptions, long_descriptions

# Use ProcessPoolExecutor for parallel processing
def load_data():
    with ProcessPoolExecutor() as executor:
        future_drugs = executor.submit(load_drugs)
        future_ingredients = executor.submit(load_ingredients)
        future_icd9 = executor.submit(load_icd9)
        future_icd10 = executor.submit(load_icd10)

        drugs = future_drugs.result()
        ingredients = future_ingredients.result()
        icd9_patterns, icd9_descriptions = future_icd9.result()
        icd10_patterns, icd10_codes, short_descriptions, long_descriptions = future_icd10.result()
    
    return drugs, ingredients, icd9_patterns, icd9_descriptions, icd10_patterns, icd10_codes, short_descriptions, long_descriptions

# Lazy-load data
drugs = load_data_lazy('Drugs.json', load_drugs)
ingredients = load_data_lazy('Ingredients.json', load_ingredients)
icd9_patterns, icd9_descriptions = load_data_lazy('ValidICD9-Jan2024.csv', load_icd9)
icd10_patterns, icd10_codes, short_descriptions, long_descriptions = load_data_lazy('ValidICD10-Jan2024.csv', load_icd10)

class UnifiedRecognizer(PatternRecognizer):
    """Recognize drugs, ingredients, ICD-9, and ICD-10 codes and descriptions using patterns."""

    def __init__(
        self,
        supported_language: str = "en",
        supported_entity: str = "UNIFIED_ENTITY",
    ):
        patterns = (
            [Pattern(f"Drug Name: {drug}", rf"\b{drug}\b", 0.9) for drug in drugs] +
            [Pattern(f"Ingredient: {ingredient}", rf"\b{ingredient}\b", 0.9) for ingredient in ingredients] +
            icd9_patterns +
            icd10_patterns
        )
        context = [
            "drug", "medication", "prescription", "treatment",
            "ingredient", "compound", "substance", "component",
            "icd", "code", "diagnosis", "disease", "condition",
        ]
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

# if __name__ == "__main__":
#     recognizer = UnifiedRecognizer()
#     text = "Sample text with codes and descriptions..."
#     results = recognizer.analyze(text, language="en")

#     for result in results:
#         print(result)
