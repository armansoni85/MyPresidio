import re
import json
import os
from typing import List
from presidio_analyzer import LocalRecognizer, RecognizerResult
from ahocorasick import Automaton

class HipaaRegRecognizer(LocalRecognizer):
    def __init__(self, supported_language="en", supported_entity="HIPAAREG"):
        super().__init__(supported_entities=[supported_entity], supported_language=supported_language)

        self.icd9_codes = set()
        self.icd10_codes = set()
        self.icd10_descriptions = set()
        self.drugs = set()
        self.ingredients = set()

        # ✅ Corrected regex patterns (single backslashes)
        self.six_digit_pattern = re.compile(r"\b\d{6,}\b")
        self.unformatted_ssn_pattern = re.compile(r"\b\d{3}-?\d{2}-?\d{4}\b")

        self.automaton = Automaton()
        self.load_data()

    def load_data(self):
        base_path = "/usr/share/data"

        # ICD-9
        try:
            with open(os.path.join(base_path, "ValidICD9-Jan2024.json")) as f:
                icd9_data = json.load(f)
                for item in icd9_data:
                    if code := item.get("CODE"):
                        code_l = code.lower()
                        self.icd9_codes.add(code_l)
                        self.automaton.add_word(code_l, (code, "ICD9"))
        except Exception as e:
            print("ICD-9 load error:", e)

        # ICD-10
        try:
            with open(os.path.join(base_path, "ValidICD10-Jan2024.json")) as f:
                icd10_data = json.load(f)
                for item in icd10_data:
                    if code := item.get("CODE"):
                        code_l = code.lower()
                        self.icd10_codes.add(code_l)
                        self.automaton.add_word(code_l, (code, "ICD10"))
                    for desc_key in ["SHORT DESCRIPTION (VALID ICD-10 FY2024)", "LONG DESCRIPTION (VALID ICD-10 FY2024)"]:
                        if desc := item.get(desc_key):
                            desc_l = desc.lower()
                            self.icd10_descriptions.add(desc_l)
                            self.automaton.add_word(desc_l, (desc, "ICD10_DESC"))
        except Exception as e:
            print("ICD-10 load error:", e)

        # Drugs
        try:
            with open(os.path.join(base_path, "Drugs.json")) as f:
                self.drugs = set([d.lower() for d in json.load(f)])
                for drug in self.drugs:
                    self.automaton.add_word(drug, (drug, "DRUG"))
        except Exception as e:
            print("Drug load error:", e)

        # Ingredients
        try:
            with open(os.path.join(base_path, "Ingredients.json")) as f:
                self.ingredients = set([i.lower() for i in json.load(f)])
                for ing in self.ingredients:
                    self.automaton.add_word(ing, (ing, "INGREDIENT"))
        except Exception as e:
            print("Ingredient load error:", e)

        self.automaton.make_automaton()

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        if "HIPAAREG" not in entities:
            return []

        text_lower = text.lower()
        condition_1_matches = []
        condition_2_matches = []

        # Condition 1: ICD/Drug/Ingredient
        for end_idx, (matched_val, category) in self.automaton.iter(text_lower):
            start_idx = end_idx - len(matched_val) + 1
            condition_1_matches.append(
                RecognizerResult(
                    entity_type="HIPAAREG", start=start_idx, end=end_idx + 1, score=0.85
                )
            )

        # Condition 2: SSN or 6+ digit number
        for match in self.six_digit_pattern.finditer(text):
            condition_2_matches.append(
                RecognizerResult(
                    entity_type="HIPAAREG", start=match.start(), end=match.end(), score=0.8
                )
            )
        for match in self.unformatted_ssn_pattern.finditer(text):
            condition_2_matches.append(
                RecognizerResult(
                    entity_type="HIPAAREG", start=match.start(), end=match.end(), score=0.95
                )
            )

        if condition_1_matches and condition_2_matches:
            print("✅ HIPAAREG triggered")
            return self._filter_overlaps(condition_1_matches + condition_2_matches)

        return []

    def _filter_overlaps(self, results: List[RecognizerResult]) -> List[RecognizerResult]:
        results.sort(key=lambda x: (x.start, -x.score))
        filtered = []
        last_end = -1
        for res in results:
            if res.start >= last_end:
                filtered.append(res)
                last_end = res.end
        return filtered

