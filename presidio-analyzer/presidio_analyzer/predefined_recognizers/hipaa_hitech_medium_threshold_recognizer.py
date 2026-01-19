import os
import re
import json
from typing import List
from presidio_analyzer import LocalRecognizer, RecognizerResult
from ahocorasick import Automaton


class HIPAAHITECHMEDIUMRecognizer(LocalRecognizer):
    def __init__(self, supported_language="en", supported_entity="HIPAA_HITECH_MEDIUM_THRESHOLD"):
        super().__init__(supported_entities=[supported_entity], supported_language=supported_language)

        self.healthcare_terms = set()
        self.automaton = Automaton()

        self.patient_id_pattern = re.compile(
            r"\b(?:Patient Identification Number|Patient Id|Patient Number|Patient #|PIN)[:\s]*\d{4,}\b",
            re.IGNORECASE
        )
        self.ssn_pattern = re.compile(r"\b\d{3}-?\d{2}-?\d{4}\b")
        self.npi_pattern = re.compile(r"\b\d{10}\b")

        self._load_healthcare_dictionaries()

    def _load_healthcare_dictionaries(self):
        base_path = "/usr/share/data"

        for filename in ["ValidICD9-Jan2024.json", "ValidICD10-Jan2024.json"]:
            try:
                with open(os.path.join(base_path, filename)) as f:
                    data = json.load(f)
                    for item in data:
                        for key in item:
                            val = item.get(key, '').strip().lower()
                            if val:
                                self.healthcare_terms.add(val)
                                self.automaton.add_word(val, (val, "TERM"))
            except Exception as e:
                print(f"Error loading {filename}: {e}")

        for filename in ["Drugs.json", "Ingredients.json"]:
            try:
                with open(os.path.join(base_path, filename)) as f:
                    items = json.load(f)
                    for term in items:
                        term = term.strip().lower()
                        self.healthcare_terms.add(term)
                        self.automaton.add_word(term, (term, "TERM"))
            except Exception as e:
                print(f"Error loading {filename}: {e}")

        self.automaton.make_automaton()

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        if "HIPAA_HITECH_MEDIUM_THRESHOLD" not in entities:
            return []

        text_lower = text.lower()
        results = []

        # Condition A: Any of the 3 ID-based conditions
        a_results = []

        for match in self.patient_id_pattern.finditer(text):
            a_results.append(RecognizerResult("PATIENT_ID", match.start(), match.end(), 0.85))

        for match in self.ssn_pattern.finditer(text):
            a_results.append(RecognizerResult("US_SSN", match.start(), match.end(), 0.9))

        for match in self.npi_pattern.finditer(text):
            a_results.append(RecognizerResult("NPI", match.start(), match.end(), 0.9))

        # Condition B: Match from healthcare dictionary
        b_results = []
        for end_idx, (val, _) in self.automaton.iter(text_lower):
            start_idx = end_idx - len(val) + 1
            b_results.append(RecognizerResult("HEALTH_TERM", start_idx, end_idx + 1, 1.0))

        # Only return results if A and B both have matches
        if a_results and b_results:
            results.extend(a_results)
            results.extend(b_results)

        return self._filter_overlaps(results)

    def _filter_overlaps(self, results: List[RecognizerResult]) -> List[RecognizerResult]:
        results.sort(key=lambda x: (x.start, -x.score))
        filtered = []
        last_end = -1
        for res in results:
            if res.start >= last_end:
                filtered.append(res)
                last_end = res.end
        return filtered

