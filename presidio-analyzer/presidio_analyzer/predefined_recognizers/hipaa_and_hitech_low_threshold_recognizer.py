import os
import re
import json
import logging
from typing import List
from presidio_analyzer import RecognizerResult, LocalRecognizer
from ahocorasick import Automaton

# Configure logger
logger = logging.getLogger("LowThresholdHIPAARecognizer")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(levelname)s] %(message)s")
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)


class LowThresholdHIPAARecognizer(LocalRecognizer):
    """
    Self-contained recognizer for HIPAA Low Threshold policy.
    """

    def __init__(self, supported_language="en"):
        super().__init__(
            supported_entities=["HIPAA_HITECH_LOW_THRESHOLD"],
            supported_language=supported_language,
        )

        # --- 1️⃣ Numeric & SSN Patterns ---
        # Accepts SSNs in format XXX-XX-XXXX, XXX XX XXXX, or XXXXXXXXX
        # But excludes test/demo patterns (000, 666, 9xx, etc.)
        self.NUMBER_PATTERN = re.compile(r"\b\d{6,}\b")

        self.SSN_PATTERN = re.compile(
            r"\b(?!000|666|9\d{2})\d{3}[-\s]?(?!00)\d{2}[-\s]?(?!0000)\d{4}\b"
        )

        # Common invalid/test SSNs to exclude explicitly
        self.INVALID_SSNS = {
            "000-00-0000", "000000000", "123-45-6789", "111-11-1111",
            "222-22-2222", "333-33-3333", "444-44-4444", "555-55-5555",
            "666-66-6666", "777-77-7777", "888-88-8888", "999-99-9999"
        }

        # --- 2️⃣ Date Patterns ---
        self.DATE_PATTERN = re.compile(
            r"\b(?:DOB|DOS|Date(?: of Birth| of Service)?|Date)\b[:\s]*"
            r"("
            r"(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4})|"
            r"(?:\d{4}[/-]\d{1,2}[/-]\d{1,2})|"
            r"(?:\d{1,2}\.\d{1,2}\.\d{2,4})|"
            r"(?:\d{1,2}\s(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s\d{2,4})|"
            r"(?:(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s\d{1,2},?\s\d{2,4})"
            r")",
            re.IGNORECASE,
        )

        # --- 3️⃣ Proper Name Pattern ---
        self.NAME_PATTERN = re.compile(r"\b[A-Z][a-z]+(?:[-'\s][A-Z][a-z]+)+\b")

        # --- 4️⃣ Medical Terms Dictionary ---
        self.automaton = Automaton()
        self._load_medical_terms()

    def _load_medical_terms(self):
        base_path = "/usr/share/data"

        def safe_load(filename):
            try:
                with open(os.path.join(base_path, filename), encoding="utf-8") as f:
                    data = json.load(f)
                    logger.debug(f"Loaded {len(data)} records from {filename}")
                    return data
            except Exception as e:
                logger.warning(f"Failed to load {filename}: {e}")
                return []

        try:
            for item in safe_load("ValidICD9-Jan2024.json"):
                desc = item.get("LONG DESCRIPTION (VALID ICD-9 FY2024)", "")
                if desc:
                    self.automaton.add_word(desc.lower(), desc)
            for item in safe_load("ValidICD10-Jan2024.json"):
                for key in [
                    "SHORT DESCRIPTION (VALID ICD-10 FY2024)",
                    "LONG DESCRIPTION (VALID ICD-10 FY2024)",
                ]:
                    if item.get(key):
                        self.automaton.add_word(item[key].lower(), item[key])
            for drug in safe_load("Drugs.json"):
                self.automaton.add_word(drug.lower(), drug)
            for ing in safe_load("Ingredients.json"):
                self.automaton.add_word(ing.lower(), ing)

            self.automaton.make_automaton()
            logger.info(" Medical term automaton built successfully")
        except Exception as e:
            logger.error(f"Failed to build medical automaton: {e}")

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        if "HIPAA_HITECH_LOW_THRESHOLD" not in entities:
            return []

        logger.debug(f"Analyzing text: {text}")
        text_lower = text.lower()

        # 1️⃣ Numeric / SSN
        condition1 = []

        # Numbers
        for m in self.NUMBER_PATTERN.finditer(text):
            val = text[m.start():m.end()]
            logger.debug(f" Found 6+ digit number: '{val}'")
            condition1.append(RecognizerResult("HIPAA_HITECH_LOW_THRESHOLD", m.start(), m.end(), 0.8))

        # SSNs
        for m in self.SSN_PATTERN.finditer(text):
            ssn = text[m.start():m.end()]
            normalized = re.sub(r"[\s-]", "", ssn)
            if ssn in self.INVALID_SSNS or normalized in {s.replace("-", "") for s in self.INVALID_SSNS}:
                logger.debug(f" Ignoring invalid/test SSN: '{ssn}'")
                continue
            logger.debug(f" Found valid SSN: '{ssn}'")
            condition1.append(RecognizerResult("HIPAA_HITECH_LOW_THRESHOLD", m.start(), m.end(), 0.95))

        # 2️⃣ Medical terms
        condition2 = []
        seen_offsets = set()
        for end, term in self.automaton.iter(text_lower):
            start = end - len(term) + 1
            if (start, end) not in seen_offsets:
                logger.debug(f" Found medical term: '{term}'")
                condition2.append(RecognizerResult("HIPAA_HITECH_LOW_THRESHOLD", start, end + 1, 1.0))
                seen_offsets.add((start, end))

        # 3️⃣ Proper names
        condition3 = []
        for m in self.NAME_PATTERN.finditer(text):
            name = text[m.start():m.end()]
            logger.debug(f" Found proper name: '{name}'")
            condition3.append(RecognizerResult("HIPAA_HITECH_LOW_THRESHOLD", m.start(), m.end(), 0.7))

        # 4️⃣ Dates
        condition4 = []
        for m in self.DATE_PATTERN.finditer(text):
            date_str = text[m.start(1):m.end(1)]
            logger.debug(f" Found date: '{date_str}'")
            condition4.append(RecognizerResult("HIPAA_HITECH_LOW_THRESHOLD", m.start(1), m.end(1), 0.85))

        # Summary
        logger.info(
            f"Matches → Numbers/SSNs: {len(condition1)}, "
            f"Medical Terms: {len(condition2)}, Names: {len(condition3)}, Dates: {len(condition4)}"
        )

        if condition1 and condition2 and condition3 and condition4:
            results = condition1 + condition2 + condition3 + condition4
            logger.info("All four conditions met — returning results")
            return self._filter_overlaps(results)

        logger.warning("One or more required conditions not met — no results returned")
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

