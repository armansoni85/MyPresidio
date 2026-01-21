import re
import json
import os
from typing import List
from presidio_analyzer import LocalRecognizer, RecognizerResult
from ahocorasick import Automaton


class HipaaRegRecognizer(LocalRecognizer):
    MEDICAL_CONTEXT = {
        "patient", "diagnosis", "diagnosed", "dx",
        "treatment", "treated", "procedure",
        "hospital", "clinic", "doctor", "physician",
        "medication", "prescribed", "symptom"
    }

    FINANCIAL_CONTEXT = {
        "bank", "routing", "account", "ach",
        "wire", "payment", "deposit", "transfer"
    }

    SSN_CONTEXT = {
        "ssn", "social security", "social-security"
    }

    #  Patient ID additions
    PATIENT_ID_CONTEXT = {
        "patient identification number",
        "patient id",
        "patient number",
        "patient #",
        "pin"
    }

    PATIENT_ID_REGEX = re.compile(r"""(?:^|[=:\[\{\-\s])(\d{4,})(?=$|[.\]\},\s])""")

    DASHED_SSN_REGEX = re.compile(r"""(?:^|[=:\[\{\-\s])(\d{3})-(\d{2})-(\d{4})(?=$|[.\]\},\s])""")
    SPACED_SSN_REGEX = re.compile(r"""(?:^|[=:\[\{\-\s])(\d{3})\s(\d{2})\s(\d{4})(?=$|[.\]\},\s])""")

    WORD_CHAR_REGEX = re.compile(r"[a-z0-9]", re.IGNORECASE)

    def __init__(self, supported_language="en", supported_entity="HIPAAREG"):
        super().__init__(
            supported_entities=[supported_entity],
            supported_language=supported_language,
        )
        self.automaton = Automaton()
        self.load_data()

    # --------------------------------------------------
    def load_data(self):
        base_path = "/usr/share/data"

        def add(word, category):
            self.automaton.add_word(word, (word, category))

        for fname, cat in [
            ("ValidICD9-Jan2024.json", "ICD9"),
            ("ValidICD10-Jan2024.json", "ICD10"),
        ]:
            try:
                with open(os.path.join(base_path, fname)) as f:
                    for item in json.load(f):
                        code = item.get("CODE")
                        if code:
                            add(code.lower(), cat)
            except Exception as e:
                print(f"{cat} load error:", e)

        for fname, cat in [
            ("Drugs.json", "DRUG"),
            ("Ingredients.json", "INGREDIENT"),
        ]:
            try:
                with open(os.path.join(base_path, fname)) as f:
                    for v in json.load(f):
                        add(v.lower(), cat)
            except Exception as e:
                print(f"{cat} load error:", e)

        self.automaton.make_automaton()

    # --------------------------------------------------
    def _has_context(self, text, start, end, words, window=50):
        left = max(0, start - window)
        right = min(len(text), end + window)
        chunk = text[left:right]
        return any(w in chunk for w in words)

    def _digit_isolated(self, text, start, end):
        if start > 0 and text[start - 1].isdigit():
            return False
        if end < len(text) and text[end].isdigit():
            return False
        return True

    def _is_valid_ssn(self, area: str, group: str, serial: str) -> bool:
        area_i = int(area)
        group_i = int(group)
        serial_i = int(serial)

        if area_i == 0 or area_i == 666 or area_i >= 900:
            return False
        if group_i == 0:
            return False
        if serial_i == 0:
            return False

        return True

    def _is_standalone_token(self, text, start, end):
        if start > 0 and text[start - 1].isalnum():
            return False
        if end < len(text) and text[end].isalnum():
            return False
        return True

    def _is_exact_token(self, text, start, end, val):
        token_start = start
        token_end = end

        while token_start > 0 and text[token_start - 1].isalnum():
            token_start -= 1
        while token_end < len(text) and text[token_end].isalnum():
            token_end += 1

        return text[token_start:token_end] == val

    def _overlaps_any(self, start, end, spans):
        for s, e in spans:
            if start < e and end > s:
                return True
        return False


    # --------------------------------------------------
    def analyze(self, text: str, entities: List[str], nlp_artifacts=None):
        if "HIPAAREG" not in entities:
            return []

        text_l = text.lower()
        ssn_spans = []
        # Hard financial block
        if self._has_context(text_l, 0, len(text_l), self.FINANCIAL_CONTEXT, 100):
            return []

        condition_1 = []
        condition_2_ssn = []
        condition_2_patient_id = []

        # --------------------------------------------------
        # Condition 1 — Healthcare Dictionaries (exact word only)
        for end, (val, cat) in self.automaton.iter(text_l):
            start = end - len(val) + 1
            end += 1

            if self._overlaps_any(start, end, ssn_spans):
                continue

            matched_text = text_l[start:end]
            if matched_text != val:
                continue

            #if not self._is_standalone_token(text_l, start, end):
            if not self._is_exact_token(text_l, start, end, val):
                continue

            if val.isdigit():
                continue

            if cat in ("ICD9", "ICD10") and val.isdigit():
                if not self._digit_isolated(text_l, start, end):
                    continue
                if not self._has_context(text_l, start, end, self.MEDICAL_CONTEXT):
                    continue

            condition_1.append(
                RecognizerResult("HIPAAREG", start, end, 0.85)
            )

        # --------------------------------------------------
        # Condition 2A — SSN
        for regex in (self.DASHED_SSN_REGEX, self.SPACED_SSN_REGEX):
            for m in regex.finditer(text):
                area, group, serial = m.groups()

                if not self._is_valid_ssn(area, group, serial):
                    continue

                if not self._has_context(text_l, m.start(), m.end(), self.SSN_CONTEXT):
                    continue
                ssn_spans.append((m.start(), m.end()))

                condition_2_ssn.append(
                    RecognizerResult("HIPAAREG", m.start(), m.end(), 0.95)
                )

        # --------------------------------------------------
        # Condition 2B — Patient ID (4+ digits + context)
        for m in self.PATIENT_ID_REGEX.finditer(text_l):
            start, end = m.start(), m.end()

            if not self._has_context(text_l, start, end, self.PATIENT_ID_CONTEXT):
                continue

            condition_2_patient_id.append(
                RecognizerResult("HIPAAREG", start, end, 0.90)
            )

        # --------------------------------------------------
        # FINAL DECISION — POLICY GATE

        # Must have healthcare dictionary
        if not condition_1:
            return []

        # Must have SSN OR Patient ID
        if not (condition_2_ssn or condition_2_patient_id):
            return []

        return self._filter_overlaps(
            condition_1 + condition_2_ssn + condition_2_patient_id
        )

    # --------------------------------------------------
    def _filter_overlaps(self, results):
        results.sort(key=lambda r: (r.start, -r.score))
        out = []
        last_end = -1
        for r in results:
            if r.start >= last_end:
                out.append(r)
                last_end = r.end
        return out
