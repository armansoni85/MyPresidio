import re
from functools import reduce
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional

class GermanVATRecognizer(PatternRecognizer):
    """
    Recognizer to detect and validate German VAT numbers (USt-IdNr).
    """

    PATTERNS = [
        Pattern(
            name="German VAT (DE + 9 digits)",
            regex=r"\bDE[0-9]{9}\b",
            score=0.8,  # Slightly lower base
        ),
    ]

    CONTEXT = [
        "vat", "vat number", "vat no", "mwst", "mehrwertsteuer",
        "mehrwertsteuer identifikationsnummer", "ust-id", "ustid",
    ]

    MIN_SCORE = 0.75  # Allow slightly lower threshold

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "DE_Germany_VAT_Number",
    ):
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns or self.PATTERNS,
            context=context or self.CONTEXT,
            supported_language=supported_language,
        )

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        results = super().analyze(text, entities, nlp_artifacts)
        filtered: List[RecognizerResult] = []

        for res in results:
            candidate = text[res.start : res.end]
            has_ctx = self._has_context(text, res.start, res.end)
            valid_ck = self._validate_checksum(candidate)

            # Adjust score intelligently
            if valid_ck and has_ctx:
                res.score = 1.0
            elif valid_ck and not has_ctx:
                res.score = 0.9  # Allow valid checksum alone to pass
            elif not valid_ck and has_ctx:
                res.score = 0.6
            else:
                res.score = 0.4

            if res.score >= self.MIN_SCORE:
                filtered.append(res)

        return filtered

    def _has_context(self, text: str, start: int, end: int) -> bool:
        window = text[max(0, start - 200) : end + 200].lower()
        return any(ctx in window for ctx in self.CONTEXT)

    def _validate_checksum(self, vat: str) -> bool:
        """
        Correct German VAT checksum (USt-IdNr) validation.
        """
        match = re.fullmatch(r"DE(\d{9})", vat)
        if not match:
            return False

        digits = list(map(int, match.group(1)))
        # Running product starting at 10
        product = 10
        for d in digits[:-1]:  # first 8 digits
            s = (d + product) % 10
            if s == 0:
                s = 10
            product = (2 * s) % 11

        check = 11 - product
        # Map special cases to 0, not to invalid
        if check == 10:
            check = 0
        elif check == 11:
            check = 0

        return check == digits[-1]

