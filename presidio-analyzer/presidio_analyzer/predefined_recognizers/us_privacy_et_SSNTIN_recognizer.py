import re
from typing import List, Optional
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging

logging.getLogger().setLevel(logging.INFO)


class SSNAndTINRecognizer(PatternRecognizer):
    """
    Detect formatted US SSN / ITIN with STRICT context rules.

    Scoring:
    - Valid number without context → 0.5
    - Valid number with context → 1.0
    """

    SUPPORTED_ENTITY = "US_SSN_ITIN"

    PATTERNS = [
        # SSN formatted
        Pattern(
            name="SSN formatted",
            regex=r"""(?:^|[=:\[\{\-\s])\d{3}[- ]\d{2}[- ]\d{4}(?=$|[.\]\},\s])""",
            score=0.5,
        ),

        # ITIN formatted
        Pattern(
            name="ITIN formatted",
            regex=r"\b9\d{2}[- ](7\d|8[0-8])[- ]\d{4}\b",
            score=0.5,
        ),
    ]

    CONTEXT_TERMS = [
        "ssn",
        "ssns",
        "ssn#",
        "ss#",
        "itin",
        "itin#",
        "social security number",
        "social security numbers",
        "tax id",
        "taxpayer identification number",
    ]

    CONTEXT_WINDOW = 80  # characters BEFORE the number

    def __init__(
        self,
        supported_language: str = "en",
        supported_entity: str = "US_SSN_ITIN",
        patterns: Optional[List[Pattern]] = None,
    ):
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns or self.PATTERNS,
            context=[],  # 🚨 disable Presidio loose context
            supported_language=supported_language,
        )

        # STRICT word / phrase context regex (no substrings)
        self.context_regex = re.compile(
            r"(?<![a-z0-9])("
            + "|".join(re.escape(t) for t in self.CONTEXT_TERMS)
            + r")(?![a-z0-9])",
            re.IGNORECASE,
        )

    def analyze(
        self, text: str, entities: List[str], nlp_artifacts=None
    ) -> List[RecognizerResult]:

        results = super().analyze(text, entities, nlp_artifacts)
        final_results = []

        for r in results:
            value = text[r.start:r.end]

            # Validate SSN / ITIN
            if self._is_invalid_number(value):
                continue

            # Default score: valid number, no context
            r.score = 0.5

            # Boost only if STRICT backward context exists
            if self._has_backward_context(text, r.start):
                r.score = 1.0

            final_results.append(r)

        return final_results

    def _has_backward_context(self, text: str, start: int) -> bool:
        """
        Check ONLY BEFORE the detected number.
        No substrings, no partial-word matches.
        """
        window_start = max(0, start - self.CONTEXT_WINDOW)
        context_window = text[window_start:start]
        return bool(self.context_regex.search(context_window))

    def _is_invalid_number(self, value: str) -> bool:
        """
        Shared SSN / ITIN validation.
        """
        digits = re.sub(r"\D", "", value)

        if len(digits) != 9:
            return True

        # All digits same
        if len(set(digits)) == 1:
            return True

        # Known fake numbers
        if digits in {"123456789", "078051120", "987654320"}:
            return True

        # SSN structural rules
        area = digits[:3]
        group = digits[3:5]
        serial = digits[5:]

        if area in {"000", "666"}:
            return True
        if group == "00":
            return True
        if serial == "0000":
            return True

        return False
