import re
from typing import List
from presidio_analyzer import Pattern, PatternRecognizer
import logging
logging.getLogger().setLevel(logging.INFO)

class US_Formatted_SSN_Recognizer(PatternRecognizer):
    """
    US Social Security Number recognizer with dual scoring:
    - Valid SSN without context → score 0.7
    - Valid SSN with context → score 1.0
    """

    PATTERNS = [
        Pattern(
            name="formatted_ssn",
            regex = r"""(?<![!@#$%&*()_+|`\\\/;"'<>\?~A-Za-z0-9])(?:^|[=:\[\{\- ])\d{3}[- ]\d{2}[- ]\d{4}(?=$|[.\]\}, ])""",
            score=0.7,  # base score (no context)
        )
    ]

    CONTEXT_TERMS = [
        "ssn",
        "ssns",
        "ssn#",
        "ss#",
        "social security number",
        "social security numbers",
    ]

    CONTEXT_WINDOW = 80  # characters BEFORE the SSN

    def __init__(self, supported_language="en", supported_entity="US_Formatted_SSN"):
        super().__init__(
            supported_entity=supported_entity,
            patterns=self.PATTERNS,
            context=[],  # disable Presidio loose context
            supported_language=supported_language,
        )

        # Word-boundary-safe context regex
        self.context_regex = re.compile(
            r"\b(" + "|".join(re.escape(t) for t in self.CONTEXT_TERMS) + r")\b",
            re.IGNORECASE,
        )

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None):
        results = super().analyze(text, entities, nlp_artifacts)
        final_results = []

        for r in results:
            ssn_text = text[r.start:r.end]

            # Validate SSN
            if self._is_invalid_ssn(ssn_text):
                continue

            # Default: valid SSN, no context
            r.score = 0.7

            # Boost score if context exists
            if self._has_backward_context(text, r.start):
                r.score = 1.0

            final_results.append(r)

        return final_results

    def _has_backward_context(self, text: str, ssn_start: int) -> bool:
        """
        Check ONLY BEFORE the SSN for valid SSN context.
        Prevents substring false positives.
        """
        window_start = max(0, ssn_start - self.CONTEXT_WINDOW)
        context_window = text[window_start:ssn_start]
        return bool(self.context_regex.search(context_window))

    def _is_invalid_ssn(self, ssn_text: str) -> bool:
        """
        SSA validation rules.
        """
        digits = "".join(c for c in ssn_text if c.isdigit())

        if len(digits) != 9:
            return True

        # All digits same
        if len(set(digits)) == 1:
            return True

        # Invalid segments
        area = digits[:3]
        group = digits[3:5]
        serial = digits[5:]

        if area in {"000", "666"} or area.startswith("9"):
            return True
        if group == "00":
            return True
        if serial == "0000":
            return True

        # Known fake SSNs
        if digits in {"123456789", "078051120", "987654320"}:
            return True

        return False
