from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional
import re

class SSNAndTINRecognizer(PatternRecognizer):
    """
    Detect formatted US SSN or ITIN/TIN only.
    """

    PATTERNS = [
        # ITIN formatted (hyphen or space)
        Pattern(
            name="ITIN formatted",
            regex=r"\b9\d{2}[- ](7\d|8[0-8])[- ]\d{4}\b",
            score=0.5,
        ),

        # SSN formatted (not starting with 9)
        Pattern(
            name="SSN formatted",
            regex=r"""(?:^|[=:\[\{\-\s])\d{3}[- ]\d{2}[- ]\d{4}(?=$|[.\]\},\s])""",
            score=0.5,
        ),
    ]

    CONTEXT = [
        "social security",
        "social security number",
        "ssn",
        "ssn#",
        "itin",
        "itin#",
        "tax id",
        "tax id#",
        "taxpayer identification number",
        "taxpayer id",
        "security number",
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "US_SSN_ITIN",
    ):
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns or self.PATTERNS,
            context=context or self.CONTEXT,
            supported_language=supported_language,
        )

    def analyze(
        self, text: str, entities: List[str], nlp_artifacts=None
    ) -> List[RecognizerResult]:

        results = super().analyze(text, entities, nlp_artifacts)
        final_results = []

        for result in results:
            match_text = text[result.start:result.end]

            if self._invalidate(match_text):
                continue

            # Context-based scoring
            if self._has_context(text, result.start, result.end):
                result.score = 1.0
            else:
                result.score = 0.5

            final_results.append(result)

        return final_results

    def _has_context(self, text: str, start: int, end: int) -> bool:
        window = text[max(0, start - 50): min(len(text), end + 50)].lower()
        return any(ctx in window for ctx in self.CONTEXT)

    def _invalidate(self, value: str) -> bool:
        digits = re.sub(r"\D", "", value)

        # Must be exactly 9 digits
        if len(digits) != 9:
            return True

        # Reject repeated digits
        if len(set(digits)) == 1:
            return True

        # Block known test numbers
        blocked = {
            "123456789", "078051120", "000000000",
            "111111111", "222222222", "333333333",
            "444444444", "555555555", "666666666",
            "777777777", "888888888", "999999999",
        }

        if digits in blocked:
            return True

        return False
