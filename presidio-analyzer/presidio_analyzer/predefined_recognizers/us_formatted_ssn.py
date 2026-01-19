from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional

class US_Formatted_SSN_Recognizer(PatternRecognizer):
    """Recognizer to detect US Formatted Social Security Numbers (SSN) with context matching."""

    PATTERNS = [
        Pattern(name="formatted SSN", regex=r"\b\d{3}[- ]\d{2}[- ]\d{4}\b", score=0.85),
    ]

    CONTEXT = [
        "SSA Number",
        "social security number",
        "social security #",
        "social security#",
        "social security no",
        "Social Security#",
        "Soc Sec",
        "SSN",
        "SSNS",
        "SSN#",
        "SS#",
        "SSID",
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "US_Formatted_SSN",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None):
        results = super().analyze(text, entities, nlp_artifacts)

        # Apply context-based scoring
        for result in results:
            if self._has_context(text, result.start, result.end):
                result.score = min(result.score + 0.4, 1.0)  # Boost score with context
            else:
                result.score = result.score * 0.5  # Reduce score if no context found

        return results

    def _has_context(self, text: str, start: int, end: int) -> bool:
        """Check if there is relevant context around the detected pattern."""
        window_size = 100  # Number of characters to check before and after the detected pattern
        context_window = text[max(0, start - window_size): min(len(text), end + window_size)]
        context_found = any(context_word.lower() in context_window.lower() for context_word in self.CONTEXT)
        return context_found

    def invalidate_result(self, pattern_text: str) -> bool:
        """
        Invalidate SSNs that don't meet certain criteria.
        """
        only_digits = "".join(c for c in pattern_text if c.isdigit())

        # Validate length
        if len(only_digits) != 9:
            return True

        # All digits the same
        if all(d == only_digits[0] for d in only_digits):
            return True

        # Invalid SSN segments
        if only_digits[:3] in {"000", "666", "900"} or only_digits[3:5] == "00" or only_digits[5:] == "0000":
            return True

        # Specific invalid SSNs
        if only_digits in {"123456789", "987654320", "078051120"}:
            return True

        return False