from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional


class UsSsnRecognizer(PatternRecognizer):
    """Recognizer to detect US Social Security Numbers (SSN) with context matching."""

    PATTERNS = [
        Pattern("SSN (medium)", r"\b[0-9]{3}[- ]?[0-9]{2}[- ]?[0-9]{4}\b", 0.5),
        Pattern("SSN (medium)", r"\b[0-9]{9}\b", 0.5),
    ]

    CONTEXT = [
        "social security number",
        "social security",
        "ssn",
        "ss#",
        "ssn#",
        "security number",
        "patient social security number",
        "patient ssn",
        "patient's ssn",
        "patient",
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "US_SSN",
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
        window_size = 50  # Number of characters to check before and after the detected pattern
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
