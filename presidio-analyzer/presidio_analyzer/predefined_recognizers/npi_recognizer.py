from presidio_analyzer import Pattern, PatternRecognizer
from typing import List, Optional

class NPIRecognizer(PatternRecognizer):
    """
    Recognizer to detect National Provider Identifiers (NPI) numbers
    (10-digit numbers with LUHN checksum validation).
    """

    PATTERNS = [
        Pattern("NPI (10-digit number)", r"\b\d{10}\b", 0.5),
    ]

    CONTEXT = [
        "npi", "national provider id", "provider id", "provider identifier",
        "npi number", "healthcare provider"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "US_NPI",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def validate_luhn(self, npi_number: str) -> bool:
        """LUHN checksum validation for NPI numbers."""
        total = 0
        reverse_digits = npi_number[::-1]
        for i, digit in enumerate(reverse_digits):
            n = int(digit)
            if i % 2 == 0:
                total += n
            else:
                doubled = n * 2
                total += doubled if doubled < 10 else doubled - 9
        return total % 10 == 0

    def invalidate_result(self, pattern_text: str) -> bool:
        """Invalidate if LUHN checksum fails."""
        return not self.validate_luhn(pattern_text)

