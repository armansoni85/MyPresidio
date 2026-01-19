import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional

class GermanPassportRecognizer(PatternRecognizer):
    """
    Recognizer to detect German Passport Numbers.
    """

    # Patterns for German Passport Numbers (9 to 11 characters)
    PATTERNS = [
        # Pattern for German Passport Numbers without checksum: 9 characters
        Pattern(
            name="Germany Passport Number (9 characters, no checksum)",
            regex=r"\b[C-HJ-K][CFGHJKLMNPRTVWXYZ0-9]{8}\b",
            score=0.85,
        ),
        # Pattern for German Passport Numbers with checksum: 10 characters
        Pattern(
            name="Germany Passport Number (10 characters with checksum)",
            regex=r"\b[C-HJ-K][CFGHJKLMNPRTVWXYZ0-9]{8}[0-9]\b",
            score=0.9,
        ),
        # Pattern for German Passport Numbers with checksum and optional character: 11 characters
        Pattern(
            name="Germany Passport Number (11 characters with checksum and optional character)",
            regex=r"\b[C-HJ-K][CFGHJKLMNPRTVWXYZ0-9]{8}[0-9][dD]?\b",
            score=0.95,
        ),
        # Pattern for German Passport Numbers with checksum Matches an 11-character long alphanumeric string
        Pattern(
            name="Germany Passport Number (Matches an 11-character long alphanumeric string)",
            regex=r"\b[CF-HJ-NPRTV-Z0-9]{9}[0-9]?D?\b",
            score=1.0,
        ),
    ]

    CONTEXT = [
        "reisepasse", "reisepassnummer", "No-Reisepass", "Nr-Reisepass", "Reisepass-Nr",
        "Passnummer", "reisepässe", "passeport no.", "passeport no",
        "passport#", "passport #", "passportid", "passports", "passportno", "passport no",
        "passportnumber", "passport number", "passportnumbers", "passport numbers"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "de",
        supported_entity: str = "DE_Germany_Passport",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        results = super().analyze(text, entities, nlp_artifacts)

        # Apply context-based scoring and checksum validation
        for result in results:
            if self._has_context(text, result.start, result.end) and self._validate_checksum(text[result.start:result.end]):
                result.score = min(result.score + 0.15, 1.0)  # Boost score if context and checksum pass
            else:
                result.score = result.score * 0.5  # Reduce score if context or checksum fails

        return results

    def _validate_checksum(self, passport_number: str) -> bool:
        """Validate the checksum of the German passport number."""
        # Implement the checksum logic here, if applicable.
        # Since the actual checksum logic isn't specified, assuming a placeholder validation.
        # Replace with actual checksum algorithm as required.
        return True  # Placeholder: Assume checksum is valid

    def _has_context(self, text: str, start: int, end: int) -> bool:
        """Check if there is relevant context around the detected pattern."""
        window_size = 300  # Number of characters to check before and after the detected pattern
        context_window = text[max(0, start - window_size): min(len(text), end + window_size)]
        context_found = any(context_word.lower() in context_window.lower() for context_word in self.CONTEXT)
        return context_found
