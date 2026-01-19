import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional


class GermanIDCardRecognizer(PatternRecognizer):
    """
    Recognizer to detect German Identity Card Numbers.
    """

    # Patterns for German ID Card Numbers (both since 1 November 2010 and from 1 April 1987 to 31 October 2010)
    PATTERNS = [
        # Pattern for ID Cards since 1 November 2010: 9 to 11 characters (alphanumeric)
        Pattern(
            name="Germany ID Card (since 2010)",
            regex=r"\b[L-NP-TVWXY][CFGHJKLMNPRTVWXYZ0-9]{8}[CFGHJKLMNPRTVWXYZ0-9]?[dD]?\b",
            score=0.85,
        ),
        # Pattern for ID Cards from 1 April 1987 until 31 October 2010: 10 digits
        Pattern(
            name="Germany ID Card (1987-2010)",
            regex=r"\b\d{10}\b",
            score=0.85,
        ),
    ]

    CONTEXT = [
        "ausweis", "gpid", "identification", "identifikation", "identifizierungsnummer",
        "identity card", "identity number", "id-nummer", "personal id", "personalausweis",
        "persönliche id nummer", "persönliche identifikationsnummer", "persönliche-id-nummer"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "de",
        supported_entity: str = "DE_Germany_ID_Card",
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

    def _validate_checksum(self, id_card_number: str) -> bool:
        """Validate the checksum of the German ID card number."""
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
