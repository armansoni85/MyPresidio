import re
import logging
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional

# Configure logging
logger = logging.getLogger("presidio-analyzer")
logger.setLevel(logging.DEBUG)  # or INFO in production


class GermanIBANRecognizer(PatternRecognizer):
    """
    Recognizer to detect and validate German International Bank Account Numbers (IBAN).
    """

    PATTERNS = [
        Pattern(
            name="German IBAN (compact)",
            regex=r"\bDE\d{20}\b",
            score=1.0,
        ),
        Pattern(
            name="German IBAN (spaced)",
            regex=r"\bDE\d{2}(?:\s?\d{4}){5}\b",
            score=1.0,
        ),
    ]

    CONTEXT = [
        "iban", "international bank account number", "bank account", "bank account number",
        "bankkonto", "kontonummer", "bankverbindung", "girokonto"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "de",
        supported_entity: str = "DE_German_IBAN",
    ):
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns or self.PATTERNS,
            context=context or self.CONTEXT,
            supported_language=supported_language,
        )

    def analyze(
        self, text: str, entities: Optional[List[str]] = None, nlp_artifacts=None
    ) -> List[RecognizerResult]:

        results = super().analyze(text, entities, nlp_artifacts)
        logger.debug(f"Analyzing text for German IBANs: Found {len(results)} potential matches")

        for result in results:
            iban_text = text[result.start:result.end].replace(" ", "")
            logger.debug(f"Detected pattern: '{iban_text}' at [{result.start}:{result.end}]")

            has_context = self._has_context(text, result.start, result.end)
            logger.debug(f"Context found: {has_context}")

            is_valid_checksum = self._validate_checksum(iban_text)
            logger.debug(f"Checksum valid: {is_valid_checksum}")

            # Adjust scoring based on validation & context
            old_score = result.score
            if has_context and is_valid_checksum:
                result.score = min(result.score + 0.2, 1.0)
            elif not is_valid_checksum:
                result.score *= 0.4
            elif not has_context:
                result.score *= 0.7

            logger.debug(
                f"Adjusted score for '{iban_text}': {old_score:.2f} → {result.score:.2f}"
            )

        return results

    def _validate_checksum(self, iban: str) -> bool:
        """Validate German IBAN checksum using the MOD-97 algorithm."""
        if not re.match(r"^DE\d{20}$", iban):
            logger.debug(f"IBAN '{iban}' failed format validation (must be DE + 20 digits)")
            return False

        rearranged = iban[4:] + iban[:4]
        try:
            numeric = "".join(str(int(ch, 36)) if ch.isalpha() else ch for ch in rearranged)
            valid = int(numeric) % 97 == 1
            logger.debug(f"IBAN '{iban}' numeric value modulo 97 = {int(numeric) % 97} → valid: {valid}")
            return valid
        except Exception as e:
            logger.error(f"Error validating IBAN checksum for '{iban}': {e}")
            return False

    def _has_context(self, text: str, start: int, end: int) -> bool:
        """Check for relevant context words near the detected pattern."""
        window_size = 100
        context_window = text[max(0, start - window_size): min(len(text), end + window_size)]
        context_found = any(word.lower() in context_window.lower() for word in self.CONTEXT)
        logger.debug(f"Context window check: Found={context_found}")
        return context_found

