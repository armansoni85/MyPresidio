from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class FranceIBANRecognizer(PatternRecognizer):
    logger.info("Initializing France IBAN Recognizer...")

    # Define patterns for France IBAN Numbers
    PATTERNS = [
        Pattern(
            "France IBAN - Full Pattern",
            r"\b(FR|MC)\d{2}(?: ?[A-Z0-9]{4}){1,7}[A-Z0-9]{1,3}\b",  # IBAN format with groups of 4, allowing spaces
            0.5  # Initial confidence score for the pattern match
        ),
        Pattern(
            "France IBAN - Alternative Pattern",
            r"\b(FR|MC)[0-9]{12}[A-Z0-9]{11}[0-9]{2}\b",  # IBAN pattern: FR/MC, 12 digits, 11 alphanumeric, 2 digits
            0.5  # Initial confidence score for the pattern match
        )
    ]

    # Context keywords for IBAN
    CONTEXT = [
        "IBAN", "international bank account number", "numéro de compte", "numéro IBAN",
        "code IBAN", "identifiant bancaire"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "fr",  # Supports French and English
        supported_entity: str = "FRANCE_IBAN",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def analyze(
        self, text: str, entities: List[str], nlp_artifacts=None
    ) -> List[RecognizerResult]:
        logger.info(f"Analyzing text for France IBAN: {text}")
        results = super().analyze(text, entities, nlp_artifacts)
        for result in results:
            iban_number = text[result.start:result.end]
            logger.debug(f"Detected IBAN: {iban_number}, Confidence: {result.score}")

            # Remove spaces for checksum validation
            cleaned_iban = re.sub(r"\s+", "", iban_number)

            # Perform checksum validation
            if self._is_valid_checksum(cleaned_iban):
                logger.info(f"Checksum valid for IBAN: {iban_number}")
                result.score = 0.7  # Medium confidence if checksum passes
                if any(keyword in text.lower() for keyword in self.CONTEXT):
                    logger.info(f"Context keywords found for IBAN: {iban_number}, setting high confidence.")
                    result.score = 1.0  # High confidence if context keywords are present
            else:
                logger.warning(f"Invalid checksum for IBAN: {iban_number}")
                result.score = 0.0  # Invalid IBAN
        return results

    def _is_valid_checksum(self, iban: str) -> bool:
        """
        Validate the IBAN checksum according to the ISO 13616 standard.
        The IBAN is validated by moving the first four characters to the end and converting letters to numbers.
        """
        # Move first four characters to the end
        rearranged_iban = iban[4:] + iban[:4]

        # Replace each letter with its corresponding number (A = 10, B = 11, ..., Z = 35)
        numeric_iban = ''.join(str(int(char, 36)) if char.isalpha() else char for char in rearranged_iban)

        # Perform modulus 97 check
        try:
            return int(numeric_iban) % 97 == 1
        except ValueError:
            return False  # Handle the case where numeric_iban isn't fully numeric
