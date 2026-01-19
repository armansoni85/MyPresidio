from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class FranceVATRecognizer(PatternRecognizer):
    logger.info("Initializing France VAT Recognizer...")

    # Define patterns for France VAT Numbers
    PATTERNS = [
        Pattern(
            "France VAT - Standard Format",
            r"\bFR[-\s]?[0-9]{2}[-\s]?[0-9]{3}[-\s]?[0-9]{3}[-\s]?[0-9]{3}\b",  # FR + 2 digits + 3 + 3 + 3 digits
            0.7
        ),
        Pattern(
            "France VAT - Compact Format",
            r"\bFR[0-9]{11}\b",  # FR followed by exactly 11 digits
            0.8
        ),
        Pattern(
            "France VAT - With Letters",
            r"\bFR[-\s]?[A-Za-z0-9]{2}[-\s]?[A-Za-z0-9]{3}[-\s]?[A-Za-z0-9]{3}[-\s]?[A-Za-z0-9]{3}\b",  # Allows letters in all parts
            0.5
        )
    ]

    # Context keywords for VAT Numbers
    CONTEXT = [
        "vat number", "vat no", "vat#", "value added tax", "siren identification no",
        "numéro d'identification", "taxe sur la valeur ajoutée", "n° tva",
        "numéro de tva", "numéro d'identification siren", "siret", "siren",
        "tva intracommunautaire", "numero tva"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "fr",
        supported_entity: str = "FRANCE_VAT",  # Changed to more standard naming
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
        logger.info(f"Analyzing text for France VAT: {text}")
        results = super().analyze(text, entities, nlp_artifacts)
        for result in results:
            vat_number = text[result.start:result.end]
            logger.debug(f"Detected VAT: {vat_number}, Confidence: {result.score}")

            # Remove spaces and non-alphanumeric characters for validation
            cleaned_vat = re.sub(r"[^A-Za-z0-9]", "", vat_number).upper()

            # Perform validation
            if self._is_valid_vat(cleaned_vat):
                logger.info(f"Valid VAT: {vat_number}")
                result.score = 0.9  # High confidence for valid format
                if any(keyword.lower() in text.lower() for keyword in self.CONTEXT):
                    logger.info(f"Context keywords found for VAT: {vat_number}")
                    result.score = 1.0  # Very high confidence with context
            else:
                logger.warning(f"Invalid VAT format: {vat_number}")
                result.score = 0.3  # Low confidence for invalid format
        return results

    def _is_valid_vat(self, vat: str) -> bool:
        """
        Validate French VAT number format and checksum.
        French VAT numbers can be in these formats:
        - FR + 2 digits (SIREN checksum) + 9 digits (SIREN)
        - FR + 2 letters + 9 digits
        - FR + 1 letter + 10 digits
        - FR + 11 digits (numeric)
        """
        if not vat.startswith("FR"):
            return False

        # Check total length (FR + 11 characters = 13)
        if len(vat) != 13:
            return False

        # Extract the part after FR
        vat_body = vat[2:]

        # For numeric VAT (FR + 11 digits)
        if vat_body.isdigit():
            # The first two digits should be a checksum of the remaining 9 (SIREN)
            checksum = int(vat_body[:2])
            siren = int(vat_body[2:])
            return (12 + 3 * siren) % 97 == checksum
        else:
            # For alphanumeric VAT, we can't do checksum validation but can check format
            # Pattern: FR + (2 letters or 1 letter + 1 digit or 2 digits) + 9 digits
            return bool(re.fullmatch(r"([A-Z]{2}|[A-Z]\d|\d[A-Z]|\d{2})\d{9}", vat_body))
