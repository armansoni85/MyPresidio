from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class SpainSSNRecognizer(PatternRecognizer):
    logger.info("Initializing Spain Social Security Number Recognizer...")

    # Define patterns for Spanish Social Security Numbers (SSN)
    PATTERNS = [
        Pattern(
            "Spain SSN - High Confidence",
            r"(?:\b[0-6][0-9][ -]?[0-9]{7,8}[ -]?[0-9]{2}\b)",  # Updated to handle 11/12 digits
            0.5  # Initial confidence score for the pattern match
        ),
        Pattern(
            "Spain SSN - Alternate Format",
            r"\b\d{2}/?\d{7,8}/?\d{2}\b",  # Alternate format for SSN
            0.5  # Initial confidence score for the pattern match
        )
    ]

    # Spanish and English context keywords for SSN
    CONTEXT = [
        "ssn", "ssn#", "socialsecurityno", "social security no",
        "social security number", "número de la seguridad social"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "es",  # Supports Spanish and English
        supported_entity: str = "SPAIN_SSN",
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
        logger.info(f"Analyzing text: {text}")
        results = super().analyze(text, entities, nlp_artifacts)
        for result in results:
            ssn = text[result.start:result.end]
            logger.debug(f"Detected SSN: {ssn}, Confidence: {result.score}")
            if self._is_valid_checksum(ssn):
                logger.info(f"Checksum valid for SSN: {ssn}")
                result.score = 0.7  # Medium confidence if checksum passes
                # Check for context keywords in the surrounding text
                if any(keyword in text.lower() for keyword in self.CONTEXT):
                    logger.info(f"Context keywords found for SSN: {ssn}, setting high confidence.")
                    result.score = 1.0  # High confidence if checksum passes and context keywords are present
            else:
                logger.warning(f"Invalid checksum for SSN: {ssn}")
                result.score = 0.0  # Invalid SSN
        return results

    def _is_valid_checksum(self, ssn: str) -> bool:
        """
        Validate the checksum for the given Spanish SSN.
        The last two digits in the SSN represent the checksum.
        """
        logger.debug(f"Validating checksum for SSN: {ssn}")

        # Remove any non-numeric characters
        ssn_digits = re.sub(r"\D", "", ssn)
        logger.debug(f"SSN digits after removing non-numeric characters: {ssn_digits}")

        # Validate length (must be 11 or 12 digits)
        if len(ssn_digits) not in (11, 12):
            logger.error(f"SSN {ssn} has invalid length: {len(ssn_digits)} digits")
            return False

        # Determine base number length (9 digits for 11-digit SSN, 10 for 12-digit)
        base_length = 9 if len(ssn_digits) == 11 else 10
        base_number = ssn_digits[:base_length]
        checksum = ssn_digits[-2:]
        logger.debug(f"Base number: {base_number}, Provided checksum: {checksum}")

        # Calculate expected checksum
        try:
            calculated_checksum = str(int(base_number) % 97).zfill(2)
        except ValueError as e:
            logger.error(f"Error converting base number to integer: {e}")
            return False

        logger.debug(f"Calculated checksum: {calculated_checksum}")

        # Check if calculated checksum matches the provided checksum
        is_valid = checksum == calculated_checksum
        if is_valid:
            logger.info(f"Checksum for SSN {ssn} is valid.")
        else:
            logger.error(f"Checksum for SSN {ssn} is invalid. Expected {calculated_checksum}, but got {checksum}.")
        return is_valid
