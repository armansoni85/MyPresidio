from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class SwedenNationalIDRecognizer(PatternRecognizer):
    logger.info("Initializing Sweden National Identification Number Recognizer...")

    # Define patterns for Swedish National Identification Numbers
    PATTERNS = [
        Pattern(
            "Sweden National ID - 10 or 12 digits",
            r"\b(?:19|20)?[0-9]{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01])[-\s+]?[0-9]{4}\b",  # Allows hyphen, space, or plus
            1.0  # High confidence due to date validation
        )
    ]

    # Context keywords for Swedish National ID
    CONTEXT = [
        "id no", "id number", "id#", "identification no", "identification number",
        "identifikationsnumret#", "identifikationsnumret", "identitetshandling",
        "identity document", "identity no", "identity number", "id-nummer", "personal id",
        "personnummer#", "personnummer", "skatteidentifikationsnummer"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "sv",  # Supports Swedish and English
        supported_entity: str = "SWEDEN_NATIONAL_ID",
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
        logger.info(f"Analyzing text for Swedish National ID: {text}")
        results = super().analyze(text, entities, nlp_artifacts)
        for result in results:
            national_id = text[result.start:result.end]
            logger.debug(f"Detected potential National ID: {national_id}, Initial confidence: {result.score}")

            # Remove all delimiters for checksum validation
            cleaned_national_id = re.sub(r"[-\s+]", "", national_id)
            logger.debug(f"Cleaned National ID: {cleaned_national_id}")

            # Perform Luhn checksum validation
            if self._is_valid_checksum(cleaned_national_id):
                logger.info(f"Checksum valid for National ID: {national_id}")
                result.score = 0.7  # Medium confidence if checksum passes
                
                # Boost confidence if context keywords are present
                if any(keyword in text.lower() for keyword in self.CONTEXT):
                    logger.info(f"Context keywords found for National ID: {national_id}, setting high confidence.")
                    result.score = 1.0
            else:
                logger.warning(f"Invalid checksum for National ID: {national_id}")
                result.score = 0.0  # Invalid ID
        return results

    def _is_valid_checksum(self, national_id: str) -> bool:
        """
        Validate the checksum using Luhn's algorithm (Modulus 10).
        Only the last 10 digits are considered for validation.
        """
        # Consider only the last 10 digits for validation
        if len(national_id) < 10:
            logger.error(f"Invalid length for National ID: {national_id}")
            return False
            
        check_digits = national_id[-10:]
        logger.debug(f"Validating checksum for: {check_digits}")
        
        total = 0
        for i, digit in enumerate(reversed(check_digits)):
            num = int(digit)
            if i % 2 == 1:  # Double every second digit
                num *= 2
                if num > 9:
                    num -= 9
            total += num
            
        return total % 10 == 0
