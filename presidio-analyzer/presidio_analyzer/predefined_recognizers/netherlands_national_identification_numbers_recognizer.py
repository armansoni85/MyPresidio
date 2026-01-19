from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")
logger.setLevel(logging.DEBUG)

class NetherlandsNationalIDRecognizer(PatternRecognizer):
    logger.info("Initializing Netherlands National Identification Number Recognizer...")

    # Pattern to match BSNs in different formats: 123456789, 123 456 789, 123-456-789
    PATTERNS = [
        Pattern(
            "Netherlands National ID - High Confidence",
            r"\b\d{3}[- ]?\d{3}[- ]?\d{2,3}\b",  # Matches 8 or 9 digits with optional spaces or dashes
            0.7
        )
    ]

    # Context keywords for National Identification Number
    CONTEXT = [
        "burgerservicenummer", "national identification number", "bsn",
        "national ID", "nummer", "ID number", "identificatienummer"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "nl",
        supported_entity: str = "NETHERLANDS_NATIONAL_ID",
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
        logger.info(f"Analyzing text for Netherlands National ID: {text}")
        results = super().analyze(text, entities, nlp_artifacts)

        for result in results:
            national_id_raw = text[result.start:result.end]
            # Remove spaces and dashes for validation
            national_id = re.sub(r"[- ]", "", national_id_raw)
            logger.debug(f"Detected National ID: {national_id_raw}, Cleaned: {national_id}")

            if self._is_valid_bsn(national_id):
                logger.info(f"Valid BSN detected: {national_id}")
                result.score = 0.7
                if any(keyword.lower() in text.lower() for keyword in self.CONTEXT):
                    logger.info(f"Context keywords found. Boosting score for {national_id}")
                    result.score = 1.0
            else:
                logger.warning(f"Invalid BSN: {national_id}")
                result.score = 0.0

        return results

    def _is_valid_bsn(self, national_id: str) -> bool:
        """
        Validate a Netherlands BSN (8 or 9 digits) using the 11-test algorithm.
        For 9-digit BSN: last digit multiplied by -1.
        For 8-digit BSN: multiply from 9 down to 2.
        """
        national_id = national_id.strip()
        if not national_id.isdigit() or len(national_id) not in (8, 9):
            logger.error(f"BSN {national_id} is not numeric or wrong length.")
            return False

        digits = [int(d) for d in national_id]
        length = len(digits)

        if length == 9:
            multipliers = list(range(9, 1, -1)) + [-1]
        else:  # 8-digit BSN
            multipliers = list(range(9, 1, -1))

        total_sum = sum(d * m for d, m in zip(digits, multipliers))
        is_valid = total_sum % 11 == 0

        if is_valid:
            logger.debug(f"BSN {national_id} passed 11-test.")
        else:
            logger.debug(f"BSN {national_id} failed 11-test.")

        return is_valid

