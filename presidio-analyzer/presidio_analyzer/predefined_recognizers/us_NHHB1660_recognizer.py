import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class NewHampshirePolicyRecognizer(PatternRecognizer):
    """
    Recognizer for detecting sensitive personal data from New Hampshire including:
    - Credit Card Numbers (CCN)
    - CCN Track Data
    - Social Security Numbers (SSN)
    - Driver's License Numbers
    """

    # Define patterns for New Hampshire specific data
    CREDIT_CARD_PATTERN = (
        r"\b(?:"
        r"4[0-9]{12}(?:[0-9]{3})?|"               # Visa (13 or 16 digits)
        r"5[1-5][0-9]{14}|"                       # MasterCard (16 digits)
        r"3[47][0-9]{13}|"                        # American Express (15 digits)
        r"6(?:011|5[0-9]{2})[0-9]{12}|"           # Discover (16 digits)
        r"3(?:0[0-5]|[68][0-9])[0-9]{11}|"        # Diners Club (14 digits)
        r"(?:2131|1800|35\d{3})\d{11}|"           # JCB (15 or 16 digits)
        r"(?:5[06789]|6)[0-9]{2}[0-9]{12,17}|"    # Maestro (12 to 19 digits)
        r"62[0-9]{14,17}"                         # China UnionPay (16 to 19 digits)
        r")\b"
    )
    CCN_TRACK_DATA_PATTERN = r"Name\"\s*\w*\s*\"(?P<name>[\w\s\-]+)\".*?Credit_Card_Number\"\s*\d+\s*\"(?P<ccn>\d+)\".*?Expiry[_\s]Date\"\s*\d+\s*\"(?P<expiry>\d{2}\/\d{2})\""
    SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"
    DRIVER_LICENSE_PATTERN = r"\b[A-Z0-9]{9}\b"  # Simplified driver’s license number pattern

    # Context keywords for New Hampshire
    CONTEXT_TERMS: List[str] = [
        "New Hampshire", "NH", "credit card", "social security", "SSN", "driver's license", "DL"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        # Create patterns for each sensitive entity
        patterns = [
            Pattern("New Hampshire Credit Card Number", self.CREDIT_CARD_PATTERN, 0.85),
            Pattern("New Hampshire CCN Track Data", self.CCN_TRACK_DATA_PATTERN, 0.9),
            Pattern("New Hampshire SSN", self.SSN_PATTERN, 0.9),
            Pattern("New Hampshire Driver's License", self.DRIVER_LICENSE_PATTERN, 0.8)
        ]
        super().__init__(
            supported_entity="US_NHHB1660",
            patterns=patterns,
            context=self.CONTEXT_TERMS,
            supported_language=supported_language
        )

    def validate_ccn(self, ccn: str) -> bool:
        """
        Validate credit card number using the Luhn algorithm.
        """
        logger.debug(f"Validating CCN: {ccn}")
        # Ensure the entire string matches the regex
        if not re.fullmatch(r"^[\d\s-]{13,19}$", ccn):
            logger.debug(f"CCN does not match basic format: {ccn}")
            return False

        # Clean the credit card number (remove spaces and hyphens)
        cleaned_ccn = re.sub(r"[^\d]", "", ccn)

        def luhn_checksum(card_number: str) -> bool:
            def digits_of(n):
                return [int(d) for d in str(n)]
            digits = digits_of(card_number)
            odd_digits = digits[-1::-2]
            even_digits = digits[-2::-2]
            checksum = sum(odd_digits)
            for d in even_digits:
                checksum += sum(digits_of(d * 2))
            return checksum % 10 == 0

        is_valid = luhn_checksum(cleaned_ccn)
        logger.debug(f"Luhn validation result for {ccn}: {is_valid}")
        return is_valid

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None):
        """
        Override analyze to incorporate CCN validation.
        """
        results = super().analyze(text, entities, nlp_artifacts)
        validated_results = []
        for result in results:
            detected_ccn = text[result.start:result.end]
            if result.entity_type == "US_NHHB1660":
                if self.validate_ccn(detected_ccn):
                    validated_results.append(result)
                else:
                    logger.debug(f"Invalid CCN detected and excluded: {detected_ccn}")
            else:
                validated_results.append(result)
        return validated_results
