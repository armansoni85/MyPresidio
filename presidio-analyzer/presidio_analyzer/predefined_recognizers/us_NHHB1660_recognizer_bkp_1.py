import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional
import logging

logger = logging.getLogger("presidio-analyzer")

class NewHampshirePolicyRecognizer(PatternRecognizer):
    """
    Recognizer for detecting sensitive personal data from New Hampshire, including:
    - Credit Card Numbers (CCN)
    """

    # Updated CCN Regex
    CREDIT_CARD_PATTERN = (
        r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|6(?:011|5[0-9]{2})[0-9]{12}|"
        r"3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|(?:2131|1800|35\d{3})\d{11})\b"
    )

    CONTEXT_TERMS: List[str] = [
        "New Hampshire", "NH", "credit card", "social security", "SSN", "driver's license", "DL"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("New Hampshire Credit Card Number", self.CREDIT_CARD_PATTERN, 0.85),
        ]
        super().__init__(
            supported_entity="US_NHHB1660",
            patterns=patterns,
            context=self.CONTEXT_TERMS,
            supported_language=supported_language,
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

