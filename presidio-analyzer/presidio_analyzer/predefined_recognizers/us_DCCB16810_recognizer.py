import re
from presidio_analyzer import PatternRecognizer, Pattern, RecognizerResult
from typing import List, Optional

class ColumbiaDLPRecognizer(PatternRecognizer):
    """
    Recognizer for detecting Columbia-specific personal data including:
    - Credit Card Numbers (CCN)
    - Credit Card Track Data
    - Social Security Numbers (SSN)
    - Driver's License Numbers
    """

    # Patterns for detecting Columbia-specific data
    COLUMBIA_CCN_PATTERN = r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})\b"  # Detects Columbia credit card numbers
    COLUMBIA_SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"  # Detects Columbia Social Security Numbers
    COLUMBIA_DL_PATTERN = r"\b[A-Z0-9]{9}\b"  # Columbia Driver's License Pattern (customizable)
    COLUMBIA_TRACK_DATA_PATTERN = r'Name\"\s*s\s\d+\s\"(?P<name>[\w\s\-]+)\".*?Credit_Card_Number\"\s*s\s\d+\s\"(?P<ccn>\d+)\".*?Issuer\"\s*s\s\d+\s\"(?P<issuer>\w+)\".*?Expiry[_\s]Date\"\s*s\s\d+\s\"(?P<expiry>\d{2}\\\/\d{2})\".*?cvv\"\s*s\s\d+\s\"(?P<cvv>\d{3})\"'  # Columbia Credit Card Track Data pattern

    # Context terms to boost confidence scores
    CONTEXT_TERMS: List[str] = [
        "credit card", "card number", "expiry date", "cvv",
        "social security", "ssn", "driver's license", "dl",
        "Número de seguridad social", "cédula", "tarjeta de crédito"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("Columbia Credit Card Number", self.COLUMBIA_CCN_PATTERN, 0.85),
            Pattern("Columbia SSN", self.COLUMBIA_SSN_PATTERN, 0.9),
            Pattern("Columbia Driver's License", self.COLUMBIA_DL_PATTERN, 0.8),
            Pattern("Columbia Credit Card Track Data", self.COLUMBIA_TRACK_DATA_PATTERN, 0.95),
        ]
        super().__init__(
            supported_entity="US_DCCB16810",
            patterns=patterns,
            context=self.CONTEXT_TERMS,
            supported_language=supported_language
        )

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        """
        Analyze method to detect Columbia-specific personal data in text.
        """
        results = super().analyze(text, entities, nlp_artifacts=nlp_artifacts)

        # Validate credit card numbers using the Luhn algorithm
        valid_results = []
        for result in results:
            if result.entity_type == "Columbia Credit Card Number" and self.validate_ccn(text[result.start:result.end]):
                valid_results.append(result)
            else:
                valid_results.append(result)  # For other patterns, no validation logic is applied

        return valid_results

    def validate_ccn(self, ccn: str) -> bool:
        """
        Validate credit card number using the Luhn algorithm.
        """
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

        cleaned_ccn = re.sub(r"\D", "", ccn)  # Remove non-digit characters
        return luhn_checksum(cleaned_ccn)
