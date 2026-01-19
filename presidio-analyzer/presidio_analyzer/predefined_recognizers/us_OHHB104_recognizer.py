import re
from presidio_analyzer import Pattern, PatternRecognizer
from typing import List, Optional

class OhioDataRecognizer(PatternRecognizer):
    """
    Recognizer for detecting Ohio-based personal data including Credit Card Number (CCN),
    Credit Card Track Data, Social Security Numbers (SSNs), and Ohio Driver's License numbers.
    """

    # Regular expressions for different patterns of Ohio personal data
    OHIO_CCN_PATTERN = r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})\b"  # General CCN regex
    OHIO_SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"  # US SSN
    OHIO_DRIVERS_LICENSE_PATTERN = r"\bOH\d{8}\b"  # Ohio Driver's License pattern (this is an example pattern, it can be more specific)
    CCN_TRACK_DATA_PATTERN = r'Name\"\s*s\s\d+\s\"(?P<name>[\w\s\-]+)\".*?Credit_Card_Number\"\s*s\s\d+\s\"(?P<ccn>\d+)\".*?Issuer\"\s*s\s\d+\s\"(?P<issuer>\w+)\".*?Expiry[_\s]Date\"\s*s\s\d+\s\"(?P<expiry>\d{2}\\\/\d{2})\".*?cvv\"\s*s\s\d+\s\"(?P<cvv>\d{3})\"'

    # Context terms to increase detection confidence
    CONTEXT_TERMS: List[str] = [
        "credit card", "card number", "expiry date", "cvv", 
        "social security", "ssn", "ohio driver's license", "ohio dl", "ohio id"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        # Define patterns for Ohio-related data
        patterns = [
            Pattern("Ohio CCN", self.OHIO_CCN_PATTERN, 0.85),
            Pattern("Ohio SSN", self.OHIO_SSN_PATTERN, 0.9),
            Pattern("Ohio Driver's License", self.OHIO_DRIVERS_LICENSE_PATTERN, 0.8),
            Pattern("Credit Card Track Data", self.CCN_TRACK_DATA_PATTERN, 0.95)
        ]
        super().__init__(
            supported_entity="US_OHHB104",
            patterns=patterns,
            context=self.CONTEXT_TERMS,
            supported_language=supported_language
        )

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
