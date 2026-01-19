import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional

class NewJerseyDLPRecognizer(PatternRecognizer):
    """
    Recognizer for detecting New Jersey specific data including:
    - New Jersey Credit Card Numbers (CCN)
    - Credit Card Track Data
    - New Jersey Social Security Numbers (SSN)
    - New Jersey Driver's License Numbers
    """

    # Define regex patterns for the various data types
    CREDIT_CARD_PATTERN = r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})\b"  # Visa, Mastercard, Amex
    TRACK_DATA_PATTERN = r'Name\"\s*s\s\d+\s\"(?P<name>[\w\s\-]+)\".*?Credit_Card_Number\"\s*s\s\d+\s\"(?P<ccn>\d+)\".*?Issuer\"\s*s\s\d+\s\"(?P<issuer>\w+)\".*?Expiry[_\s]Date\"\s*s\s\d+\s\"(?P<expiry>\d{2}\\\/\d{2})\".*?cvv\"\s*s\s\d+\s\"(?P<cvv>\d{3})\"'
    SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"  # Standard SSN pattern
    NEW_JERSEY_DL_PATTERN = r"\bNJ[A-Z0-9]{8}\b"  # New Jersey DL with NJ prefix followed by 8 alphanumeric characters

    # Define context terms for New Jersey data
    CONTEXT_TERMS: List[str] = [
        "New Jersey", "NJ", "driver's license", "credit card", "social security", "SSN", "credit card track data", "payment card"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("New Jersey Credit Card Number", self.CREDIT_CARD_PATTERN, 0.85),
            Pattern("New Jersey SSN", self.SSN_PATTERN, 0.9),
            Pattern("New Jersey Driver's License", self.NEW_JERSEY_DL_PATTERN, 0.8),
            Pattern("Credit Card Track Data", self.TRACK_DATA_PATTERN, 0.95)
        ]
        super().__init__(
            supported_entity="US_NJA4001",
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
            for d in even_digits:
                checksum += sum(digits_of(d * 2))
            return checksum % 10 == 0

        cleaned_ccn = re.sub(r"\D", "", ccn)  # Remove non-digit characters
        return luhn_checksum(cleaned_ccn)


# Sample input text for testing
# text = """
# John Doe from New Jersey has the following information. 
# His credit card number is 4111-1111-1111-1111. 
# His Social Security Number (SSN) is 123-45-6789. 
# He also holds a New Jersey Driver's License NJD1234567.
