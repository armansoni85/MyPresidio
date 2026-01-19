import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional

class OklahomaRecognizer(PatternRecognizer):
    """
    Recognizer for detecting Oklahoma-based personal data including Credit Card Numbers,
    Credit Card Track Data, Social Security Numbers, and Oklahoma Driver's License Numbers.
    """

    # Updated patterns for detecting Oklahoma personal data
    CREDIT_CARD_PATTERN = r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})(?:[-\s]?\d{4})?\b"  # Detects credit card numbers with spaces/dashes
    CCN_TRACK_DATA_PATTERN = r'Name:\s*"?(?P<name>[\w\s\-]+)"?.*?Card_Number:\s*"?(?P<ccn>\d{12,16})"?\s*.*?Issuer:\s*"?(?P<issuer>\w+)"?.*?Expiry_Date:\s*"?(?P<expiry>\d{2}/\d{2})"?'
    SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"  # Detects valid SSNs in Oklahoma
    OKLAHOMA_DL_PATTERN = r"\b[0-9]{9}\b"  # Detects Oklahoma Driver's License Numbers (simplified pattern)

    # Common context terms for Oklahoma-related data
    CONTEXT_TERMS: List[str] = [
        "Oklahoma", "OK", "driver's license", "license", "SSN", "social security", "credit card", "card number", "ccn", "payment card"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("Credit Card Number", self.CREDIT_CARD_PATTERN, 0.85),
            Pattern("Credit Card Track Data", self.CCN_TRACK_DATA_PATTERN, 0.95),
            Pattern("SSN", self.SSN_PATTERN, 0.9),
            Pattern("Oklahoma Driver's License", self.OKLAHOMA_DL_PATTERN, 0.85)
        ]
        super().__init__(
            supported_entity="US_OKHB2357",
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

# Sample input text
# text = """
# John Doe, a resident of Oklahoma, reported his CCN is 4316-0200-0063-0490.
# The Oklahoma Driver's License Number is 123456789.
# His social security number (SSN) is 123-45-6789, and the following track data was found: 
# Name: "John Doe", Card_Number: "4316020000630490", Issuer: "Visa", Expiry_Date: "12/25".
# """
