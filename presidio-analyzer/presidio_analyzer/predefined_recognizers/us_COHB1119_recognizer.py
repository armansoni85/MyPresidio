import re
from presidio_analyzer import Pattern, PatternRecognizer
from typing import List, Optional

class COHB1119Recognizer(PatternRecognizer):
    """
    Custom recognizer for detecting Colorado-based Credit Card Numbers (CCN), 
    Credit Card Track Data, Social Security Numbers (SSN), and Colorado Driver's License Numbers.
    """

    # Patterns for detecting sensitive information
    COLORADO_CCN_PATTERN = r"\b(?:4[0-9]{3}[-\s]?[0-9]{4}[-\s]?[0-9]{4}[-\s]?[0-9]{4}|5[1-5][0-9]{2}[-\s]?[0-9]{4}[-\s]?[0-9]{4}[-\s]?[0-9]{4}|3[47][0-9]{2}[-\s]?[0-9]{6}[-\s]?[0-9]{5})\b"  # CCN pattern
    CCN_TRACK_DATA_PATTERN = r'Name\"\s*s\s\d+\s\"(?P<name>[\w\s\-]+)\".*?Credit_Card_Number\"\s*s\s\d+\s\"(?P<ccn>\d+)\".*?Expiry[_\s]Date\"\s*s\s\d+\s\"(?P<expiry>\d{2}\\\/\d{2})\".*?cvv\"\s*s\s\d+\s\"(?P<cvv>\d{3})\"'
    COLORADO_SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"
    COLORADO_DL_PATTERN = r"\b[0-9]{9}\b"  # Pattern for Colorado Driver's License (Assume 9 digits)

    # Common context terms for credit card, SSN, and driver's license data
    CONTEXT_TERMS: List[str] = [
        "colorado", "CO", "credit card", "card number", "cvv",
        "social security", "ssn", "driver's license", "dl"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("Colorado Credit Card Number", self.COLORADO_CCN_PATTERN, 0.85),
            Pattern("Credit Card Track Data", self.CCN_TRACK_DATA_PATTERN, 0.95),
            Pattern("Colorado SSN", self.COLORADO_SSN_PATTERN, 0.9),
            Pattern("Colorado Driver's License", self.COLORADO_DL_PATTERN, 0.8)
        ]
        super().__init__(
            supported_entity="US_COHB1119",
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

# Sample input text to be analyzed
# text = """
# Bob and Jane Smith live in Colorado. Their CCN is 4316-0200-0063-0490. 
# Bobs Colorado Drivers License is 123456789. The date is 2024-09-09. Their next of kin is James Smith 
# reporting their SSNs were 260-53-2093 and 260-56-4928. Their credit card track data includes: 
# Name: "John Doe", Credit_Card_Number: "4111111111111111", Expiry_Date: "12/25", CVV: "123".
# """