import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional

class FLHB481Recognizer(PatternRecognizer):
    """
    Recognizer for detecting Florida-based personal data including Credit Card Numbers (CCNs),
    Credit Card Track Data, Social Security Numbers (SSNs), and Florida Driver License Numbers in text.
    """

    # Patterns for Florida CCN, SSN, Driver's License, and Credit Card Track Data
    FLORIDA_CCN_PATTERN = r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})\b"
    TRACK_DATA_PATTERN = r'Name\"\s*s\s\d+\s\"(?P<name>[\w\s\-]+)\".*?Credit_Card_Number\"\s*s\s\d+\s\"(?P<ccn>\d+)\".*?Issuer\"\s*s\s\d+\s\"(?P<issuer>\w+)\".*?Expiry[_\s]Date\"\s*s\s\d+\s\"(?P<expiry>\d{2}\\\/\d{2})\".*?cvv\"\s*s\s\d+\s\"(?P<cvv>\d{3})\"'
    FLORIDA_SSN_PATTERN = r"\b(?!666|000|9\d{2})[2-7]\d{2}-(?!00)\d{2}-(?!0000)\d{4}\b"  # Florida SSNs (Florida area SSNs typically start with 261-267)
    FLORIDA_DL_PATTERN = r"\b[A-Z]{1}\d{12}\b"  # Florida Driver's License format (1 letter + 12 digits)

    # Context keywords for additional matching confidence
    CONTEXT_TERMS: List[str] = [
        "Florida", "FL", "credit card", "CCN", "SSN", "social security", "driver license", "DL", "track data",
        "expiry date", "card number", "cvv", "mastercard", "visa", "amex"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("Florida Credit Card Number", self.FLORIDA_CCN_PATTERN, 0.85),
            Pattern("Credit Card Track Data", self.TRACK_DATA_PATTERN, 0.95),
            Pattern("Florida SSN", self.FLORIDA_SSN_PATTERN, 0.9),
            Pattern("Florida Driver's License", self.FLORIDA_DL_PATTERN, 0.8)
        ]
        super().__init__(
            supported_entity="US_FLHB481",
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
# Bob and Jane Smith from Florida have the following information to report. Their CCN is 4316020000630490. 
# Bob's Florida Drivers License is D123456789123. The date is 2024-09-09. Their next of kin is James Smith 
# reporting their SSNs were 267-53-2093 and 260-56-4928. Thank you. 

# Track data: Name: "Angelo Rath", Credit_Card_Number: "5168570765333728", Issuer: "mastercard", Expiry_Date: "06/27", CVV: "806".
# """