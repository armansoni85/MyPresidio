import re
from presidio_analyzer import Pattern, PatternRecognizer
from typing import List, Optional

class LouisianaRecognizer(PatternRecognizer):
    """
    Recognizer for detecting Louisiana-based personal data including Credit Card Track Data,
    Credit Card Numbers, Social Security Numbers, and Louisiana Driver's License Numbers.
    """

    # Patterns for detecting Louisiana personal data
    CCN_PATTERN = r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})(?:[-\s]?[0-9]{4})?\b"  # Detects credit card numbers with spaces/dashes
    TRACK_DATA_PATTERN = r'Name"\s*\d+"\s*"(?P<name>[\w\s]+)".*?Credit_Card_Number"\s*\d+"\s*"(?P<ccn>\d+)".*?Issuer"\s*\d+"\s*"(?P<issuer>\w+)".*?Expiry_Date"\s*\d+"\s*"(?P<expiry>\d{2}/\d{2})".*?cvv"\s*\d+"\s*"(?P<cvv>\d{3})"'  # Detects track data for CCNs
    SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"  # Detects valid SSNs
    LA_DL_PATTERN = r"\b[0-9]{1}\d{2}-\d{3}-\d{4}\b"  # Louisiana driver's license pattern (assumed format)

    # Context keywords for boosting confidence
    CONTEXT_TERMS: List[str] = [
        "credit card", "ccn", "payment card", "SSN", "social security number",
        "Louisiana driver's license", "LA DL", "Louisiana DL", "Louisiana ID", "LASB205"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("Louisiana Credit Card Number", self.CCN_PATTERN, 0.85),
            Pattern("Louisiana Credit Card Track Data", self.TRACK_DATA_PATTERN, 0.95),
            Pattern("Louisiana SSN", self.SSN_PATTERN, 0.9),
            Pattern("Louisiana Driver's License", self.LA_DL_PATTERN, 0.8)
        ]
        super().__init__(
            supported_entity="US_LASB205",
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


# Example input text
# text = """
# Bob's Louisiana CCN is 4316-0200-0063-0490. His Louisiana Driver's License is 123-456-7890.
# James Smith reported their SSNs as 123-45-6789 and 260-53-2093.
# Additionally, the track data includes: Name: "John Doe", Credit_Card_Number: "5500005555555559", 
# Issuer: "MasterCard", Expiry_Date: "12/25", CVV: "123".
# """