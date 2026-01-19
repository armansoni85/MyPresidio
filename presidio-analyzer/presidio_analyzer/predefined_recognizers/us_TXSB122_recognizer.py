import re
from presidio_analyzer import Pattern, PatternRecognizer
from typing import List, Optional

class TexasPolicyRecognizer(PatternRecognizer):
    """
    Recognizer for detecting Texas specific personal data including:
    - Credit Card Numbers (CCN)
    - Credit Card Track Data
    - Texas Social Security Numbers (SSNs)
    - Texas Driver's License Numbers
    """

    # Regular expressions to detect sensitive data related to Texas
    TEXAS_CREDIT_CARD_PATTERN = r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})(?:[-\s]?[0-9]{4})?\b"  # Visa, Mastercard, Amex, Discover
    TEXAS_SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"  # Valid SSN format
    TEXAS_DRIVER_LICENSE_PATTERN = r"\b\d{8,9}\b"  # Texas driver's license is typically 8 or 9 digits
    TEXAS_CCN_TRACK_DATA_PATTERN = r'Name\"\s*s\s\d+\s\"(?P<name>[\w\s\-]+)\".*?Credit_Card_Number\"\s*s\s\d+\s\"(?P<ccn>\d+)\".*?Issuer\"\s*s\s\d+\s\"(?P<issuer>\w+)\".*?Expiry[_\s]Date\"\s*s\s\d+\s\"(?P<expiry>\d{2}\\\/\d{2})\".*?cvv\"\s*s\s\d+\s\"(?P<cvv>\d{3})\"'  # Example pattern for Track Data

    # Context keywords to boost confidence of detection
    CONTEXT_TERMS: List[str] = [
        "Texas", "TX", "CCN", "credit card", "credit card number", "SSN", "Social Security Number", 
        "driver's license", "DL", "expiry date", "cvv", "track data"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("Texas Credit Card Number", self.TEXAS_CREDIT_CARD_PATTERN, 0.85),
            Pattern("Texas SSN", self.TEXAS_SSN_PATTERN, 0.9),
            Pattern("Texas Driver's License", self.TEXAS_DRIVER_LICENSE_PATTERN, 0.8),
            Pattern("Texas Credit Card Track Data", self.TEXAS_CCN_TRACK_DATA_PATTERN, 0.95)
        ]
        super().__init__(
            supported_entity="US_TXSB122",
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

# Sample input text to test the policy
# text = """
# John Doe from Texas has the following information. His CCN is 4111-1111-1111-1111. His SSN is 123-45-6789.
# John's Texas driver's license number is 12345678. There is also some credit card track data:
# Name: "John Doe", Credit_Card_Number: "4111111111111111", Issuer: "Visa", Expiry_Date: "12/25", CVV: "123".
# """
