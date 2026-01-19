import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional

class MassachusettsDataRecognizer(PatternRecognizer):
    """
    Recognizer for detecting Massachusetts specific personal data including Credit Card Track Data,
    Credit Card Numbers, Social Security Numbers (SSNs), and Driver's License Numbers.
    """

    # Patterns for detecting personal data
    CREDIT_CARD_PATTERN = r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})(?:[-\s]?[0-9]{4})?\b"  # Detects CCNs including spaces/dashes
    TRACK_DATA_PATTERN = r'Name\"\s*s\s\d+\s\"(?P<name>[\w\s\-]+)\".*?Credit_Card_Number\"\s*s\s\d+\s\"(?P<ccn>\d+)\".*?Issuer\"\s*s\s\d+\s\"(?P<issuer>\w+)\".*?Expiry[_\s]Date\"\s*s\s\d+\s\"(?P<expiry>\d{2}\\\/\d{2})\".*?cvv\"\s*s\s\d+\s\"(?P<cvv>\d{3})\"'
    SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"  # Detects SSNs
    MA_DRIVER_LICENSE_PATTERN = r"\b(?<!\d)(S|S\d)\d{8}\b"  # Massachusetts DL pattern example

    # Context keywords for improved matching accuracy
    CONTEXT_TERMS: List[str] = [
        "credit card", "ccn", "track data", "payment", "expiry date", "cvv", 
        "ssn", "social security number", "driver's license", "license number", 
        "massachusetts"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("Massachusetts Credit Card Number", self.CREDIT_CARD_PATTERN, 0.85),
            Pattern("Massachusetts CCN Track Data", self.TRACK_DATA_PATTERN, 0.95),
            Pattern("Massachusetts SSN", self.SSN_PATTERN, 0.9),
            Pattern("Massachusetts Driver's License", self.MA_DRIVER_LICENSE_PATTERN, 0.8)
        ]
        super().__init__(
            supported_entity="US_MASS201",
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

# Sample text input to test the recognizer
# text = """
# John Doe from Massachusetts reports his Credit Card Number is 4316-0200-0063-0490. 
# Additionally, his Massachusetts SSN is 123-45-6789 and his driver's license number is S12345678. 
# Another record shows Credit Card Track Data: Name: John Doe, Credit_Card_Number: 5500005555555559, Issuer: mastercard, Expiry_Date: 12/25, CVV: 123.
# """
