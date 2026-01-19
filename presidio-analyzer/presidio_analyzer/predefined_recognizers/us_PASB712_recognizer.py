import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional

class PennsylvaniaRecognizer(PatternRecognizer):
    """
    Recognizer for detecting Pennsylvania-specific personal data including Credit Card Numbers (CCN),
    Credit Card Track Data, Social Security Numbers (SSNs), and Pennsylvania Driver's License numbers.
    """

    # Patterns for detecting Pennsylvania personal data
    PA_CREDIT_CARD_PATTERN = r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})(?:[-\s]?[0-9]{4})?\b"  # Detects CCNs with spaces/dashes
    PA_SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"  # Detects valid Pennsylvania SSNs
    PA_DRIVER_LICENSE_PATTERN = r"\b([0-9]{8})\b"  # Detects Pennsylvania Driver's License numbers (8 digits)
    PA_TRACK_DATA_PATTERN = r'Name\s*[:\-]?\s*(?P<name>[\w\s]+)\s*Credit\s*Card\s*Number\s*[:\-]?\s*(?P<ccn>\d{16})\s*Issuer\s*[:\-]?\s*(?P<issuer>[\w\s]+)\s*Expiry\s*Date\s*[:\-]?\s*(?P<expiry>\d{2}/\d{2})\s*CVV\s*[:\-]?\s*(?P<cvv>\d{3})'  # Credit Card Track Data
    
    # Context terms related to Pennsylvania-specific documents and personal data
    CONTEXT_TERMS: List[str] = [
        "Pennsylvania", "PA", "credit card", "card number", "payment card", "SSN", "social security", "driver's license", "DL", "track data", "account number"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("Pennsylvania Credit Card Number", self.PA_CREDIT_CARD_PATTERN, 0.85),
            Pattern("Pennsylvania SSN", self.PA_SSN_PATTERN, 0.9),
            Pattern("Pennsylvania Driver's License", self.PA_DRIVER_LICENSE_PATTERN, 0.8),
            Pattern("Pennsylvania Credit Card Track Data", self.PA_TRACK_DATA_PATTERN, 0.95)
        ]
        super().__init__(
            supported_entity="US_PASB712",
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

# Sample input text containing Pennsylvania-specific personal information
# text = """
# John Doe from Pennsylvania reported the following information. His Credit Card Number is 4316 0200 0063 0490. 
# John's SSN is 260-53-2093. His Pennsylvania Driver's License number is 12345678. Additionally, his track data 
# is as follows: Name: John Doe, Credit Card Number: 5168570765333728, Issuer: Mastercard, Expiry Date: 06/27, CVV: 806.
# """
