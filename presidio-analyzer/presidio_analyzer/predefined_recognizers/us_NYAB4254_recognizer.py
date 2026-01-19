import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional

class NewYorkDataRecognizer(PatternRecognizer):
    """
    Recognizer for detecting New York-based personal data including:
    - Credit Card Number (CCN)
    - Credit Card Track Data
    - Social Security Number (SSN)
    - New York Driver's License Number
    """

    # Patterns for detecting personal data
    NY_CREDIT_CARD_PATTERN = r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})\b"  # Credit card numbers
    NY_SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"  # Social Security Number
    NY_DRIVER_LICENSE_PATTERN = r"\b[A-Z]\d{7}\b"  # Simplified pattern for New York Driver's License (a letter followed by 7 digits)
    NY_CC_TRACK_DATA_PATTERN = r'Name"\s*\d+"(?P<name>[\w\s\-]+)"\s*Credit_Card_Number"\s*\d+"(?P<ccn>\d+)"\s*Expiry_Date"\s*\d+"(?P<expiry>\d{2}/\d{2})"\s*CVV"\s*\d+"(?P<cvv>\d{3})"'  # Credit Card Track Data
    
    # Context terms related to New York
    CONTEXT_TERMS: List[str] = [
        "New York", "NY", "NYC", "Albany", "Buffalo", "Manhattan",
        "New York driver's license", "NY driver's license", "New York SSN",
        "New York credit card", "NY CCN", "NY Track Data"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("New York Credit Card Number", self.NY_CREDIT_CARD_PATTERN, 0.85),
            Pattern("New York SSN", self.NY_SSN_PATTERN, 0.9),
            Pattern("New York Driver's License", self.NY_DRIVER_LICENSE_PATTERN, 0.8),
            Pattern("New York Credit Card Track Data", self.NY_CC_TRACK_DATA_PATTERN, 0.95),
        ]
        super().__init__(
            supported_entity="US_NYAB4254",
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


# Sample input text that includes New York-related sensitive data
# text = """
# John Doe lives in New York. His credit card number is 4111-1111-1111-1111. 
# His SSN is 123-45-6789 and his New York driver's license number is N1234567.
# Additionally, there is some credit card track data: 
# Name: "Jane Doe", Credit_Card_Number: "5555555555554444", Expiry_Date: "12/25", CVV: "123".
# """
