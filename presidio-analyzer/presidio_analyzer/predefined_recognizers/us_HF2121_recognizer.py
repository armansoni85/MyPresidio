import re
from presidio_analyzer import Pattern, PatternRecognizer
from typing import List, Optional

class MinnesotaRecognizer(PatternRecognizer):
    """
    Recognizer for detecting Minnesota-based personal data such as Credit Card Number (CCN),
    Credit Card Track Data, Social Security Numbers (SSN), and Minnesota Driver's License Numbers.
    """

    # Regular expressions for detecting Minnesota CCNs, SSNs, and Driver's License
    MINNESOTA_CCN_PATTERN = r"\b(?:4[0-9]{3}[-\s]?[0-9]{4}[-\s]?[0-9]{4}[-\s]?[0-9]{4}|5[1-5][0-9]{2}[-\s]?[0-9]{4}[-\s]?[0-9]{4}[-\s]?[0-9]{4}|3[47][0-9]{2}[-\s]?[0-9]{6}[-\s]?[0-9]{5})\b"  # Visa, MasterCard, Amex formats
    MINNESOTA_SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"  # Valid SSNs
    MINNESOTA_DL_PATTERN = r"\b[M][0-9]{12}\b"  # Minnesota driver's license pattern starts with "M" followed by 12 digits
    CCN_TRACK_DATA_PATTERN = r'Name\"\s*s\s\d+\s\"(?P<name>[\w\s\-]+)\".*?Credit_Card_Number\"\s*s\s\d+\s\"(?P<ccn>\d+)\".*?Expiry[_\s]Date\"\s*s\s\d+\s\"(?P<expiry>\d{2}\\\/\d{2})\".*?cvv\"\s*s\s\d+\s\"(?P<cvv>\d{3})\"'

    # Context keywords that strengthen the matching of Minnesota-specific terms
    CONTEXT_TERMS: List[str] = [
        "Minnesota", "MN", "driver's license", "DL", "credit card", "card number", 
        "CCN", "track data", "social security", "SSN"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("Minnesota CCN", self.MINNESOTA_CCN_PATTERN, 0.85),
            Pattern("Minnesota SSN", self.MINNESOTA_SSN_PATTERN, 0.9),
            Pattern("Minnesota Driver's License", self.MINNESOTA_DL_PATTERN, 0.9),
            Pattern("CCN Track Data", self.CCN_TRACK_DATA_PATTERN, 0.95)
        ]
        super().__init__(
            supported_entity="US_HF2121",
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
# John Doe has a Minnesota driver's license: M123456789012. 
# His credit card number is 4316 0200 0063 0490 and social security number is 260-53-2093.
# Minnesota CCN track data: Name "John Doe" Credit_Card_Number "5500005555555559" Expiry_Date "12/24" cvv "123".
# """