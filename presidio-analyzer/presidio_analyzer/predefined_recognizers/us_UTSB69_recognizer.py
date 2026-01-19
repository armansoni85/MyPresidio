import re
from presidio_analyzer import Pattern, PatternRecognizer
from typing import List, Optional

class UtahPolicyRecognizer(PatternRecognizer):
    """
    Recognizer for detecting Utah-specific personal data including:
    - Credit Card Number (CCN)
    - CCN Track Data
    - Social Security Number (SSN)
    - Utah Driver's License Number
    """

    # Regex patterns for detecting Utah-specific data
    CREDIT_CARD_PATTERN = r"\b(?:(?:card\s*number|credit\s*card|ccn|account\s*number|payment\s*card)\s*[:-]?\s*)?(?:4[0-9]{3}[-\s]?[0-9]{4}[-\s]?[0-9]{4}[-\s]?[0-9]{4}|5[1-5][0-9]{2}[-\s]?[0-9]{4}[-\s]?[0-9]{4}[-\s]?[0-9]{4}|3[47][0-9]{2}[-\s]?[0-9]{6}[-\s]?[0-9]{5}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})(?:\s*(?:number|credit\s*card|ccn|payment\s*card))?\b"
    CCN_TRACK_DATA_PATTERN = r"Name\"\s*s\s\d+\s\"(?P<name>[\w\s\-]+)\".*?Credit_Card_Number\"\s*s\s\d+\s\"(?P<ccn>\d+)\".*?Issuer\"\s*s\s\d+\s\"(?P<issuer>\w+)\".*?Expiry[_\s]Date\"\s*s\s\d+\s\"(?P<expiry>\d{2}\\\/\d{2})\".*?cvv\"\s*s\s\d+\s\"(?P<cvv>\d{3})\""
    SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"  # U.S. SSN format
    UTAH_DL_PATTERN = r"\b[A-Z]{1}[0-9]{7}\b"  # Example pattern for Utah driver's license (starts with a letter, followed by 7 digits)
    
    # Contextual keywords relevant to Utah data
    CONTEXT_TERMS: List[str] = [
        "utah", "credit card", "social security", "driver's license", "dl", "ccn", "ssn", "track data", "payment card"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("Credit Card Number", self.CREDIT_CARD_PATTERN, 0.85),
            Pattern("CCN Track Data", self.CCN_TRACK_DATA_PATTERN, 0.9),
            Pattern("Utah SSN", self.SSN_PATTERN, 0.9),
            Pattern("Utah Driver's License", self.UTAH_DL_PATTERN, 0.85)
        ]
        super().__init__(
            supported_entity="US_UTSB69",
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
# Bob and Jane Smith have the following information to report. Their CCN is 4316-0200-0063-0490. 
# Their SSNs were 260-53-2093 and 260-56-4928. Jane's Utah driver's license is A1234567. 
# Additionally, CCN track data: Name "Bob", Credit_Card_Number "4111111111111111", Issuer "Visa", Expiry_Date "12/25", CVV "123".
# """
