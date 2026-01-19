import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional

class US_AZSB1338Recognizer(PatternRecognizer):
    """
    Recognizer for detecting U.S. based personal data including Credit Card Track Data,
    Credit Card Numbers, Social Security Numbers, and Arizona Driver's License Numbers in text.
    """

    # Updated patterns for detecting U.S. personal data
    CREDIT_CARD_PATTERN = r"\b(?:(?:card\s*number|credit\s*card|ccn|account\s*number|payment\s*card)\s*[:-]?\s*)?(?:4[0-9]{3}[-\s]?[0-9]{4}[-\s]?[0-9]{4}[-\s]?[0-9]{4}|5[1-5][0-9]{2}[-\s]?[0-9]{4}[-\s]?[0-9]{4}[-\s]?[0-9]{4}|3[47][0-9]{2}[-\s]?[0-9]{6}[-\s]?[0-9]{5}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})(?:\s*(?:number|credit\s*card|ccn|payment\s*card))?\b"  # Detects credit card numbers with spaces/dashes
    SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"  # Detects valid SSNs
    ARIZONA_DL_PATTERN = r"\b[A-Z0-9]{9}\b"  # Simplified pattern for Arizona Driver's License
    
    # Common context terms related to credit card details
    CONTEXT_TERMS: List[str] = [
        "credit card", "card number", "expiry date", "cvv",
        "social security", "ssn", "driver's license", "dl"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("Credit Card Number", self.CREDIT_CARD_PATTERN, 0.85),
            Pattern("SSN", self.SSN_PATTERN, 0.9),
            Pattern("Arizona Driver's License", self.ARIZONA_DL_PATTERN, 0.8)
        ]
        super().__init__(
            supported_entity="US_AZSB1338",
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
# Bob and Jane Smith have the following information to report. Their CCN is 4316020000630490. 
# Bobs Georgia Drivers License is 056698494. The date is 2024-09-09. Their next of kin is James Smith 
# reporting their SSNs were 260-53-2093 and 260-56-4928. Thank you. 

# Bob and Jane Smith have the following information to report. Their CCN is 5500005555555559. 
# Bobs Georgia Drivers License is 056698494. The date is 2024-09-09. Their next of kin is James Smith 
# reporting their SSNs were 260-53-2093 and 260-56-4928. Thank you. 

# Bob and Jane Smith have the following information to report. Their CCN is 371449635398431. 
# Bobs Georgia Drivers License is 056698494. The date is 2024-09-09. Their next of kin is James Smith 
# reporting their SSNs were 260-53-2093 and 260-56-4928. Thank you.
# """
