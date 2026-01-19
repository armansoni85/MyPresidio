import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional

class CASB1386Recognizer(PatternRecognizer):
    """
    Recognizer for detecting California CCN, CCN Track Data, California SSN, 
    and California Driver's License.
    """

    # Patterns for detecting California-specific personal data
    CCN_PATTERN = r"\b(?:4[0-9]{3}[-\s]?[0-9]{4}[-\s]?[0-9]{4}[-\s]?[0-9]{4}|5[1-5][0-9]{2}[-\s]?[0-9]{4}[-\s]?[0-9]{4}[-\s]?[0-9]{4})\b"  # Visa, Mastercard formats
    CCN_TRACK_DATA_PATTERN = r'Name\"\s*s\s\d+\s\"(?P<name>[\w\s\-]+)\".*?Credit_Card_Number\"\s*s\s\d+\s\"(?P<ccn>\d+)\".*?Issuer\"\s*s\s\d+\s\"(?P<issuer>\w+)\".*?Expiry[_\s]Date\"\s*s\s\d+\s\"(?P<expiry>\d{2}\\\/\d{2})\".*?cvv\"\s*s\s\d+\s\"(?P<cvv>\d{3})\"'
    CALIFORNIA_SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"
    CALIFORNIA_DL_PATTERN = r"\b[A-Z]{1}[0-9]{7}\b"  # Pattern for California DL, format: 1 letter followed by 7 digits

    # Context terms related to CCN, SSN, and driver's license
    CONTEXT_TERMS: List[str] = [
        "credit card", "card number", "ccn", "payment card",
        "social security", "ssn", "driver's license", "dl"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("California Credit Card Number", self.CCN_PATTERN, 0.85),
            Pattern("California CCN Track Data", self.CCN_TRACK_DATA_PATTERN, 0.95),
            Pattern("California SSN", self.CALIFORNIA_SSN_PATTERN, 0.9),
            Pattern("California Driver's License", self.CALIFORNIA_DL_PATTERN, 0.8)
        ]
        super().__init__(
            supported_entity="US_CASB1386",
            patterns=patterns,
            context=self.CONTEXT_TERMS,
            supported_language=supported_language
        )

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        """
        Analyze method to detect California personal data in text.
        """
        # Pass nlp_artifacts to the superclass analyze method
        results = super().analyze(text, entities, nlp_artifacts=nlp_artifacts)

        # Validate results with custom validation logic (e.g., Luhn validation for CCN)
        valid_results = []
        for result in results:
            if result.entity_type == "California Credit Card Number" and self.validate_ccn(text[result.start:result.end]):
                valid_results.append(result)
            elif result.entity_type == "California CCN Track Data" and self.validate_ccn(result.entity_value):
                valid_results.append(result)
            elif result.entity_type in ["California SSN", "California Driver's License"]:
                valid_results.append(result)

        return valid_results


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
# Bob and Jane Smith have the following information to report. 
# Their California CCN is 4316020000630490. 
# Their California Driver's License is A1234567. 
# Their SSNs were 123-45-6789 and 123-45-9876. 
# Additionally, they have a track data record: 
# Name: "John Doe", Credit_Card_Number: "4111111111111111", Issuer: "Visa", Expiry_Date: "12/25", CVV: "123".
# """