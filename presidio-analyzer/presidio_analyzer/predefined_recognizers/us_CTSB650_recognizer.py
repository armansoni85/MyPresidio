import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional

class ConnecticutDLPRecognizer(PatternRecognizer):
    """
    Recognizer for detecting Connecticut-based Credit Card Numbers (CCNs),
    Credit Card Track Data, SSNs, and Connecticut Driver's License Numbers.
    """

    # Regular expression patterns for different sensitive data types
    CT_CREDIT_CARD_PATTERN = r"\b(?:(?:card\s*number|credit\s*card|ccn|account\s*number|payment\s*card)\s*[:-]?\s*)?(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})(?:\s*(?:number|credit\s*card|ccn|payment\s*card))?\b"
    CT_SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"
    CT_DRIVER_LICENSE_PATTERN = r"\b(?:[0-9]{9})\b"
    TRACK_DATA_PATTERN = r'Name\"\s*s\s\d+\s\"(?P<name>[\w\s\-]+)\".*?Credit_Card_Number\"\s*s\s\d+\s\"(?P<ccn>\d+)\".*?Expiry[_\s]Date\"\s*s\s\d+\s\"(?P<expiry>\d{2}\\\/\d{2})\".*?cvv\"\s*s\s\d+\s\"(?P<cvv>\d{3})\"'

    # Context terms for improving recognition accuracy
    CONTEXT_TERMS: List[str] = [
        "connecticut", "credit card", "card number", "ccn", "payment card", "ssn",
        "social security", "driver's license", "dl", "track data", "cvv", "expiry date"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("Connecticut Credit Card Number", self.CT_CREDIT_CARD_PATTERN, 0.85),
            Pattern("Connecticut SSN", self.CT_SSN_PATTERN, 0.9),
            Pattern("Connecticut Driver's License", self.CT_DRIVER_LICENSE_PATTERN, 0.8),
            Pattern("CCN Track Data", self.TRACK_DATA_PATTERN, 0.95)
        ]
        super().__init__(
            supported_entity="US_CTSB650",
            patterns=patterns,
            context=self.CONTEXT_TERMS,
            supported_language=supported_language
        )

    def analyze(self, text: str, entities: List[str], **kwargs) -> List[RecognizerResult]:
        """
        Analyze method to detect Connecticut personal data in text.
        Validate credit card numbers using the Luhn algorithm.
        """
        # Get the results from the parent class's analyze method
        results: List[RecognizerResult] = super().analyze(text=text, entities=entities, **kwargs)

        # Validate results
        valid_results = []
        for result in results:
            if result.entity_type == "Connecticut Credit Card Number":
                # Validate the detected credit card number using the Luhn algorithm
                ccn = text[result.start:result.end]
                if self.validate_ccn(ccn):
                    valid_results.append(result)
            else:
                valid_results.append(result)  # For other patterns, no validation logic is applied

        return valid_results

    @staticmethod
    def validate_ccn(ccn: str) -> bool:
        """
        Validate a credit card number using the Luhn algorithm.
        """
        def digits_of(n):
            return [int(d) for d in str(n)]
        digits = digits_of(ccn)
        odd_digits = digits[-1::-2]
        even_digits = digits[-2::-2]
        checksum = sum(odd_digits)
        for d in even_digits:
            checksum += sum(digits_of(d * 2))
        return checksum % 10 == 0
