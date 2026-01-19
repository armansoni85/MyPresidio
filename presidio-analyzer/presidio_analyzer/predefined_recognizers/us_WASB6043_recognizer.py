import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional

class WashingtonStateRecognizer(PatternRecognizer):
    """
    Recognizes Washington-specific sensitive data including:
    - All US Social Security Numbers (SSN)
    - All credit card numbers (with Luhn validation)
    - Washington Driver's License numbers (both regular and enhanced formats)
    - Credit Card Track Data
    """

    # Enhanced patterns for comprehensive detection
    CREDIT_CARD_PATTERN = r"\b(?:4[0-9]{3}(?:[-\s]?[0-9]{4}){3}|5[1-5][0-9]{2}(?:[-\s]?[0-9]{4}){3}|3[47][0-9]{2}[-\s]?[0-9]{6}[-\s]?[0-9]{5}|6(?:011|5[0-9]{2})(?:[-\s]?[0-9]{4}){3}|(?:2131|1800|35\d{3})\d{11})\b"
    CCN_TRACK_DATA_PATTERN = r"(?:%?[Bb]?\d{1,19}\^[^\^]{2,26}\^(?:\d{7}|\d{4}\^\d{3})[^?]+\??|;\d{1,19}=\d{4}\d{3}[^?]+\??)"
    SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}[-\s]?(?!00)\d{2}[-\s]?(?!0000)\d{4}\b"
    WA_DL_ENHANCED_PATTERN = r"\bWDL[A-Z0-9]{9}\b"
    WA_DL_REGULAR_PATTERN = r"\b[A-Z]{1,7}\*?[A-Z]?\d{4,9}\b"

    # Comprehensive context terms covering all entities
    CONTEXT_TERMS: List[str] = [
        # SSN context
        "social security", "SSN", "SS#", "SSN#", "social", "security number", 
        "tax ID", "tax identification", "TIN", "social insurance",
        
        # Credit card context
        "credit card", "card number", "CCN", "CC#", "account number", 
        "payment card", "bank card", "visa", "mastercard", "amex", "discover",
        "cardholder", "card holder", "expiration", "expiry", "exp date", 
        "valid thru", "valid through", "cvv", "cvv2", "cid", "csc", "ccv",
        
        # Track data context
        "track data", "track1", "track2", "track 1", "track 2", 
        "payment track", "card track", "magnetic stripe", 
        
        # Driver's license context
        "driver's license", "drivers license", "driver license", "DL#", 
        "license number", "Washington DL", "WA DL", "WDL", "DOL", 
        "department of licensing", "driver id", "driver identification",
        "photo license", "state ID", "state identification",
        
        # Washington-specific context
        "Washington", "WA", "Washington state", "Seattle", "Spokane", "Tacoma",
        "Vancouver", "Bellevue", "Olympia", "Evergreen State", "WA DOL",
        "King County", "Pierce County", "Snohomish County", "Spokane County",
        
        # General financial context
        "payment", "billing", "account", "transaction", "finance", "financial",
        "banking", "bank", "debit", "credit", "charge card", "card payment"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("Credit Card Number", self.CREDIT_CARD_PATTERN, 0.85),
            Pattern("CCN Track Data", self.CCN_TRACK_DATA_PATTERN, 0.95),
            Pattern("SSN", self.SSN_PATTERN, 0.9),
            Pattern("WA Enhanced Driver's License", self.WA_DL_ENHANCED_PATTERN, 0.8),
            Pattern("WA Regular Driver's License", self.WA_DL_REGULAR_PATTERN, 0.7)
        ]
        super().__init__(
            supported_entity="US_WASB6043",
            patterns=patterns,
            context=self.CONTEXT_TERMS,
            supported_language=supported_language
        )

    def validate(self, pattern: Pattern, text: str) -> Optional[RecognizerResult]:
        """
        Validate pattern matches with entity-specific checks
        """
        # First get base result from parent class
        base_result = super().validate(pattern, text)
        if not base_result:
            return None

        # Extract matched text
        matched_text = text[base_result.start:base_result.end]

        # Entity-specific validation
        if pattern.name == "Credit Card Number":
            if not self._validate_ccn(matched_text):
                return None
        elif pattern.name == "SSN":
            if not self._validate_ssn(matched_text):
                return None

        # Return the validated result
        return base_result

    def _validate_ccn(self, ccn: str) -> bool:
        """Validate credit card number using Luhn algorithm"""
        digits = [int(d) for d in re.sub(r"\D", "", ccn)]
        if len(digits) < 13 or len(digits) > 19:
            return False
            
        checksum = 0
        parity = len(digits) % 2
        for i, digit in enumerate(digits):
            if i % 2 == parity:
                digit *= 2
                if digit > 9:
                    digit -= 9
            checksum += digit
        return checksum % 10 == 0

    def _validate_ssn(self, ssn: str) -> bool:
        """Validate SSN by checking reserved ranges"""
        digits = re.sub(r"\D", "", ssn)
        if len(digits) != 9:
            return False
            
        area = int(digits[0:3])
        group = int(digits[3:5])
        serial = int(digits[5:9])
        
        # Validate ranges (SSA rules)
        if area == 0 or area == 666 or (900 <= area <= 999):
            return False
        if group == 0 or group > 99:
            return False
        if serial == 0 or serial > 9999:
            return False
        return True
