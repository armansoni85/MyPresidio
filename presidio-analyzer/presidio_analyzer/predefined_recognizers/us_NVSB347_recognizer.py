import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional

class NevadaRecognizer(PatternRecognizer):
    """
    Recognizer for detecting Nevada-specific personal data including Credit Card Numbers (CCN),
    Credit Card Track Data, Social Security Numbers (SSN), and Nevada Driver's License Numbers.
    """

    # Patterns for detecting Nevada-based personal data
    NEVADA_CREDIT_CARD_PATTERN = r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})\b"
    NEVADA_SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"
    NEVADA_DL_PATTERN = r"(?i)\bNevada\s*Driver'?s?\s*License\s*:?\s*([A-Z0-9]{7,12})\b"
    NEVADA_CC_TRACK_DATA_PATTERN = r'(?i)Name\s*:\s*"([^"]+)"\s*CCN\s*:\s*"(\d+)"\s*Expiry\s*:\s*"(\d{2}/\d{2})"\s*CVV\s*:\s*"(\d{3,4})"'

    # Context keywords to improve detection accuracy
    CONTEXT_TERMS: List[str] = [
        "credit card", "ccn", "account number", "payment card",
        "social security", "ssn", "driver's license", "nevada",
        "nv dl", "nevada driver license", "nevada dl", "cc track data"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("Nevada Credit Card Number", self.NEVADA_CREDIT_CARD_PATTERN, 0.75),
            Pattern("Nevada SSN", self.NEVADA_SSN_PATTERN, 0.75),
            Pattern("Nevada Driver's License", self.NEVADA_DL_PATTERN, 0.75),
            Pattern("Nevada Credit Card Track Data", self.NEVADA_CC_TRACK_DATA_PATTERN, 0.75)
        ]
        super().__init__(
            supported_entity="US_NVSB347",
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

        cleaned_ccn = re.sub(r"\D", "", ccn)
        return cleaned_ccn.isdigit() and luhn_checksum(cleaned_ccn)

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        """
        Overridden analyze method to:
          1. Get candidate results from the parent class
          2. Validate CCN and Track Data results
          3. Boost confidence to 1.0 if context is present
        """
        # Get results using parent class logic
        results = super().analyze(text, entities, nlp_artifacts)
        if not results:
            return []
        
        # Validate results that require additional checks
        valid_results = []
        for result in results:
            matched_text = text[result.start:result.end]
            
            # Skip validation for entities that don't require it
            if not self._needs_validation(matched_text):
                valid_results.append(result)
                continue
                
            # Validate credit card numbers
            if re.fullmatch(self.NEVADA_CREDIT_CARD_PATTERN, matched_text):
                if self.validate_ccn(matched_text):
                    valid_results.append(result)
                    
            # Validate track data
            elif re.fullmatch(self.NEVADA_CC_TRACK_DATA_PATTERN, matched_text, re.IGNORECASE):
                match = re.search(self.NEVADA_CC_TRACK_DATA_PATTERN, matched_text, re.IGNORECASE)
                if match and match.group(2) and self.validate_ccn(match.group(2)):
                    valid_results.append(result)
                    
            # For SSN and Driver's License, just add without validation
            else:
                valid_results.append(result)

        # Check for context presence
        text_lower = text.lower()
        context_found = any(context_term in text_lower for context_term in self.CONTEXT_TERMS)
        
        # Boost score to 1.0 if context is present
        if context_found:
            for result in valid_results:
                result.score = 1.0
        
        return valid_results

    def _needs_validation(self, text: str) -> bool:
        """Check if text requires validation (CCN or Track Data)"""
        return (re.fullmatch(self.NEVADA_CREDIT_CARD_PATTERN, text) or
                re.fullmatch(self.NEVADA_CC_TRACK_DATA_PATTERN, text, re.IGNORECASE))
