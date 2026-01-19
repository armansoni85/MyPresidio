import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from presidio_analyzer.nlp_engine import NlpArtifacts
from typing import List, Optional, Dict, Any

class CCTrackDataRecognizer(PatternRecognizer):
    """
    Recognizer for detecting Credit Card Track Data (Track-1 and Track-2) in text.
    """

    # Credit Card Number (CCN) pattern
    CREDIT_CARD_PATTERN = r"\b(?:\d[ -]*?){13,19}\b"  # More flexible pattern

    # Expiry Date pattern (MM/YY or MM/YYYY)
    EXPIRY_DATE_PATTERN = r"\b(0[1-9]|1[0-2])[/-]?(\d{2}|\d{4})\b"

    # Service code pattern (3 digits)
    SERVICE_CODE_PATTERN = r"\b\d{3}\b"

    # Track data sentinels
    TRACK_1_SENTINEL = r"%B"
    TRACK_2_SENTINEL = r";"

    # Enhanced context terms
    CONTEXT_TERMS: List[str] = [
        "credit card", "card number", "expiry date", "cvv", "cvc", 
        "track-1", "track-2", "track-3", "track data", "magnetic stripe", 
        "magstripe", "cardholder", "pan", "primary account number", 
        "expiration date", "expiry", "discretionary data", "service code", 
        "name on card", "cardholder name", "ccn", "credit card number", 
        "expiring", "expires", "valid thru", "valid through", "valid from",
        "card verification", "security code", "track data", "magstripe data",
        "card number", "card no", "card #", "creditcard", "debit card",
        "payment card", "bank card", "card details", "card information",
        "card expiry", "card exp", "card expiration", "valid until"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("CREDIT_CARD", self.CREDIT_CARD_PATTERN, 0.5),
            Pattern("EXPIRY_DATE", self.EXPIRY_DATE_PATTERN, 0.8),
            Pattern("SERVICE_CODE", self.SERVICE_CODE_PATTERN, 0.7),
            Pattern("TRACK_1_SENTINEL", self.TRACK_1_SENTINEL, 0.9),
            Pattern("TRACK_2_SENTINEL", self.TRACK_2_SENTINEL, 0.9)
        ]
        super().__init__(
            supported_entity="CREDIT_CARD_TRACK_DATA",
            patterns=patterns,
            context=None,  # Remove context from base class
            supported_language=supported_language
        )
        # Context handling parameters
        self.context_boost = 0.2
        self.min_score_with_context = 0.7
        self.context_window = 50  # Characters around match to look for context

    def analyze(
        self, 
        text: str, 
        entities: List[str], 
        nlp_artifacts: Optional[NlpArtifacts] = None
    ) -> List[RecognizerResult]:
        # Detect base patterns
        results = super().analyze(text, entities, nlp_artifacts)
        
        # Add PERSON entities from NLP
        if nlp_artifacts and nlp_artifacts.entities:
            for entity in nlp_artifacts.entities:
                if entity.label_ == "PERSON":
                    results.append(
                        RecognizerResult(
                            entity_type="PERSON",
                            start=entity.start_char,
                            end=entity.end_char,
                            score=0.85
                        )
                    )
        
        # Add context terms as hits
        context_results = self._detect_context_terms(text)
        results.extend(context_results)
        
        # Count valid CCNs and adjust scores
        valid_ccns = self._count_valid_ccns(text, results)
        valid_results = []
        track_data_count = 0
        
        # Apply context-based scoring boost
        for result in results:
            if self.custom_validate_result(result, text):
                # Boost scores based on surrounding context
                if result.entity_type in ["CREDIT_CARD", "EXPIRY_DATE", "SERVICE_CODE"]:
                    context_found = self._check_context(text, result)
                    if context_found:
                        result.score = max(result.score, self.min_score_with_context)
                
                # Additional boost for multiple valid cards
                if result.entity_type == "CREDIT_CARD" and valid_ccns > 1:
                    result.score = min(1.0, result.score + 0.5 * (valid_ccns - 1))
                
                valid_results.append(result)
                if result.entity_type in [
                    "CREDIT_CARD", "EXPIRY_DATE", "SERVICE_CODE", 
                    "PERSON", "CREDIT_CARD_CONTEXT"
                ]:
                    track_data_count += 1
        
        return valid_results if track_data_count >= 3 else []

    def _detect_context_terms(self, text: str) -> List[RecognizerResult]:
        """Detect context terms as separate hits"""
        results = []
        for term in self.CONTEXT_TERMS:
            # Case-insensitive search with word boundaries
            pattern = r"\b" + re.escape(term) + r"\b"
            for match in re.finditer(pattern, text, re.IGNORECASE):
                results.append(
                    RecognizerResult(
                        entity_type="CREDIT_CARD_CONTEXT",
                        start=match.start(),
                        end=match.end(),
                        score=0.5
                    )
                )
        return results

    def _check_context(self, text: str, result: RecognizerResult) -> bool:
        """Check if context terms appear near a match"""
        start = max(0, result.start - self.context_window)
        end = min(len(text), result.end + self.context_window)
        context_area = text[start:end]
        
        for term in self.CONTEXT_TERMS:
            if re.search(r"\b" + re.escape(term) + r"\b", context_area, re.IGNORECASE):
                return True
        return False

    def _count_valid_ccns(self, text: str, results: List[RecognizerResult]) -> int:
        """Count valid credit card numbers in results"""
        valid_count = 0
        for result in results:
            if result.entity_type == "CREDIT_CARD":
                ccn = text[result.start:result.end]
                if self.validate_ccn(ccn):
                    valid_count += 1
        return valid_count

    def validate_ccn(self, ccn: str) -> bool:
        """Validate credit card number using Luhn algorithm"""
        def luhn_checksum(card_number: str) -> bool:
            digits = [int(d) for d in card_number if d.isdigit()]
            if not digits:
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

        cleaned_ccn = re.sub(r"\D", "", ccn)
        return len(cleaned_ccn) >= 13 and luhn_checksum(cleaned_ccn)

    def custom_validate_result(self, result: RecognizerResult, text: str) -> bool:
        """Custom validation for different track data components"""
        matched_text = text[result.start:result.end]
        
        if result.entity_type == "CREDIT_CARD":
            return self.validate_ccn(matched_text)
            
        elif result.entity_type == "EXPIRY_DATE":
            # Validate expiry date format
            return bool(re.fullmatch(self.EXPIRY_DATE_PATTERN, matched_text))
            
        elif result.entity_type == "SERVICE_CODE":
            # Validate service code is 3 digits
            return bool(re.fullmatch(r"\d{3}", matched_text))
            
        return True  # For other entity types (PERSON, CONTEXT, SENTINELS)
