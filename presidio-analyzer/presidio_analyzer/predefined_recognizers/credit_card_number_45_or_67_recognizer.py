from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class CreditCardRecognizer(PatternRecognizer):
    
    # Improved patterns for better coverage
    PATTERNS = [
        Pattern(
            "Credit Card - Formatted (45/67)",
            r"\b(?:45|67)\d{2}(?:[\s-]?\d{4}){3}\b",  # Handles all formatting styles
            0.5
        ),
        Pattern(
            "Credit Card - Plain (45/67)", 
            r"\b(?:45|67)\d{14}\b",  # Plain 16 digits
            0.5
        )
    ]
    
    # Context keywords for credit cards
    CONTEXT = [
        "credit card",
        "card number", 
        "payment card",
        "CCN",
        "ccn",
        "cardholder",
        "card details", 
        "bank card",
        "visa",
        "mastercard"
    ]
    
    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "45_67_CREDIT_CARD",
    ):
        logger.info("Initializing Credit Card Recognizer...")
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )
    
    def analyze(
        self, text: str, entities: List[str], nlp_artifacts=None
    ) -> List[RecognizerResult]:
        logger.debug(f"Analyzing text for Credit Card: {text}")
        results = super().analyze(text, entities, nlp_artifacts)
        
        final_results = []
        for result in results:
            original_card_number = text[result.start:result.end]
            logger.debug(f"Detected potential Credit Card: {original_card_number}")
            
            # Remove all non-digit characters for validation
            cleaned_card_number = re.sub(r"[^\d]", "", original_card_number)
            
            # Validation checks
            if len(cleaned_card_number) != 16:
                logger.warning(f"Invalid length ({len(cleaned_card_number)} digits): {original_card_number}")
                continue
                
            if not cleaned_card_number.startswith(('45', '67')):
                logger.warning(f"Does not start with 45 or 67: {original_card_number}")
                continue
            
            # Luhn checksum validation
            if self._is_valid_checksum(cleaned_card_number):
                logger.info(f"Valid credit card detected: {original_card_number}")
                
                # Context analysis
                has_context = self._has_context(text, original_card_number)
                
                if has_context:
                    logger.info(f"Context found for: {original_card_number}")
                    result.score = 0.9  # High confidence
                else:
                    result.score = 0.7  # Medium confidence
                
                final_results.append(result)
            else:
                logger.warning(f"Invalid checksum: {original_card_number}")
        
        return final_results
    
    def _has_context(self, text: str, card_number: str) -> bool:
        """Check if context keywords are present near the card number."""
        # Look for context in a window around the card number
        start_pos = text.find(card_number)
        if start_pos == -1:
            return False
            
        # Define context window (50 chars before and after)
        window_start = max(0, start_pos - 50)
        window_end = min(len(text), start_pos + len(card_number) + 50)
        context_window = text[window_start:window_end].lower()
        
        return any(keyword in context_window for keyword in self.CONTEXT)
    
    def _is_valid_checksum(self, card_number: str) -> bool:
        """Validate the checksum using Luhn's algorithm."""
        try:
            total = 0
            reverse_digits = card_number[::-1]
            for i, digit in enumerate(reverse_digits):
                n = int(digit)
                if i % 2 == 1:  # Double every second digit
                    n *= 2
                    if n > 9:
                        n -= 9
                total += n
            return total % 10 == 0
        except (ValueError, IndexError):
            return False
