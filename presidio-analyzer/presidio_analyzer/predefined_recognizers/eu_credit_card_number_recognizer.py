from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class EUDebitCardRecognizer(PatternRecognizer):
    logger.info("Initializing EU Debit Card Recognizer...")

    # Define patterns for EU debit card numbers (formatted and unformatted)
    PATTERNS = [
        Pattern(
            "EU Debit Card - Unformatted",
            r"\b(0604|5018|5020|5038|5612|5893|6304|6390|6706|6709|6759|6761|6762|6763|6771|6799)\d{12,15}\b",  
            0.5  # Initial confidence score for the pattern match
        ),
        Pattern(
            "EU Debit Card - Formatted (Spaces)",
            r"\b(0604|5018|5020|5038|5612|5893|6304|6390|6706|6709|6759|6761|6762|6763|6771|6799) \d{4} \d{4} \d{4,7}\b",  
            0.5  # Initial confidence score for the pattern match
        ),
        Pattern(
            "EU Debit Card - Formatted (Hyphens)",
            r"\b(0604|5018|5020|5038|5612|5893|6304|6390|6706|6709|6759|6761|6762|6763|6771|6799)-\d{4}-\d{4}-\d{4,7}\b",  
            0.5  # Initial confidence score for the pattern match
        ),
        Pattern(
            "EU Debit Card - Formatted (Dots)",
            r"\b(0604|5018|5020|5038|5612|5893|6304|6390|6706|6709|6759|6761|6762|6763|6771|6799).\d{4}.\d{4}.\d{4,7}\b",  
            0.5  # Initial confidence score for the pattern match
        ),
        Pattern(
            "EU Debit Card - Alternative Patterns",
            r"\b(4026|417500|4405|4508|4844|4913|4917|5019)[0-9]{12}\b|\b(4026|417500|4405|4508|4844|4913|4917|5019) [0-9]{4} [0-9]{4} [0-9]{4}\b|\b(4026|417500|4405|4508|4844|4913|4917|5019)-[0-9]{4}-[0-9]{4}-[0-9]{4}\b|\b(4026|417500|4405|4508|4844|4913|4917|5019)\.[0-9]{4}\.[0-9]{4}\.[0-9]{4}\b",  
            0.5  # Initial confidence score for the pattern match
        )
    ]

    # Context keywords for debit cards
    CONTEXT = [
        "debit card", "card number", "security code", "expiration date", "expiry date", "cvv", "cvc", "valid thru", "exp", 
        "carte de débit", "numéro de carte", "code de sécurité", "date d'expiration"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",  # Supports English and other EU languages
        supported_entity: str = "EU_DEBIT_CARD",
    ):
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
        logger.info(f"Analyzing text for EU debit card: {text}")
        results = super().analyze(text, entities, nlp_artifacts)
        
        for result in results:
            card_number = text[result.start:result.end]
            logger.debug(f"Detected debit card number: {card_number}, Confidence: {result.score}")

            # Clean card number by removing spaces, hyphens, and dots
            cleaned_card_number = re.sub(r"[-\s.]", "", card_number)

            # Perform Luhn checksum validation
            if self._is_valid_checksum(cleaned_card_number):
                logger.info(f"Checksum valid for card number: {card_number}")
                result.score = 0.7  # Medium confidence if checksum passes
                
                # Check for context keywords or expiration date format within the nearby text
                if any(keyword in text.lower() for keyword in self.CONTEXT) or re.search(r"\b\d{2}/\d{2}\b|\b\d{2}/\d{4}\b", text):
                    logger.info(f"Context keywords or expiration date found near card number: {card_number}, setting high confidence.")
                    result.score = 1.0  # High confidence if keywords or expiration date format are found
            else:
                logger.warning(f"Invalid checksum for card number: {card_number}")
                result.score = 0.0  # Invalid card number

        return results

    def _is_valid_checksum(self, card_number: str) -> bool:
        """
        Validate the card number using Luhn's algorithm (Modulus 10).
        """
        sum_ = 0
        alternate = False
        
        # Luhn's algorithm: process digits from right to left
        for digit in reversed(card_number):
            num = int(digit)
            if alternate:
                num *= 2
                if num > 9:
                    num -= 9
            sum_ += num
            alternate = not alternate
        
        return sum_ % 10 == 0
