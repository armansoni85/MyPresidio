from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class SwedenPassportRecognizer(PatternRecognizer):
    logger.info("Initializing Sweden Passport Number Recognizer...")

    # Define patterns for Sweden Passport Numbers (8-digit number)
    PATTERNS = [
        Pattern(
            "Sweden Passport - 8 Digit Number",
            r"\b\d{8}\b",  # 8-digit number pattern
            0.5  # Initial confidence score for the pattern match
        )
    ]

    # Context keywords for passport numbers and date-related terms
    CONTEXT = [
        "passport", "passnummer", "travel document", "pass number", "national passport", "international passport"
    ]
    
    # Keywords related to date formats and EU passport keywords
    DATE_CONTEXT = [
        "issued", "expiry", "valid until", "expiration date", "issue date", "valid from", "validity"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "sv",  # Supports Swedish and English
        supported_entity: str = "SWEDEN_PASSPORT",
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
        logger.info(f"Analyzing text for Sweden Passport: {text}")
        results = super().analyze(text, entities, nlp_artifacts)

        for result in results:
            passport_number = text[result.start:result.end]
            logger.debug(f"Detected Passport Number: {passport_number}, Confidence: {result.score}")

            # Check if passport-related terms are nearby
            nearby_text = text[max(0, result.start - 100):min(len(text), result.end + 100)].lower()
            if any(keyword in nearby_text for keyword in self.CONTEXT):
                logger.info(f"Context keywords found near Passport Number: {passport_number}")
                # Check if date-related terms are also nearby
                if any(date_keyword in nearby_text for date_keyword in self.DATE_CONTEXT):
                    logger.info(f"Date-related context found near Passport Number: {passport_number}, setting high confidence.")
                    result.score = 1.0  # High confidence if both passport and date context found
                else:
                    logger.info(f"No date-related context found for Passport Number: {passport_number}, setting medium confidence.")
                    result.score = 0.7  # Medium confidence for passport number and keyword without date context
            else:
                result.score = 0.5  # Initial confidence for pattern match only
            
        return results
