from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class USCustomSSNRecognizer(PatternRecognizer):
    logger.info("Initializing US Social Security Number Recognizer...")

    # Define patterns for unformatted SSN (9 consecutive digits)
    PATTERNS = [
        Pattern(
            "US SSN - Unformatted",
            r"\b\d{3}\d{2}\d{4}\b",  # Unformatted SSN: 9 consecutive digits
            0.5  # Initial confidence score for the pattern match
        )
    ]

    # Context keywords for SSN-related phrases
    CONTEXT = [
        "social security number", "ssn", "employee"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",  # Supports English
        supported_entity: str = "US_CUSTOM_SSN",
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
        logger.info(f"Analyzing text for US SSN: {text}")
        results = super().analyze(text, entities, nlp_artifacts)
        
        for result in results:
            ssn = text[result.start:result.end]
            logger.debug(f"Detected SSN: {ssn}, Confidence: {result.score}")

            # Check if any SSN-related phrases exist within 100 characters of the SSN
            nearby_text = text[max(0, result.start - 100):min(len(text), result.end + 100)]
            if any(keyword in nearby_text.lower() for keyword in self.CONTEXT):
                logger.info(f"Context keywords found near SSN: {ssn}, setting high confidence.")
                result.score = 1.0  # High confidence if context keywords are within 100 characters
            else:
                result.score = 0.5  # Medium confidence if no keywords are found
            
        return results
