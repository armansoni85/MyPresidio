from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class EUBICSwiftRecognizer(PatternRecognizer):
    logger.info("Initializing EU BIC/SWIFT Number Recognizer...")

    # Define patterns for EU BIC/SWIFT Numbers
    PATTERNS = [
        Pattern(
            "EU BIC/SWIFT Number - Medium Confidence",
            r"\b[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?\b",  # Pattern for BIC/SWIFT: 4 letters + 5-31 alphanumeric characters
            0.5  # Medium confidence for the pattern match
        )
    ]

    # Context keywords for SWIFT/BIC
    CONTEXT = [
        "BIC", "SWIFT", "SWIFT code", "BIC code", "bank identifier code", "bank code", 
        "código SWIFT", "código BIC", "código de identificación bancaria"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",  # Supports English and other EU languages
        supported_entity: str = "EU_BIC_SWIFT",
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
        logger.info(f"Analyzing text for BIC/SWIFT: {text}")
        results = super().analyze(text, entities, nlp_artifacts)
        for result in results:
            bic_swift_number = text[result.start:result.end]
            logger.debug(f"Detected BIC/SWIFT Number: {bic_swift_number}, Confidence: {result.score}")
            # Set medium confidence for valid SWIFT/BIC numbers
            result.score = 0.5  # Medium confidence as requested
        return results
