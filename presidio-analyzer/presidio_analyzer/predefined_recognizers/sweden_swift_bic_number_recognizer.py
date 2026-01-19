from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging

logger = logging.getLogger("presidio-analyzer")

class SwedenBICSwiftRecognizer(PatternRecognizer):
    logger.info("Initializing Sweden BIC/SWIFT Recognizer...")

    # Define patterns for Sweden BIC/SWIFT Numbers
    PATTERNS = [
        Pattern(
            "Sweden BIC/SWIFT - General Pattern",
            r"\b[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?\b",  # Pattern: 4 letters (bank code), SE, 2 characters (location), optional 3 (branch)
            0.5  # Initial confidence score for the pattern match
        )
    ]

    # Context keywords for BIC/SWIFT
    CONTEXT = [
        "BIC", "SWIFT", "bank identifier code", "bank code", "bic nummer", 
        "bic-kod", "bankens kod", "bankens identifiering", "swift code"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "sv",  # Supports Swedish and English
        supported_entity: str = "SWEDEN_BIC_SWIFT",
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
        logger.info(f"Analyzing text for Sweden BIC/SWIFT: {text}")
        results = super().analyze(text, entities, nlp_artifacts)

        for result in results:
            bic_swift_number = text[result.start:result.end]
            logger.debug(f"Detected BIC/SWIFT: {bic_swift_number}, Confidence: {result.score}")

            # Check if any BIC/SWIFT-related terms exist in the nearby text
            nearby_text = text[max(0, result.start - 100):min(len(text), result.end + 100)]
            if any(keyword in nearby_text.lower() for keyword in self.CONTEXT):
                logger.info(f"Context keywords found near BIC/SWIFT: {bic_swift_number}, increasing confidence.")
                result.score = 1.0  # Increase confidence to high if context keywords are found
            else:
                result.score = 0.5  # Medium confidence for pattern match only
            
        return results
