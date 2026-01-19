from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging

logger = logging.getLogger("presidio-analyzer")

class SpainBICSwiftRecognizer(PatternRecognizer):
    logger.info("Initializing Spain BIC/SWIFT Number Recognizer...")

    # Define patterns for Spain BIC/SWIFT Numbers (8 or 11 characters)
    PATTERNS = [
        Pattern(
            "Spain BIC/SWIFT - 8 Characters",
            r"\b[A-Za-z]{4}ES[A-Za-z0-9]{2}\b",  # Pattern for 8-character BIC/SWIFT: 4 letters + 'ES' + 2 alphanumeric
            0.5  # Initial confidence score for the pattern match
        ),
        Pattern(
            "Spain BIC/SWIFT - 11 Characters",
            r"\b[A-Za-z]{4}ES[A-Za-z0-9]{5}\b",  # Pattern for 11-character BIC/SWIFT: 4 letters + 'ES' + 5 alphanumeric
            0.5  # Initial confidence score for the pattern match
        ),
        Pattern(
            "Spain BIC/SWIFT - 11 Characters",
            r"\b[A-Z]{6}[A-Z0-9]{2}\b",  # Pattern for 11-character BIC/SWIFT: 4 letters + 'ES' + 5 alphanumeric
            0.1  # Initial confidence score for the pattern match
        ),
        Pattern(
            "Spain BIC/SWIFT - 11 Characters",
            r"\b[A-Z]{6}[A-Z0-9]{5}\b",  # Pattern for 11-character BIC/SWIFT: 4 letters + 'ES' + 5 alphanumeric
            0.1  # Initial confidence score for the pattern match
        )
    ]

    # Context keywords for BIC/SWIFT codes
    CONTEXT = [
        "BIC", "SWIFT", "SWIFT code", "BIC code", "bank identifier code", "bank code", 
        "código SWIFT", "código BIC", "código de identificación bancaria", "code BIC", "code SWIFT"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "es",  # Supports Spanish and English
        supported_entity: str = "SPAIN_BIC_SWIFT",
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
        logger.info(f"Analyzing text for Spain BIC/SWIFT: {text}")
        results = super().analyze(text, entities, nlp_artifacts)
        for result in results:
            bic_swift_number = text[result.start:result.end]
            logger.debug(f"Detected BIC/SWIFT Number: {bic_swift_number}, Confidence: {result.score}")
            
            # Adjust confidence score based on presence of context keywords
            if any(keyword in text.lower() for keyword in self.CONTEXT):
                logger.info(f"Context keywords found for BIC/SWIFT Number: {bic_swift_number}, setting high confidence.")
                result.score = 1.0  # High confidence if context keywords are present
            else:
                result.score = 0.7  # Medium confidence without context keywords
        return results
