from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging

logger = logging.getLogger("presidio-analyzer")

class ItalyDriversLicenseRecognizer(PatternRecognizer):
    logger.info("Initializing Italy Drivers License Recognizer...")

    # Define patterns for Italy driver's license numbers
    PATTERNS = [
        Pattern(
            "Italy Driver License - High Confidence",
            r"\b[A-Za-z][AaVv][0-9]{7}[A-Za-z]\b",  # Pattern to match: 1 letter (A/V), 7 digits, 1 letter
            0.7  # Initial confidence score for the pattern match
        ),
    ]

    # Italian and English context keywords for Italy driver's licenses
    CONTEXT = [
        "patente di guida", "numero patente", "licenza di guida", 
        "italy driver's license", "driver's license", "driving license", 
        "dl number", "license number"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",  # Supports English and Italian
        supported_entity: str = "ITALY_DRIVERS_LICENSE",
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
        results = super().analyze(text, entities, nlp_artifacts)
        for result in results:
            # Increase the score if context keywords are found
            if any(keyword in text.lower() for keyword in self.CONTEXT):
                result.score = min(result.score + 0.3, 1.0)  # Increase score by 0.3, cap at 1.0
        return results
