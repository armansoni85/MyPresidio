from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class NetherlandsIBANRecognizer(PatternRecognizer):
    logger.info("Initializing Netherlands IBAN Recognizer...")

    # Define patterns for Netherlands IBAN
    PATTERNS = [
        Pattern(
            "Netherlands IBAN - High Confidence",
            r"\bNL\d{2} [A-Z]{4} \d{4} \d{4} \d{2}\b",  # Pattern for Netherlands IBAN
            0.7  # Initial confidence score for the pattern match
        ),
        Pattern(
            "Netherlands IBAN - High Confidence",
            r"\bNL\d{2}[A-Z]{4}\d{10}\b",  # Pattern for Netherlands IBAN
            0.7  # Initial confidence score for the pattern match
        )

    ]

    # Context keywords for IBAN
    CONTEXT = [
        "IBAN", "international bank account number", "bankrekeningnummer", 
        "IBAN nummer", "bank account", "rekeningnummer", "code IBAN"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "nl",  # Supports Dutch and English
        supported_entity: str = "NETHERLANDS_IBAN",
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
        logger.info(f"Analyzing text for Netherlands IBAN: {text}")
        results = super().analyze(text, entities, nlp_artifacts)
        for result in results:
            iban_number = text[result.start:result.end]
            logger.debug(f"Detected IBAN: {iban_number}, Confidence: {result.score}")
            # Adjust confidence based on context keywords
            if any(keyword in text.lower() for keyword in self.CONTEXT):
                logger.info(f"Context keywords found for IBAN: {iban_number}, setting high confidence.")
                result.score = 1.0  # High confidence if context keywords are present
            return results
