from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class ItalyIBANRecognizer(PatternRecognizer):
    logger.info("Initializing Italy IBAN Recognizer...")

    # Define patterns for Italy IBAN
    PATTERNS = [
        Pattern(
            "Italy IBAN - High Confidence",
            r"\bIT\d{2}[A-Za-z]\d{5}\d{5}\d{12}\b|\bIT\d{2}\s?[A-Za-z]\d{5}\s?\d{5}\s?\d{12}\b",  # Pattern for Italy IBAN
            0.7  # Initial confidence score for the pattern match
        ),
        Pattern(
            "Italy IBAN - High Confidence",
            r"IT\d{2}\sX\d{3}\s\d{4}\s\d{4}\s\d{4}\s\d{4}\s\d{3}",  # Pattern for Italy IBAN
            0.7  # Initial confidence score for the pattern match
        )
    ]

    # Context keywords for IBAN
    CONTEXT = [
        "IBAN", "international bank account number", "número de cuenta bancaria", 
        "conto bancario", "numero IBAN", "codice IBAN", "codice bancario"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "it",  # Supports Italian and English
        supported_entity: str = "ITALY_IBAN",
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
        logger.info(f"Analyzing text for Italy IBAN: {text}")
        results = super().analyze(text, entities, nlp_artifacts)
        for result in results:
            iban_number = text[result.start:result.end]
            logger.debug(f"Detected IBAN: {iban_number}, Confidence: {result.score}")
            # Adjust confidence based on context keywords
            if any(keyword in text.lower() for keyword in self.CONTEXT):
                logger.info(f"Context keywords found for IBAN: {iban_number}, setting high confidence.")
                result.score = 1.0  # High confidence if context keywords are present
            return results
