from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class SwedenVATRecognizer(PatternRecognizer):
    logger.info("Initializing Sweden VAT Recognizer...")

    # Define patterns for Swedish VAT Numbers
    PATTERNS = [
        Pattern(
            "Sweden VAT - Formatted",
            r"\bSE\d{12}\b",  # Pattern for SE followed by 12 digits
            0.5  # Medium confidence for formatted VAT number (SE + 12 digits)
        ),
        Pattern(
            "Sweden VAT - 12 Digits without Prefix",
            r"\b\d{12}\b",  # Pattern for 12 digits without SE prefix
            0.3  # Low confidence for 12 digits without SE prefix
        )
    ]

    # Context keywords for VAT
    CONTEXT = [
        "vat", "value added tax", "vat number", "tax number", "moms", "momsnummer", "tax identification"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "sv",  # Supports Swedish and English
        supported_entity: str = "SWEDEN_VAT",
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
        logger.info(f"Analyzing text for Sweden VAT: {text}")
        results = super().analyze(text, entities, nlp_artifacts)

        for result in results:
            vat_number = text[result.start:result.end]
            logger.debug(f"Detected VAT Number: {vat_number}, Confidence: {result.score}")

            # Check for context keywords in the surrounding text
            nearby_text = text[max(0, result.start - 100):min(len(text), result.end + 100)].lower()
            if any(keyword in nearby_text for keyword in self.CONTEXT):
                logger.info(f"Context keywords found near VAT Number: {vat_number}, increasing confidence.")
                result.score = min(result.score + 0.3, 1.0)  # Increase confidence if context found
            
        return results
