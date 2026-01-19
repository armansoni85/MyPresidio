from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class NetherlandsBICSwiftRecognizer(PatternRecognizer):
    """
    Recognizes valid Netherlands BIC/SWIFT codes (8 or 11 uppercase alphanumeric characters).
    """

    logger.info("Initializing Netherlands BIC/SWIFT Number Recognizer...")

    # Strict regex for 8 or 11 char Netherlands BIC/SWIFT codes
    PATTERNS = [
        Pattern(
            "Netherlands BIC/SWIFT (8 or 11 chars)",
            r"\b[A-Z]{4}NL[A-Z0-9]{2}([A-Z0-9]{3})?\b",
            0.6
        )
    ]

    CONTEXT = [
        "bic", "swift", "swift code", "bic code", "bank identifier code",
        "bank code", "código swift", "código bic", "code bic", "code swift"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "NETHERLANDS_BIC_SWIFT",
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
        logger.info(f"Analyzing text for Netherlands BIC/SWIFT: {text}")
        results = super().analyze(text, entities, nlp_artifacts)

        for result in results:
            bic_code = text[result.start:result.end]

            # Skip invalid length or special characters just in case
            if not re.fullmatch(r"[A-Z]{4}NL[A-Z0-9]{2}([A-Z0-9]{3})?", bic_code):
                logger.debug(f"Ignored invalid BIC/SWIFT format: {bic_code}")
                continue

            score = 0.6
            if any(keyword in text.lower() for keyword in self.CONTEXT):
                score += 0.3
            result.score = min(score, 1.0)

            logger.debug(f"Detected valid BIC/SWIFT: {bic_code}, Score: {result.score}")

        return results

