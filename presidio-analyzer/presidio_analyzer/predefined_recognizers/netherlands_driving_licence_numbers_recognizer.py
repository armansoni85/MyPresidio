from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import re
import logging

logger = logging.getLogger("presidio-analyzer")

class NetherlandsDriversLicenseRecognizer(PatternRecognizer):
    logger.info("Initializing Netherlands Driver's License Recognizer...")

    # Define patterns for Netherlands Driver's License Numbers (10 digits)
    PATTERNS = [
        Pattern(
            "Netherlands Driver's License",
            r"\b\d{10}\b",  # Pattern for exactly 10 digits
            0.4  # Lower initial confidence score
        )
    ]

    # Context keywords for driver's license (including abbreviations)
    CONTEXT = [
        "rijbewijs", "driver's license", "drivers license", "license number",
        "licentienummer", "nummer rijbewijs", "driving license", 
        "driver license", "license no", "DL", "dl", "Driving Lic", "licence"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "nl",
        supported_entity: str = "NETHERLANDS_DRIVERS_LICENSE",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )
        # Boost confidence by 0.6 when context is found near the match
        self.context_boost = 0.6
        # Context window: 30 characters before/after the match
        self.context_window = 30

    def analyze(
        self, text: str, entities: List[str], nlp_artifacts=None
    ) -> List[RecognizerResult]:
        results = super().analyze(text, entities, nlp_artifacts)
        if not results:
            return results
        
        # Check each result for nearby context keywords
        enhanced_results = []
        for result in results:
            # Extract context window around the match
            start_pos = max(0, result.start - self.context_window)
            end_pos = min(len(text), result.end + self.context_window)
            context_window_text = text[start_pos:end_pos].lower()

            # Check if any context keyword exists in the window (case-insensitive)
            if any(
                re.search(rf"\b{re.escape(keyword.lower())}\b", context_window_text)
                for keyword in self.CONTEXT
            ):
                result.score = min(1.0, result.score + self.context_boost)
                enhanced_results.append(result)
                logger.debug(f"Context found for DL {text[result.start:result.end]}")
            else:
                logger.debug(f"No context found for DL {text[result.start:result.end]}")

        return enhanced_results
