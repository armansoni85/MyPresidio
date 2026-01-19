from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class SwedenIBANRecognizer(PatternRecognizer):
    logger.info("Initializing Sweden IBAN Recognizer...")

    PATTERNS = [
        Pattern(
            "Sweden IBAN - Medium Confidence",
            r"\bSE\d{2}(?:[-\s]*\d{4}){5}\b",  # Allows flexible formatting
            0.5
        )
    ]

    CONTEXT = [
        "IBAN", "Sweden IBAN", "international bank account number", "bankkonto",
        "kontonummer", "nummer IBAN", "IBAN nummer", "bank code", "bank identifier"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "sv",
        supported_entity: str = "SWEDEN_IBAN",
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
        logger.debug(f"Analyzing text for Sweden IBAN: {text}")
        results = super().analyze(text, entities, nlp_artifacts)
        enhanced_results = []

        for result in results:
            iban_candidate = text[result.start:result.end]
            logger.debug(f"Detected IBAN candidate: {iban_candidate}")

            if self._validate_iban(iban_candidate):
                nearby_text = text[max(0, result.start - 100):min(len(text), result.end + 100)].lower()
                if any(keyword.lower() in nearby_text for keyword in self.CONTEXT):
                    logger.info("Valid IBAN with context — assigning high confidence")
                    result.score = 1.0
                else:
                    logger.info("Valid IBAN without context — assigning medium confidence")
                    result.score = 0.7
            else:
                logger.debug("Invalid IBAN checksum — discarding result")
                continue  # Skip invalid IBANs

            enhanced_results.append(result)

        return enhanced_results

    def _validate_iban(self, iban: str) -> bool:
        """Validate Swedish IBAN structure and checksum."""
        # Clean and normalize input
        iban_clean = re.sub(r'[^A-Z0-9]', '', iban.upper())
        
        # Verify length and country code
        if len(iban_clean) != 24 or not iban_clean.startswith('SE'):
            return False

        # Validate checksum
        try:
            rearranged = iban_clean[4:] + iban_clean[:4]
            remainder = 0
            for char in rearranged:
                num = int(char, 36)
                if num < 10:
                    remainder = (remainder * 10 + num) % 97
                else:
                    remainder = (remainder * 100 + num) % 97
            return remainder == 1
        except Exception as e:
            logger.error(f"Validation error for {iban}: {e}")
            return False
