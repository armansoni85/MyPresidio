from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class VisaCreditCardRecognizer(PatternRecognizer):
    logger.info("Initializing Visa Credit Card Recognizer...")

    # Updated pattern for Visa: supports 13–19 digits, spaces, and hyphens
    PATTERNS = [
        Pattern(
            "Visa Credit Card - Enhanced Pattern",
            # 4 + 12-18 digits, allowing separators (space or hyphen)
            r"\b4(?:[\s-]?[0-9]){12,18}\b",
            0.5
        )
    ]

    CONTEXT = [
        "visa", "visa card", "credit card", "visa credit card", "payment card", "card number"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "VISA_CREDIT_CARD",
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
        logger.info(f"Analyzing text for Visa credit card: {text}")
        results = super().analyze(text, entities, nlp_artifacts)

        for result in results:
            card_number = text[result.start:result.end]
            logger.debug(f"Detected Visa Card Number: {card_number}, Confidence: {result.score}")

            # Clean up card number by removing spaces and hyphens
            cleaned_card_number = re.sub(r"[\s-]", "", card_number)

            # Only proceed if it’s 13–19 digits
            if not (13 <= len(cleaned_card_number) <= 19 and cleaned_card_number.isdigit()):
                logger.warning(f"Invalid length for Visa card: {card_number}")
                result.score = 0.0
                continue

            # Validate checksum
            if self._is_valid_checksum(cleaned_card_number):
                logger.info(f"Checksum valid for Visa card: {card_number}")
                result.score = 0.7
                # Boost confidence if Visa context is nearby
                if any(keyword in text.lower() for keyword in self.CONTEXT):
                    logger.info(f"Context keywords found near Visa card: {card_number}, setting high confidence.")
                    result.score = 1.0
            else:
                logger.warning(f"Invalid checksum for Visa card: {card_number}")
                result.score = 0.0

        return results

    def _is_valid_checksum(self, card_number: str) -> bool:
        """Validate number using Luhn algorithm."""
        sum_ = 0
        alternate = False

        for digit in reversed(card_number):
            num = int(digit)
            if alternate:
                num *= 2
                if num > 9:
                    num -= 9
            sum_ += num
            alternate = not alternate

        return sum_ % 10 == 0

