from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult

class CanadaSINRecognizer(PatternRecognizer):
    """
    Recognizes Canadian Social Insurance Numbers (SIN) using patterns, context words, and Luhn algorithm.

    The recognizer uses regex patterns to identify SINs and contextual analysis to increase detection accuracy.
    """

    PATTERNS = [
        Pattern(
            "Formatted SIN",
            r"\b\d{3}[-\s]?\d{3}[-\s]?\d{3}\b",
            0.9,
        ),
        Pattern(
            "Unformatted SIN",
            r"\b\d{9}\b",
            0.85,
        ),
    ]

    CONTEXT = [
        "social insurance number", "SIN", "Canada SIN", "Canadian", "canada social insurance number"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "CA_SIN",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        """
        Analyze the text to identify Canadian SIN and adjust confidence based on context.

        :param text: Text to be analyzed.
        :param entities: List of entities to look for.
        :param nlp_artifacts: Pre-processed text artifacts.
        :return: List of RecognizerResult objects with detection details.
        """
        results = super().analyze(text, entities, nlp_artifacts)
        
        for result in results:
            if self._has_context(text, result.start, result.end):
                # Strong context match increases the score more significantly
                result.score += 0.2
            else:
                # If no strong context is found, reduce the score slightly to avoid false positives
                result.score -= 0.1

            result.score = min(result.score, 1.0)  # Ensure the score doesn't exceed 1.0
            result.score = max(result.score, 0.0)  # Ensure the score doesn't drop below 0.0

        return results


    def _has_context(self, text: str, start: int, end: int) -> bool:
        """
        Check if the context words are present near the detected entity.

        :param text: Text being analyzed.
        :param start: Start position of the detected entity.
        :param end: End position of the detected entity.
        :return: True if context words are found near the entity, False otherwise.
        """
        window_size = 50  # Number of characters around the entity to check for context
        pre_context = text[max(0, start - window_size):start].lower()
        post_context = text[end:end + window_size].lower()

        for context_word in self.CONTEXT:
            if context_word.lower() in pre_context or context_word.lower() in post_context:
                return True
        return False

    def validate_result(self, pattern_text: str) -> bool:
        """Validate SIN using Luhn algorithm."""
        sin = pattern_text.replace(" ", "").replace("-", "")
        if len(sin) != 9 or not sin.isdigit():
            return False
        return self._luhn_checksum(sin)

    @staticmethod
    def _luhn_checksum(number: str) -> bool:
        """Validate a number using the Luhn algorithm."""
        def digits_of(n):
            return [int(d) for d in str(n)]
        digits = digits_of(number)
        odd_digits = digits[-1::-2]
        even_digits = digits[-2::-2]
        checksum = sum(odd_digits)
        for d in even_digits:
            checksum += sum(digits_of(d * 2))
        return checksum % 10 == 0
