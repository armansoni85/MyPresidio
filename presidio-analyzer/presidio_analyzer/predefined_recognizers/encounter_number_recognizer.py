from typing import Optional, List, Tuple
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult

class EncounterNumberRecognizer(PatternRecognizer):
    """
    Recognizes Encounter Numbers.

    The Encounter Number is identified when it has a 7-digit number
    within a proximity of the phrase "Encounter Number", either before or after.

    :param patterns: List of patterns to be used by this recognizer
    :param context: List of context words to increase confidence in detection
    :param supported_language: Language this recognizer supports
    :param supported_entity: The entity this recognizer can detect
    :param replacement_pairs: List of tuples with potential replacement values
    for different strings to be used during pattern matching.
    This can allow a greater variety in input, for example by removing dashes or spaces.
    """

    PATTERNS = [
        Pattern(
            name="Encounter Number Pattern",
            regex=r"\b(\d{7})\b",  # Match any 7-digit number
            score=0.5,
        )
    ]

    CONTEXT = [
        "encounter number",
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "ENCOUNTER_NUMBER",
        replacement_pairs: Optional[List[Tuple[str, str]]] = None,
    ):
        self.replacement_pairs = (
            replacement_pairs if replacement_pairs else [("-", ""), (" ", "")]
        )
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        results = super().analyze(text, entities, nlp_artifacts)

        # Apply context-based logic to filter or adjust results
        validated_results = []
        for result in results:
            if self._has_valid_context(text, result.start, result.end):
                result.score = min(result.score + 0.4, 1.0)  # Boost score with context
                validated_results.append(result)
            else:
                result.score = result.score * 0.5  # Reduce score if no valid context found

        return validated_results

    def _has_valid_context(self, text: str, start: int, end: int) -> bool:
        """Check if the Encounter Number has the required context within proximity, either before or after."""
        proximity_range = 20  # Number of characters to check for proximity
        context_start = max(0, start - proximity_range)
        context_end = min(len(text), end + proximity_range)

        # Check for proximity of context phrases around the detected number
        context_window = text[context_start:context_end].lower()
        return any(context_word in context_window for context_word in self.CONTEXT)

    def invalidate_result(self, pattern_text: str) -> bool:
        """
        Invalidate numbers that don't meet certain criteria.
        """
        only_digits = "".join(c for c in pattern_text if c.isdigit())

        # Validate that it's exactly 7 digits
        if len(only_digits) != 7:
            return True

        return False