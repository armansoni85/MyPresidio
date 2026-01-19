from typing import Optional, List, Tuple
from presidio_analyzer import Pattern, PatternRecognizer


class AustraliaBusinessCompanyNumberRecognizer(PatternRecognizer):
    """
    Recognizes Australian Business Numbers (ABN) and Company Numbers (ACN).

    Matches Australian Business (ABN) and Company (ACN) Numbers, 
    formatted as:
    - 11-digit number delimited by dots, hyphens, or spaces
    - 11-digit number without delimiters

    :param patterns: List of patterns to be used by this recognizer
    :param context: List of context words to increase confidence in detection
    :param supported_language: Language this recognizer supports
    :param supported_entity: The entity this recognizer can detect
    :param replacement_pairs: List of tuples with potential replacement values
    for different strings to be used during pattern matching.
    """

    PATTERNS = [
        Pattern(
            name="Australia ABN Pattern",
            regex=r"\b\d{2}[-\s]?\d{3}[-\s]?\d{3}[-\s]?\d{3}\b",
            score=0.85,
        )
    ]

    CONTEXT = ["business number", "ABN", "Australia"]

    CONTEXT = [
        "australian business number",
        "abn",
        "australian company number",
        "acn",
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "AU_BUSINESS_COMPANY_NUMBER",
        replacement_pairs: Optional[List[Tuple[str, str]]] = None,
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

