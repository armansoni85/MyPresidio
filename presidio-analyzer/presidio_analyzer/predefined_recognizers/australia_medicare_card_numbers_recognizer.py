from typing import Optional, List, Tuple
from presidio_analyzer import Pattern, PatternRecognizer

class AustraliaMedicareCardRecognizer(PatternRecognizer):
    """
    Recognizes Australian Medicare Card Numbers.

    Australian Medicare Card Numbers are typically formatted as:
    - 10-digit number without a delimiter
    - 10-digit number delimited by spaces
    - 10-digit number delimited by hyphens
    - 11-digit number without a delimiter
    - 11-digit number delimited by spaces
    - 11-digit number delimited by hyphens

    This recognizer identifies these patterns using regex and context words.

    :param patterns: List of patterns to be used by this recognizer
    :param context: List of context words to increase confidence in detection
    :param supported_language: Language this recognizer supports
    :param supported_entity: The entity this recognizer can detect
    :param replacement_pairs: List of tuples with potential replacement values
    for different strings to be used during pattern matching.
    """

    PATTERNS = [
        # 10-digit patterns
        Pattern(
            "Australia Medicare Card Number (No Delimiter, 10 digits)",
            r"\b[2-6]\d{8}\d\b",
            1.0,
        ),
        Pattern(
            "Australia Medicare Card Number (Space Delimiter, 10 digits)",
            r"\b[2-6]\d{3} \d{5} \d\b",
            1.0,
        ),
        Pattern(
            "Australia Medicare Card Number (Hyphen Delimiter, 10 digits)",
            r"\b[2-6]\d{3}-\d{5}-\d\b",
            1.0,
        ),
        # 11-digit patterns
        Pattern(
            "Australia Medicare Card Number (No Delimiter, 11 digits)",
            r"\b[2-6]\d{8}\d{2}\b",
            1.0,
        ),
        Pattern(
            "Australia Medicare Card Number (Space Delimiter, 11 digits)",
            r"\b[2-6]\d{3} \d{5} \d \d\b",
            1.0,
        ),
        Pattern(
            "Australia Medicare Card Number (Hyphen Delimiter, 11 digits)",
            r"\b[2-6]\d{3}-\d{5}-\d-\d\b",
            1.0,
        ),
    ]

    CONTEXT = [
        "medicare",
        "medicare card",
        "australian medicare",
        "medicare number",
        "medicare card number",
        "medical account number"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "AU_MEDICARE_CARD",
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
