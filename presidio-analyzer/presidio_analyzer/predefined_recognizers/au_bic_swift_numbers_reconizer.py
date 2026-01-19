from typing import Optional, List, Tuple
from presidio_analyzer import Pattern, PatternRecognizer

class AustraliaBICRecognizer(PatternRecognizer):
    """
    Recognizes Australian Bank Identifier Code (BIC) / SWIFT numbers.

    The BIC/SWIFT code follows the format:
    - 4 letters (institution code)
    - 2 letters (country code, "AU" for Australia)
    - 2 letters or digits (location code)
    - Optional 3 letters or digits (branch code)

    :param patterns: List of patterns to be used by this recognizer
    :param context: List of context words to increase confidence in detection
    :param supported_language: Language this recognizer supports
    :param supported_entity: The entity this recognizer can detect
    """

    PATTERNS = [
        Pattern(
            "BIC/SWIFT (11 characters)",
            r"\b[A-Za-z]{4}AU[A-Za-z0-9]{2}[A-Za-z0-9]{3}\b",
            1.0,
        ),
        Pattern(
            "BIC/SWIFT (8 characters)",
            r"\b[A-Za-z]{4}AU[A-Za-z0-9]{2}\b",
            1.0,
        ),
    ]

    CONTEXT = [
        "bic",
        "swift",
        "bank identifier code",
        "australia bic",
        "australian bic",
        "australia swift",
        "australian swift",
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "AU_BIC",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )