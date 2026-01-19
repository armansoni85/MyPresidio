from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer


class DOBRecognizer(PatternRecognizer):
    """
    Recognize the text 'DOB' (not case-sensitive).
    """

    PATTERNS = [
        Pattern(
            "DOB Pattern",
            r"\bDOB\b",  # Matches the exact text 'DOB' with word boundaries
            1.0,         # High confidence
        )
    ]

    CONTEXT = ["DOB", "dob"]  # Additional context keywords

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "DOB",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )