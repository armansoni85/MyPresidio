from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer

class BSBCodeRecognizer(PatternRecognizer):
    """
    Recognizes Australian Bank State Branch (BSB) codes.
    BSB codes follow the format:
    - Three digits, a hyphen or space, followed by three digits
    This recognizer uses regex and context words to identify BSB codes.
    """

    PATTERNS = [
        Pattern(
            "BSB Code (High Confidence)",
            r"\b\d{3}[- ]\d{3}\b",  # Pattern for BSB code with hyphen or space
            0.85,  # High confidence due to specific pattern
        ),
    ]

    CONTEXT = [
        "bank state branch",
        "bsb",
        "bank code",
        "branch code",
        "financial institution",
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "AU_BSB_CODE",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )