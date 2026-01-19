from typing import List, Tuple, Optional

from presidio_analyzer import Pattern, PatternRecognizer
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class AbaRoutingRecognizer(PatternRecognizer):
    logger.info("Initializing ABA Routing Recognizer...")

    """
    Recognize American Banking Association (ABA) routing number.

    Also known as routing transit number (RTN) and used to identify financial
    institutions and process transactions.

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
            "ABA routing number",
            r"\b(00|01|02|03|04|05|06|07|08|10|11|12)\d{3}-\d{4}-\d\b",
            0.5,
        ),
        Pattern(
            "ABA routing number (high confidence)",
            r"\b(00|01|02|03|04|05|06|07|08|10|11|12)\d{7}\b",  # 9-digit number starting with 00-12
            0.95,
        ),
        Pattern(
            "ABA routing number with hyphens (high confidence)",
            r"\b(00|01|02|03|04|05|06|07|08|10|11|12)\d{2}-\d{4}-\d\b",  # Format: XX-XXXX-XXX
            0.95,
        ),
    ]


    CONTEXT = [
        "aba",
        "aba number",
        "aba#",
        "abarouting",
        "abarouting#",
        "abaroutingnumber",
        "americanbankassociationrouting#",
        "americanbankassociationroutingnumber",
        "bankrouting",
        "bankrouting#",
        "bankroutingnumber",
        "routing",
        "routing #",
        "routing no",
        "routing number",
        "routing transit number",
        "routing#",
        "RTN",
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "ABA_ROUTING_NUMBER",
        replacement_pairs: Optional[List[Tuple[str, str]]] = None,
    ):
        self.replacement_pairs = replacement_pairs or [("-", "")]
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def validate_result(self, pattern_text: str) -> bool:  # noqa D102
        sanitized_value = self.__sanitize_value(pattern_text, self.replacement_pairs)
        return self.__checksum(sanitized_value)

    @staticmethod
    def __checksum(sanitized_value: str) -> bool:
        s = 0
        for idx, m in enumerate([3, 7, 1, 3, 7, 1, 3, 7, 1]):
            s += int(sanitized_value[idx]) * m
        return s % 10 == 0

    @staticmethod
    def __sanitize_value(text: str, replacement_pairs: List[Tuple[str, str]]) -> str:
        for search_string, replacement_string in replacement_pairs:
            text = text.replace(search_string, replacement_string)
        return text
