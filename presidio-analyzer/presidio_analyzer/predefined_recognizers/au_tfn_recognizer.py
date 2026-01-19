from typing import Optional, List, Tuple
from presidio_analyzer import Pattern, PatternRecognizer

class AuTfnRecognizer(PatternRecognizer):
    PATTERNS = [
        Pattern(
            "TFN (Continuous)",
            r"\b\d{9}\b",  # 9-digit TFN without spaces or hyphens
            0.8,
        ),
        Pattern(
            "TFN (Spaces)",
            r"\b\d{3} \d{3} \d{3}\b",  # TFN with spaces
            0.9,
        ),
        Pattern(
            "TFN (Hyphens)",
            r"\b\d{3}-\d{3}-\d{3}\b",  # TFN with hyphens
            0.9,
        ),
    ]

    CONTEXT = [
        "tax file number",
        "tfn",
        "tax number",
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "AU_TFN",
        replacement_pairs: Optional[List[Tuple[str, str]]] = None,
    ):
        self.replacement_pairs = replacement_pairs if replacement_pairs else [("-", ""), (" ", "")]
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def validate_result(self, pattern_text: str) -> bool:
        text = self.__sanitize_value(pattern_text, self.replacement_pairs)
        tfn_list = [int(digit) for digit in text if not digit.isspace()]
        weight = [1, 4, 3, 7, 5, 8, 6, 9, 10]
        sum_product = 0
        for i in range(9):
            sum_product += tfn_list[i] * weight[i]
        remainder = sum_product % 11
        return remainder == 0

    @staticmethod
    def __sanitize_value(text: str, replacement_pairs: List[Tuple[str, str]]) -> str:
        for search_string, replacement_string in replacement_pairs:
            text = text.replace(search_string, replacement_string)
        return text