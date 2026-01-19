from typing import List, Optional
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from presidio_analyzer.nlp_engine import NlpArtifacts


class ItFiscalCodeRecognizer(PatternRecognizer):
    """
    Recognizes Italian Fiscal Code (Codice Fiscale) using regex detection
    and checksum validation.
    """

    PATTERNS = [
        Pattern(
            "Fiscal Code",
            r"\b([A-Z0-9]{16})\b",  # simplified match, real check via checksum
            0.5,  # base confidence slightly higher than 0.3
        ),
    ]

    CONTEXT = [
        "codice fiscale", "cf", "fiscal code", "tax code", 
        "italian fiscal code", "italian tax code", "cf:",
        "codice fiscale:", "tax id", "fiscal id", "cod.fisc.",
        "numero fiscale"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "it",
        supported_entity: str = "IT_FISCAL_CODE",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def validate_result(self, pattern_text: str) -> bool:
        """
        Validate fiscal code using checksum algorithm.
        """

        pattern_text = pattern_text.upper().strip()

        if len(pattern_text) != 16:
            return False

        control_char = pattern_text[-1]
        text_to_validate = pattern_text[:-1]

        map_odd = {
            "0": 1, "1": 0, "2": 5, "3": 7, "4": 9, "5": 13, "6": 15, "7": 17, "8": 19, "9": 21,
            "A": 1, "B": 0, "C": 5, "D": 7, "E": 9, "F": 13, "G": 15, "H": 17, "I": 19, "J": 21,
            "K": 2, "L": 4, "M": 18, "N": 20, "O": 11, "P": 3, "Q": 6, "R": 8, "S": 12, "T": 14,
            "U": 16, "V": 10, "W": 22, "X": 25, "Y": 24, "Z": 23,
        }

        map_even = {
            "0": 0, "1": 1, "2": 2, "3": 3, "4": 4, "5": 5, "6": 6, "7": 7, "8": 8, "9": 9,
            "A": 0, "B": 1, "C": 2, "D": 3, "E": 4, "F": 5, "G": 6, "H": 7, "I": 8, "J": 9,
            "K": 10, "L": 11, "M": 12, "N": 13, "O": 14, "P": 15, "Q": 16, "R": 17, "S": 18,
            "T": 19, "U": 20, "V": 21, "W": 22, "X": 23, "Y": 24, "Z": 25,
        }

        map_mod = {
            0: "A", 1: "B", 2: "C", 3: "D", 4: "E", 5: "F", 6: "G", 7: "H", 8: "I", 9: "J",
            10: "K", 11: "L", 12: "M", 13: "N", 14: "O", 15: "P", 16: "Q", 17: "R", 18: "S",
            19: "T", 20: "U", 21: "V", 22: "W", 23: "X", 24: "Y", 25: "Z",
        }

        odd_sum = 0
        even_sum = 0

        for i, char in enumerate(text_to_validate):
            if (i + 1) % 2 == 1:  # odd positions
                odd_sum += map_odd.get(char, 0)
            else:  # even positions
                even_sum += map_even.get(char, 0)

        total = odd_sum + even_sum
        remainder = total % 26
        expected_control = map_mod.get(remainder)

        return expected_control == control_char

    def analyze(
        self, text: str, entities: List[str], nlp_artifacts: NlpArtifacts = None
    ) -> List[RecognizerResult]:
        results = super().analyze(text, entities, nlp_artifacts)
        if not results:
            return results

        text_lower = text.lower()
        has_context = any(context_word in text_lower for context_word in self.context)

        for result in results:
            is_valid = self.validate_result(result.text)

            if is_valid and has_context:
                result.score = 0.9
            elif is_valid and not has_context:
                result.score = 0.75
            elif not is_valid and has_context:
                result.score = 0.6
            else:
                result.score = 0.4

        return results

