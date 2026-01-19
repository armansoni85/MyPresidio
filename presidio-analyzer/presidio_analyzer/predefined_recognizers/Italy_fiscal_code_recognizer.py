from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class ItalyFiscalCodeRecognizer(PatternRecognizer):
    logger.info("Initializing Italy Fiscal Code Recognizer...")

    # Define the pattern for the Italian fiscal code
    PATTERNS = [
        Pattern(
            "Italy Fiscal Code - High Confidence",
            r"(?i:\b[A-Z]{3}[ -]?[A-Z]{3}[ -]?[0-9]{2}[A-EHLMPRST](?:[04][1-9]|[1256][0-9]|[37][01])[ -]?[A-MZ][0-9]{3}[A-Z]\b)",
            0.3,  # Base score - we'll adjust based on validation and context
        ),
        Pattern(
            "Italy Fiscal Code - Medium Confidence",
            r"^([B-DF-HJ-NP-TV-Z]{3})([B-DF-HJ-NP-TV-Z]{3})(\d{2})([A-EHLMPR-T])(\d{2})(\d{4})(\d)$",
            0.3,  # Base score - we'll adjust based on validation and context
        ),
        Pattern(
            "Fiscal Code",
            (
                r"(?i)((?:[A-Z][AEIOU][AEIOUX]|[AEIOU]X{2}"
                r"|[B-DF-HJ-NP-TV-Z]{2}[A-Z]){2}"
                r"(?:[\dLMNP-V]{2}(?:[A-EHLMPR-T](?:[04LQ][1-9MNP-V]|[15MR][\dLMNP-V]"
                r"|[26NS][0-8LMNP-U])|[DHPS][37PT][0L]|[ACELMRT][37PT][01LM]"
                r"|[AC-EHLMPR-T][26NS][9V])|(?:[02468LNQSU][048LQU]"
                r"|[13579MPRTV][26NS])B[26NS][9V])(?:[A-MZ][1-9MNP-V][\dLMNP-V]{2}"
                r"|[A-M][0L](?:[1-9MNP-V][\dLMNP-V]|[0L][1-9MNP-V]))[A-Z])"
            ),
            0.3,
        ),
    ]

    # Extended context keywords for Italy fiscal codes
    CONTEXT = [
        "codice fiscale", "fiscal code", "italian tax code", "italy personal code",
        "cf", "tax code", "italian fiscal code", "cf:", "codice fiscale:",
        "tax id", "fiscal id", "cod.fisc.", "numero fiscale"
    ]

    # Character values for checksum calculation
    ODD_VALUES = {
        **{char: val for char, val in zip('ABCDEFGHIJKLMNOPQRSTUVWXYZ', [1, 0, 5, 7, 9, 13, 15, 17, 19, 21, 2, 4, 18, 20, 11, 3, 6, 8, 12, 14, 16, 10, 22, 25, 24, 23])},
        **{str(digit): val for digit, val in zip('0123456789', [1, 0, 5, 7, 9, 13, 15, 17, 19, 21])}
    }

    EVEN_VALUES = {
        **{char: val for char, val in zip('ABCDEFGHIJKLMNOPQRSTUVWXYZ', range(26))},
        **{str(digit): int(digit) for digit in '0123456789'}
    }

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "it",  # Supports Italian and English
        supported_entity: str = "ITALY_FISCAL_CODE",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def analyze(
        self, text: str, entities: List[str], nlp_artifacts=None
    ) -> List[RecognizerResult]:
        # First get the basic results from parent class
        results = super().analyze(text, entities, nlp_artifacts)
        
        if not results:
            return results

        # Check if context keywords are present in the text
        text_lower = text.lower()
        has_context = any(keyword.lower() in text_lower for keyword in self.CONTEXT)
        
        for result in results:
            # Extract the fiscal code text using start and end positions
            code_text = text[result.start:result.end]
            # Clean the detected fiscal code - remove spaces, hyphens, etc.
            code = re.sub(r'[^A-Z0-9]', '', code_text.upper())
            
            # Validate the fiscal code
            is_valid = self._validate_checksum(code)
            
            # Apply the scoring logic according to requirements
            if is_valid and has_context:
                # Condition 3: Fiscal code with keyword and code is correct -> high score (> 0.8)
                result.score = 0.9
            elif is_valid and not has_context:
                # Condition 1: Fiscal code with no keyword but code is correct -> score > 0.7
                result.score = 0.75
            elif not is_valid and has_context:
                # Condition 2: Fiscal code with keyword but code is not correct -> score < 0.7
                result.score = 0.6
            else:
                # Fiscal code with no keyword and code is not valid -> low score
                result.score = 0.4
                
        return results

    def _validate_checksum(self, code: str) -> bool:
        """
        Validates the checksum for the given Italian fiscal code.
        """
        if len(code) != 16:
            return False

        # Calculate checksum
        checksum_sum = 0
        for i, char in enumerate(code[:-1]):
            if i % 2 == 0:  # Odd positions (0-indexed)
                checksum_sum += self.ODD_VALUES.get(char, 0)
            else:  # Even positions
                checksum_sum += self.EVEN_VALUES.get(char, 0)

        # Calculate checksum character
        checksum_char = chr((checksum_sum % 26) + ord('A'))

        # Validate checksum
        return checksum_char == code[-1]
