from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import re

class MedicalRecordNumberRecognizer(PatternRecognizer):
    """
    Recognizes Medical Record Numbers (MRN) and scores them based on context and validity.
    Allows flexible separators (space, dash, colon) after keywords.
    """

    PATTERNS = [
        Pattern(
            "MRN with context",
            r"(?:(Medical\sRecord\sNumber|Person\sNumber|MRN)[\s:-]+[0-9]{6})",
            0.9,
        ),
        Pattern(
            "MRN without context",
            r"\b[0-9]{6}\b",
            0.5,
        ),
    ]

    CONTEXT = [
        "medical record number",
        "person number",
        "mrn",
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "MRN",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def _is_valid_mrn(self, mrn: str) -> bool:
        """
        For testing: any 6-digit number is considered valid.
        """
        return mrn.isdigit() and len(mrn) == 6

    def enhance_pattern_result(
        self, text: str, pattern_result: RecognizerResult
    ) -> Optional[RecognizerResult]:
        """
        Adjust the score based on context presence and MRN validity.
        """

        entity_text = text[pattern_result.start:pattern_result.end]

        # Extract just the 6-digit number
        mrn_match = re.search(r"\b[0-9]{6}\b", entity_text)
        mrn = mrn_match.group(0) if mrn_match else None

        # Check for context words within a 50-character window
        context_window = text[max(0, pattern_result.start-50):pattern_result.end+50].lower()
        has_context = any(ctx in context_window for ctx in self.CONTEXT)

        if mrn:
            is_valid = self._is_valid_mrn(mrn)

            if has_context and not is_valid:
                # Case 1: keywords + incorrect MRN
                pattern_result.score = min(pattern_result.score, 0.6)
            elif not has_context and is_valid:
                # Case 2: no keywords + correct MRN
                pattern_result.score = max(pattern_result.score, 0.75)
            elif has_context and is_valid:
                # Case 3: keywords + correct MRN
                pattern_result.score = max(pattern_result.score, 0.9)

        # Cap at 1.0
        pattern_result.score = min(pattern_result.score, 1.0)

        return pattern_result

