import re
from datetime import datetime
from typing import List, Optional
from presidio_analyzer import PatternRecognizer, Pattern, RecognizerResult
import inspect


class UKDriversLicenseRecognizer(PatternRecognizer):
    """
    Recognizer for UK Driver's License numbers (16- and 18-character formats)
    with validation and context-based scoring.
    """

    PATTERNS = [
        Pattern(
            "UK Driver License 16-char Pattern",
            r"\b[A-Z9]{5}\d{7}[A-Z9]{2}[A-Z0-9]{3}\b",
            0.6
        ),
        Pattern(
            "UK Driver License 18-char Pattern",
            r"\b[A-Z9]{5}\d{7}[A-Z9]{2}[A-Z0-9]{5}\b",
            0.7
        ),
        Pattern(
            "UK Driver License Detailed Pattern",
            r"\b[A-Z9]{5}\d(0[1-9]|1[0-2]|5[1-9]|6[0-2])([0][1-9]|[12]\d|3[01])\d[A-Z9]{2}[A-Z0-9]{2,5}\b",
            0.8
        ),
    ]

    CONTEXT = [
        "driver's licence", "driving licence", "licence number", "UK license",
        "UK driving licence", "DVLA", "driver license", "driving license",
        "licence card", "photocard licence"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        super().__init__(
            supported_entity="UK_DRIVERS_LICENSE",
            supported_language=supported_language,
            patterns=self.PATTERNS,
            context=self.CONTEXT
        )

        # ✅ Detect if RecognizerResult supports 'text' parameter
        self._recognizer_has_text_param = 'text' in inspect.signature(RecognizerResult.__init__).parameters

    # -------------------------------------------------------------------------
    def _make_result(self, start, end, score, text_str):
        """Safely create RecognizerResult, compatible with all Presidio versions."""
        kwargs = dict(entity_type="UK_DRIVERS_LICENSE", start=start, end=end, score=score)
        if self._recognizer_has_text_param:
            kwargs["text"] = text_str
        return RecognizerResult(**kwargs)

    # -------------------------------------------------------------------------
    def validate_result(self, pattern_result, full_text: Optional[str] = None) -> bool:
        """Validate UK Driver's License number."""
        # Extract text safely
        if isinstance(pattern_result, RecognizerResult):
            if hasattr(pattern_result, "text") and pattern_result.text:
                text = pattern_result.text
            elif full_text:
                text = full_text[pattern_result.start:pattern_result.end]
            else:
                return False
        elif isinstance(pattern_result, str):
            text = pattern_result
        else:
            return False

        text_upper = text.strip().upper()

        # Length check
        if len(text_upper) not in [16, 18]:
            return False

        # Validate surname part
        if not all(c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ9" for c in text_upper[:5]):
            return False

        try:
            decade_digit = int(text_upper[5])
            month_part = text_upper[6:8]
            day_part = text_upper[8:10]
            year_digit = int(text_upper[10])

            current_year = datetime.now().year
            century = (current_year // 100) * 100
            full_year = century + decade_digit * 10 + year_digit

            month_num = int(month_part)
            is_female = month_num >= 51
            if is_female:
                month_num -= 50

            datetime(full_year, month_num, int(day_part))  # Validate date
        except Exception:
            return False

        initials_part = text_upper[11:13]
        if not all(c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ9" for c in initials_part):
            return False

        return True

    # -------------------------------------------------------------------------
    def enhance_confidence(self, text: str, pattern_result) -> RecognizerResult:
        """Adjust score based on keywords and validity."""
        if isinstance(pattern_result, RecognizerResult):
            if hasattr(pattern_result, "text") and pattern_result.text:
                result_text = pattern_result.text
            else:
                result_text = text[pattern_result.start:pattern_result.end]
            start, end, score = pattern_result.start, pattern_result.end, pattern_result.score
        elif isinstance(pattern_result, str):
            result_text, start, end, score = pattern_result, 0, len(pattern_result), 0.5
        else:
            return self._make_result(0, 0, 0.0, "")

        # Context check
        context_window = 50
        surrounding_text = text[max(0, start - context_window):min(len(text), end + context_window)].lower()
        keyword_present = any(k in surrounding_text for k in self.CONTEXT)
        is_valid = self.validate_result(result_text)

        # Custom scoring rules
        if keyword_present and not is_valid:
            new_score = min(score, 0.65)      # Case 1
        elif not keyword_present and is_valid:
            new_score = max(score, 0.75)      # Case 2
        elif keyword_present and is_valid:
            new_score = min(max(score + 0.25, 0.85), 1.0)  # Case 3
        else:
            new_score = score * 0.6

        return self._make_result(start, end, new_score, result_text)

    # -------------------------------------------------------------------------
    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        results = super().analyze(text, entities, nlp_artifacts)
        enhanced_results = []
        for result in results:
            enhanced = self.enhance_confidence(text, result)
            if enhanced.score > 0.4:
                enhanced_results.append(enhanced)
        return enhanced_results

