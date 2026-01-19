from presidio_analyzer import Pattern, PatternRecognizer
from typing import List, Optional

class HealthcareTermsRecognizer(PatternRecognizer):
    PATTERNS = [
        Pattern("SSN (medium)", r"\b([0-9]{3})[- ]([0-9]{2})[- ]([0-9]{4})\b", 0.85),
    ]

    CONTEXT = [
        "social",
        "security",
        # "sec", # Task #603: Support keyphrases ("social sec")
        "ssn",
        "ssns",
        "ssn#",
        "ss#",
        "ssid",
        "social security number",
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "US_SSN",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    # def validate_healthcare_entity(self, text: str) -> bool:
    #     """
    #     Validate if the detected healthcare entity is valid.

    #     :param text: Text detected by regex
    #     :return: True if valid, False otherwise
    #     """
    #     text_upper = text.upper()

    #     # Check if the entity is a recognized drug or ingredient
    #     if text_upper in drugs or text_upper in ingredients:
    #         return True

    #     # Check if the entity is a recognized ICD-9 or ICD-10 code
    #     if text_upper in icd10_codes or text_upper in icd9_codes:
    #         return True

    #     return False

    # def invalidate_result(self, pattern_text: str) -> bool:
    #     """
    #     Check if the pattern text cannot be validated as a healthcare entity.

    #     :param pattern_text: Text detected as pattern by regex
    #     :return: True if invalidated
    #     """
    #     # Invalidate if it does not match any known healthcare entities
    #     if not self.validate_healthcare_entity(pattern_text):
    #         return True
        
    #     return False
