from presidio_analyzer import PatternRecognizer, Pattern
import re
from typing import List, Optional

class UKNINORecognizer(PatternRecognizer):
    # Define patterns for UK National Insurance Numbers with and without spaces
    PATTERNS = [
        Pattern(
            "UK NINO", 
            r"\b(?!BG|GB|KN|NK|NT|TN|ZZ)([A-CEGHJ-PR-TW-Z]{2})\s*(\d{2})\s*(\d{2})\s*(\d{2})\s*([A-D]?)\b", 
            0.75  # Initial confidence score without proximity check
        ),
        Pattern(
            "UK NINO Compact",
            r"\b(?!BG|GB|KN|NK|NT|TN|ZZ)([A-CEGHJ-PR-TW-Z]{2})(\d{6})([A-D]?)\b",
            0.75
        )
    ]

    # Context keywords related to NINO
    CONTEXT = [
        "national insurance", "NINO", "insurance number", "NI number", "N.I. number", "NI No"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        super().__init__(
            supported_entity="UK_NINO",
            supported_language=supported_language,
            patterns=self.PATTERNS,
            context=self.CONTEXT
        )

    def enhance_confidence(self, text, pattern_result):
        """
        Enhance confidence based on proximity to NINO-specific keywords.
        """
        context_window = 50  # Check within 50 characters before and after
        start, end = pattern_result.start, pattern_result.end
        surrounding_text = text[max(0, start - context_window):min(len(text), end + context_window)].lower()

        # Check for keywords in proximity
        keyword_present = any(keyword.lower() in surrounding_text for keyword in self.CONTEXT)

        # Adjust confidence levels based on the proximity of keywords
        if keyword_present:
            pattern_result.score = 0.85  # Confidence with keyword in proximity
        else:
            pattern_result.score = 0.75  # Confidence without keyword in proximity

        return pattern_result

    def analyze(self, text, entities, nlp_artifacts=None):
        """
        Override the analyze method to enhance results with contextual information.
        """
        results = super().analyze(text, entities, nlp_artifacts)
        enhanced_results = [self.enhance_confidence(text, result) for result in results]
        return enhanced_results
