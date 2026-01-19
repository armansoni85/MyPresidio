from presidio_analyzer import PatternRecognizer, Pattern
import re
from typing import List, Optional

class ItalyBICSwiftRecognizer(PatternRecognizer):
    # Define the pattern for detecting Italy BIC/SWIFT numbers
    PATTERNS = [
        Pattern(
            "Italy BIC/SWIFT",
            r"\b[A-Z]{4}IT[A-Z0-9]{2}(?:[A-Z0-9]{3})?\b",
            0.5
        )
    ]

    def __init__(self, supported_language: Optional[str] = None):
        # Initialize the recognizer with the entity name and patterns
        super().__init__(
            supported_entity="ITALY_BIC_SWIFT",
            supported_language=supported_language,
            patterns=self.PATTERNS
        )

    def enhance_confidence(self, text, pattern_result):
        """
        Enhance confidence based on proximity to keywords related to banking.
        """
        context_window = 50  # Check within 50 characters before and after
        start, end = pattern_result.start, pattern_result.end
        surrounding_text = text[max(0, start - context_window):min(len(text), end + context_window)].lower()

        # Common banking-related keywords
        keywords = ['bic', 'swift', 'bank', 'transfer', 'account']

        # Check for keywords in proximity
        keyword_present = any(keyword in surrounding_text for keyword in keywords)

        # Adjust confidence based on proximity of keywords
        if keyword_present:
            pattern_result.score = 1.0  # High confidence
        else:
            pattern_result.score = 0.75  # Default/Medium confidence

        return pattern_result

    def analyze(self, text, entities, nlp_artifacts=None):
        """
        Override the analyze method to enhance results with contextual information.
        """
        results = super().analyze(text, entities, nlp_artifacts)
        enhanced_results = [self.enhance_confidence(text, result) for result in results]
        return enhanced_results
