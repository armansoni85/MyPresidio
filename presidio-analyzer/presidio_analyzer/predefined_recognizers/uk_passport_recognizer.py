from presidio_analyzer import PatternRecognizer, Pattern
from typing import List, Optional
import re

class UKPassportRecognizer(PatternRecognizer):
    # Define the pattern for UK Passport numbers: 1 alphanumeric followed by 8 digits
    PATTERNS = [
        Pattern("UK Passport Number", r"\b[A-Z0-9]{1}[0-9]{8}\b", 0.1)  # Default score if no context is found
    ]
    
    # Context keywords related to passports
    CONTEXT_KEYWORDS = [
        "passport number", "UK passport", "travel document", "British passport", "passport no"
    ]

    # Date phrases related to passport (e.g., issuance and expiry dates)
    DATE_PHRASES = [
        "issued on", "expiry date", "valid until", "valid from"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        super().__init__(
            supported_entity="UK_PASSPORT",
            supported_language=supported_language,
            patterns=self.PATTERNS,
            context=self.CONTEXT_KEYWORDS
        )

    def enhance_confidence(self, text, pattern_result):
        """
        Enhance confidence based on proximity to context keywords and date phrases.
        """
        context_window = 50  # Number of characters before and after the detected passport number
        start, end = pattern_result.start, pattern_result.end
        surrounding_text = text[max(0, start - context_window):min(len(text), end + context_window)].lower()

        # Check for context keywords
        context_present = any(keyword.lower() in surrounding_text for keyword in self.CONTEXT_KEYWORDS)

        # Check for date phrases
        date_phrase_present = any(phrase.lower() in surrounding_text for phrase in self.DATE_PHRASES)

        # Adjust confidence based on the presence of context and date phrases
        if context_present and date_phrase_present:
            pattern_result.score = 0.85  # High confidence
        elif context_present:
            pattern_result.score = 0.75  # Medium confidence
        else:
            pattern_result.score = 0.1  # Low confidence without context

        return pattern_result

    def analyze(self, text, entities, nlp_artifacts=None):
        """
        Override the analyze method to enhance results with contextual information.
        """
        results = super().analyze(text, entities, nlp_artifacts)
        enhanced_results = [self.enhance_confidence(text, result) for result in results]
        return enhanced_results
