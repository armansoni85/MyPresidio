from presidio_analyzer import PatternRecognizer, Pattern
from typing import Optional, List
import re

class UKTaxpayerRecognizer(PatternRecognizer):
    # Define patterns for UK Taxpayer ID numbers
    PATTERNS = [
        Pattern(
            "UK Taxpayer ID (continuous)",
            r"\b\d{10}\b",  # 10 digits together
            0.5  # Initial confidence score for pattern match
        ),
        Pattern(
            "UK Taxpayer ID (split)",
            r"\b\d{5} \d{5}\b",  # 5 digits, space, 5 digits
            0.5  # Initial confidence score for pattern match
        ),
    ]

    # Context keywords from Microsoft Purview for UK UTR numbers
    CONTEXT = [
        "unique taxpayer reference", "UTR", "taxpayer reference", "tax reference",
        "self assessment", "national insurance", "HMRC"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        super().__init__(
            supported_entity="UK_TAXPAYER_ID",
            supported_language=supported_language,
            patterns=self.PATTERNS,
            context=self.CONTEXT
        )

    def enhance_confidence(self, text, pattern_result):
        """
        Enhance confidence based on proximity to UK-specific context keywords.
        """
        context_window = 50  # Check within 50 characters before and after
        start, end = pattern_result.start, pattern_result.end
        surrounding_text = text[max(0, start - context_window):min(len(text), end + context_window)].lower()

        # Check for keywords in proximity
        keyword_present = any(keyword.lower() in surrounding_text for keyword in self.CONTEXT)

        # Adjust confidence levels based on the proximity of keywords
        if keyword_present:
            pattern_result.score = 0.85  # Set confidence to 0.85 if context is found
        else:
            pattern_result.score = 0.5  # Retain initial score if no context is found

        return pattern_result

    def analyze(self, text, entities, nlp_artifacts=None):
        """
        Override the analyze method to enhance results with contextual information.
        """
        results = super().analyze(text, entities, nlp_artifacts)
        enhanced_results = [self.enhance_confidence(text, result) for result in results]
        return enhanced_results
