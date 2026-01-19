from presidio_analyzer import PatternRecognizer, Pattern, RecognizerResult
from typing import List, Optional
import re

class NetherlandsPassportRecognizer(PatternRecognizer):
    # Define the pattern for Netherlands Passport numbers: 9 alphanumeric characters
    PATTERNS = [
        Pattern("Netherlands Passport Number", r"\b[A-Z0-9]{9}\b", 0.1)  # Default low confidence
    ]
    
    # Keywords related to Netherlands passports
    CONTEXT_KEYWORDS = [
        "passport number", "Netherlands passport", "Dutch passport", "travel document",
        "passport no", "passport", "Nederlands paspoort", "paspoort nummer"
    ]
    
    # Date patterns for Netherlands (DD MMM or MMM YYYY)
    DATE_PATTERNS = [
        r"\b\d{2} [A-Z]{3}\b",  # DD MMM (e.g., 26 MAR)
        r"\b[A-Z]{3} \d{4}\b"  # MMM YYYY (e.g., MAR 2012)
    ]
    
    def __init__(self, supported_language: Optional[str] = None):
        super().__init__(
            supported_entity="NETHERLANDS_PASSPORT",
            supported_language=supported_language,
            patterns=self.PATTERNS,
            context=self.CONTEXT_KEYWORDS
        )

    def enhance_confidence(self, text: str, pattern_result: RecognizerResult) -> RecognizerResult:
        """
        Enhance confidence based on proximity to context keywords and Netherlands date format.
        """
        context_window = 50  # Number of characters before and after the detected passport number
        start, end = pattern_result.start, pattern_result.end
        surrounding_text = text[max(0, start - context_window):min(len(text), end + context_window)].lower()

        # Check for context keywords
        context_present = any(keyword.lower() in surrounding_text for keyword in self.CONTEXT_KEYWORDS)

        # Check for Netherlands date format
        date_present = any(re.search(date_pattern, surrounding_text) for date_pattern in self.DATE_PATTERNS)

        # Adjust confidence based on the presence of context and date phrases
        if context_present and date_present:
            pattern_result.score = 0.9  # High confidence if both keyword and date format present
        elif context_present:
            pattern_result.score = 0.7  # Medium confidence if only keyword present
        else:
            pattern_result.score = 0.3  # Low confidence without context or date format

        return pattern_result

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        """
        Override the analyze method to enhance results with contextual information.
        """
        results = super().analyze(text, entities, nlp_artifacts)
        enhanced_results = [self.enhance_confidence(text, result) for result in results]
        return enhanced_results
