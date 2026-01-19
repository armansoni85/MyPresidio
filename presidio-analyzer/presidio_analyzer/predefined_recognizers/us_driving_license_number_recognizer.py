from presidio_analyzer import PatternRecognizer, Pattern
import re
from typing import List, Optional

class USDriversLicenseRecognizer(PatternRecognizer):
    # Define patterns for US Driver's License numbers
    PATTERNS = [
    Pattern("Alabama, Alaska, Delaware, Georgia, Maine, Montana, Oregon, South Carolina, Washington, D.C., West Virginia", r"\b[0-9]{7}\b", 1),
    Pattern("Arizona, Massachusetts, Nebraska, Oklahoma", r"\b[A-Z][0-9]{8}\b", 1),
    Pattern("Arkansas", r"\b9[0-9]{8}\b", 1),
    Pattern("California", r"\b[A-Z][0-9]{7}\b", 1),
    Pattern("Colorado", r"\b[0-9]{2}-[0-9]{3}-[0-9]{4}\b", 1),
    Pattern("Connecticut, Louisiana, New Mexico, Idaho, Iowa, Mississippi, Oklahoma, South Dakota, Tennessee, Utah", r"\b[0-9]{9}\b", 1),
    Pattern("Florida, Maryland, Michigan, Minnesota, Nevada", r"\b[A-Z][0-9]{12}\b", 1),
    Pattern("Formatted nine digits (e.g., ddd ddd ddd)", r"\b\d{3} \d{3} \d{3}\b", 1),
    Pattern("Unformatted nine digits won't match", r"(?!\b\d{9}\b)", 0),
    Pattern("Alabama", r"^\d{1,8}$", 1),
    Pattern("Alaska", r"^\d{1,7}$", 1),
    Pattern("Arizona", r"^[A-Z]\d{8}$|^\d{9}$", 1),
    Pattern("Arkansas", r"^\d{4,9}$", 1),
    Pattern("California", r"^[A-Z]\d{7}$", 1),
    Pattern("Colorado", r"^\d{9}$|^[A-Z]\d{3,6}$|^[A-Z]{2}\d{2,5}$", 1),
    Pattern("Connecticut", r"^\d{9}$", 1),
    Pattern("Delaware", r"^\d{1,7}$", 1),
    Pattern("District of Columbia", r"^\d{7}$|^\d{9}$", 1),
    Pattern("Florida", r"^[A-Z]\d{12}$", 1),
    Pattern("Georgia", r"^\d{7,9}$", 1),
    Pattern("Hawaii", r"^[A-Z]\d{8}$|^\d{9}$", 1),
    Pattern("Idaho", r"^[A-Z]{2}\d{6}[A-Z]$|^\d{9}$", 1),
    Pattern("Illinois", r"^[A-Z]\d{11,12}$", 1),
    Pattern("Indiana", r"^[A-Z]\d{9}$|^\d{9,10}$", 1),
    Pattern("Iowa", r"^\d{9}$|^\d{3}[A-Z]{2}\d{4}$", 1),
    Pattern("Kansas", r"^[A-Z]\d[A-Z]\d[A-Z]$|^[A-Z]\d{8}$|^\d{9}$", 1),
    Pattern("Kentucky", r"^[A-Z]\d{8}$|^[A-Z]\d{9}$|^\d{9}$", 1),
    Pattern("Louisiana", r"^\d{1,9}$", 1),
    Pattern("Maine", r"^\d{7}$|^\d{7}[A-Z]$|^\d{8}$", 1),
    Pattern("Maryland", r"^[A-Z]\d{12}$", 1),
    Pattern("Massachusetts", r"^[A-Z]\d{8}$|^\d{9}$", 1),
    Pattern("Michigan", r"^[A-Z]\d{10}$|^[A-Z]\d{12}$", 1),
    Pattern("Minnesota", r"^[A-Z]\d{12}$", 1),
    Pattern("Mississippi", r"^\d{9}$", 1),
    Pattern("Missouri", r"^\d{3}[A-Z]\d{6}$|^[A-Z]\d{5,9}$|^[A-Z]\d{6}R$|^\d{8}[A-Z]{2}$|^\d{9}[A-Z]$|^\d{9}$", 1),
    Pattern("Montana", r"^[A-Z]\d{8}$|^\d{9}$|^\d{13,14}$", 1),
    Pattern("Nebraska", r"^[A-Z]\d{6,8}$", 1),
    Pattern("Nevada", r"^\d{9,10}$|^\d{12}$|^X\d{8}$", 1),
    Pattern("New Hampshire", r"^\d{2}[A-Z]{3}\d{5}$", 1),
    Pattern("New Jersey", r"^[A-Z]\d{14}$", 1),
    Pattern("New Mexico", r"^\d{8,9}$", 1),
    Pattern("New York", r"^[A-Z]\d{7}$|^[A-Z]\d{18}$|^\d{8,9}$|^\d{16}$|^[A-Z]{8}$", 1),
    Pattern("North Carolina", r"^\d{1,12}$", 1),
    Pattern("North Dakota", r"^[A-Z]{3}\d{6}$|^\d{9}$", 1),
    Pattern("Ohio", r"^[A-Z]\d{4,8}$|^[A-Z]{2}\d{3,7}$|^\d{8}$", 1),
    Pattern("Oklahoma", r"^[A-Z]\d{9}$|^\d{9}$", 1),
    Pattern("Oregon", r"^\d{1,9}$", 1),
    Pattern("Pennsylvania", r"^\d{8}$", 1),
    Pattern("Rhode Island", r"^\d{7}$|^[A-Z]\d{6}$", 1),
    Pattern("South Carolina", r"^\d{5,11}$", 1),
    Pattern("South Dakota", r"^\d{6,10}$|^\d{12}$", 1),
    Pattern("Tennessee", r"^\d{7,9}$", 1),
    Pattern("Texas", r"^\d{7,8}$", 1),
    Pattern("Utah", r"^\d{4,10}$", 1),
    Pattern("Vermont", r"^\d{8}$|^\d{7}A$", 1),
    Pattern("Virginia", r"^[A-Z]\d{8,11}$|^\d{9}$", 1),
    Pattern("Washington", r"^[A-Z]{1,7}[A-Z0-9]{5,11}$", 1),
    Pattern("West Virginia", r"^\d{7}$|^[A-Z]{1,2}\d{5,6}$", 1),
    Pattern("Wisconsin", r"^[A-Z]\d{13}$", 1),
    Pattern("Wyoming", r"^\d{9,10}$", 1),
    ]

    # Keywords and state names to raise confidence scores
    KEYWORDS = [
        'driver\'s license', 'DL', 'permit', 'license', 'identification'
    ]

    STATES = [
        'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 
        'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 
        'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 
        'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 
        'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 
        'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 
        'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 
        'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 
        'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'
    ]

    def __init__(self, supported_language: Optional[str] = None):
        super().__init__(
            supported_entity="US_DRIVERS_LICENSE",
            supported_language=supported_language,
            patterns=self.PATTERNS,
            context=None
        )

    def enhance_confidence(self, text, pattern_result):
        """
        Enhance confidence based on proximity to keywords and state names.
        """
        context_window = 50  # Check within 50 characters before and after
        start, end = pattern_result.start, pattern_result.end
        surrounding_text = text[max(0, start - context_window):min(len(text), end + context_window)].lower()

        # Check for keywords and state names in proximity
        keyword_present = any(keyword.lower() in surrounding_text for keyword in self.KEYWORDS)
        state_present = any(state.lower() in surrounding_text for state in self.STATES)

        # Adjust confidence levels based on the proximity of keywords and states
        if keyword_present and state_present:
            pattern_result.score = 1.0  # High confidence
        elif keyword_present:
            pattern_result.score = 0.75  # Medium confidence
        elif state_present:
            pattern_result.score = 0.5  # Low confidence
        else:
            pattern_result.score = 0.25  # Very low confidence

        return pattern_result

    def analyze(self, text, entities, nlp_artifacts=None):
        """
        Override the analyze method to enhance results with contextual information.
        """
        results = super().analyze(text, entities, nlp_artifacts)
        enhanced_results = [self.enhance_confidence(text, result) for result in results]
        return enhanced_results
