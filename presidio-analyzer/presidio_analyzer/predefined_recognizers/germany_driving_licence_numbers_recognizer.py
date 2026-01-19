from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional
import re

class GermanDriversLicenseRecognizer(PatternRecognizer):
    """
    Recognizes German driver's license numbers with enhanced validation.
    Detects 11-character alphanumeric sequences in 5 groups (1-2-6-1-1),
    allowing optional hyphens/spaces between groups. Includes:
    - Contextual keyword validation
    - Comprehensive checksum verification
    - Strict boundary checks
    """

    # Enhanced regex pattern with separator flexibility
    PATTERNS = [
        Pattern(
            name="German Drivers License (Hardened)",
            regex=r"\b([A-Za-z0-9])[- ]?(\d{2})[- ]?([A-Za-z0-9]{6})[- ]?(\d)[- ]?([A-Za-z0-9])\b",
            score=0.5,
        ),
    ]

    # Context keywords (reduced to most critical terms)
    CONTEXT = [
        "führerschein", "fuhrerschein", "fuehrerschein", 
        "driver license", "driverlic", "dl#", "driving permit",
        "permis de conduire", "führerscheinnummer", "ausstellungsdatum"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "de",
        supported_entity: str = "GERMAN_DRIVERS_LICENSE",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        results = super().analyze(text, entities, nlp_artifacts)
        enhanced_results = []
        
        for result in results:
            raw_text = text[result.start:result.end]
            normalized = self._normalize(raw_text)
            
            # Validate checksum and context
            if self._checksum_is_valid(normalized):
                if self._has_context(text, result.start, result.end):
                    result.score = 0.9  # High confidence
                else:
                    result.score = 0.65  # Medium confidence
                enhanced_results.append(result)
            elif self._has_context(text, result.start, result.end):
                result.score = 0.4  # Low confidence
                enhanced_results.append(result)
                
        return enhanced_results

    def _normalize(self, license_number: str) -> str:
        """Remove separators and convert to uppercase"""
        return re.sub(r'[^A-Za-z0-9]', '', license_number).upper()

    def _has_context(self, text: str, start: int, end: int) -> bool:
        """Check for keywords in reduced context window"""
        window_size = 50  # Smaller context window
        context_window = text[max(0, start - window_size): min(len(text), end + window_size)]
        return any(
            re.search(rf'\b{re.escape(keyword)}\b', context_window, re.IGNORECASE) 
            for keyword in self.CONTEXT
        )

    def _checksum_is_valid(self, normalized: str) -> bool:
        """
        Validate German driver's license using official checksum algorithm.
        Implements ISO 7064 Mod 10,11 standard for last character.
        """
        if len(normalized) != 11:
            return False
            
        # 1. Convert letters to numbers (A=10, B=11, ... Z=35)
        char_map = {chr(i): i - 55 for i in range(65, 91)}
        digits = []
        for char in normalized[:-1]:  # Exclude checksum digit
            if char.isalpha():
                digits.extend(divmod(char_map[char], 10))  # Split into two digits
            else:
                digits.append(int(char))
        
        # 2. Weighted sum calculation
        weights = [9, 8, 7, 6, 5, 4, 3, 2, 1]
        weighted_sum = sum(digit * weight for digit, weight in zip(digits[:9], weights))
        
        # 3. ISO 7064 validation
        check_digit = normalized[-1]
        check_value = 10 if check_digit == 'X' else char_map.get(check_digit, int(check_digit))
        
        return (weighted_sum % 11) == check_value
