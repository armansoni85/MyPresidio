import re
import logging
from typing import List, Optional
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from presidio_analyzer.nlp_engine import NlpArtifacts

logger = logging.getLogger("presidio-analyzer")
logger.setLevel(logging.DEBUG)


class SSN_Formatted_Unformatted_Recognizer(PatternRecognizer):
    """
    Detects US SSNs (formatted and unformatted) with:
    - strict numeric validation
    - known test SSNs rejected
    - suspicious detection (repeated/sequential)
    - proper scoring based on context and validity
    """

    PATTERNS = [
        Pattern(
            "SSN formatted with hyphen",
            r"(?<![!@#$%&*()_+|`\\\/;\"'<>\?~A-Za-z0-9])(?:^|[=:\[\{\- ])\d{3}-\d{2}-\d{4}(?=$|[.\]\}, ])",
            0.85,
    ),

        Pattern(
            "SSN formatted with spaces",
            r"(?<![!@#$%&*()_+|`\\\/;\"'<>\?~A-Za-z0-9])(?:^|[=:\[\{\- ])\d{3} \d{2} \d{4}(?=$|[.\]\}, ])",
            0.85,
    ),

        Pattern(
            "SSN unformatted",
            r"(?<![!@#$%&*()_+|`\\\/;\"'<>\?~A-Za-z0-9])(?:^|[=:\[\{\- ])\d{9}(?=$|[.\]\}, ])",
            0.85,
    ),


    ]

    CONTEXT = [
        "ssn", "social security", "social-security", "tax id", "social",
        "ssa number", "ssn#", "social security#", "security number",
        "ss number", "ssn no", "social security no", "ssn id", "social security id"
    ]

    KNOWN_INVALID_SSNS = {
        "123456789", "078051120", "111111111", "999999999",
        "000000000", "123123123", "456456456", "789789789"
    }

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "US_formatted_unformatted_SSN",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def analyze(self, text: str, entities: List[str], nlp_artifacts: NlpArtifacts = None):
        # First get results from the parent class
        results = super().analyze(text, entities, nlp_artifacts)
        
        if not results:
            return []

        # Apply additional validation and scoring
        enhanced_results = []
        for result in results:
            pattern_text = text[result.start:result.end]
            
            # Validate the SSN
            validity = self.validate_ssn(pattern_text)
            has_context = self._has_context(text, result.start, result.end)
            
            # Adjust score based on validation and context
            adjusted_score = self._calculate_adjusted_score(result.score, validity, has_context)
            
            # Create enhanced result
            enhanced_result = RecognizerResult(
                entity_type=result.entity_type,
                start=result.start,
                end=result.end,
                score=adjusted_score,
                analysis_explanation=result.analysis_explanation,
                recognition_metadata={
                    "pattern_name": getattr(result, 'pattern_name', 'unknown'),
                    "validity": validity,
                    "has_context": has_context,
                    "original_score": result.score,
                }
            )
            enhanced_results.append(enhanced_result)

        return enhanced_results

    def _has_context(self, text: str, start: int, end: int) -> bool:
        """Check if there is relevant context around the detected pattern."""
        window_size = 100
        context_start = max(0, start - window_size)
        context_end = min(len(text), end + window_size)
        context_window = text[context_start:context_end].lower()
        
        return any(context_word.lower() in context_window for context_word in self.CONTEXT)

    def validate_ssn(self, ssn: str) -> str:
        """
        Returns:
          - "valid"      -> passes strict rules
          - "suspicious" -> repeated/sequential digits
          - "invalid"    -> fails numeric rules or known fake/test SSNs
        """
        digits = re.sub(r"\D", "", ssn)
        if len(digits) != 9:
            return "invalid"

        # All identical digits -> invalid
        if len(set(digits)) == 1:
            return "invalid"

        # Reject known test/example SSNs
        if digits in self.KNOWN_INVALID_SSNS:
            return "invalid"

        # Split parts
        area, group, serial = int(digits[:3]), int(digits[3:5]), int(digits[5:])

        # Numeric rules
        if area < 1 or area > 899 or area == 666:
            return "invalid"
        if group < 1 or group > 99:
            return "invalid"
        if serial < 1 or serial > 9999:
            return "invalid"

        # Suspicious heuristics
        if digits[:3] == digits[3:6] == digits[6:9]:  # Repeated pattern
            return "suspicious"
        if digits in "0123456789" or digits in "9876543210":  # Sequential
            return "suspicious"

        return "valid"

    def _calculate_adjusted_score(self, original_score: float, validity: str, has_context: bool) -> float:
        """Calculate adjusted score based on validation and context."""
        if validity == "invalid":
            return 0.1  # Very low score for invalid SSNs
        
        base_score = original_score
        
        # Apply context boost
        if has_context:
            base_score = min(base_score + 0.3, 1.0)
        
        # Apply validation adjustments
        if validity == "suspicious":
            base_score = base_score * 0.7  # Reduce score for suspicious SSNs
        elif validity == "valid":
            base_score = min(base_score + 0.1, 1.0)  # Slight boost for valid SSNs
        
        return max(0.0, min(1.0, base_score))  # Ensure score is between 0-1
