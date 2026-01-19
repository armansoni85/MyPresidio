from typing import Optional, List, Any
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class CanadaPIPEDARecognizer(PatternRecognizer):
    logger.info("Initializing enhanced Canada PIPEDA Recognizer...")

    # Enhanced pattern for flexible SIN formats
    PATTERNS = [
        Pattern(
            "Canada PIPEDA - Flexible SIN",
            r"\b\d{3}[-\s]?\d{3}[-\s]?\d{3}\b", 
            0.7  # Base confidence score
        )
    ]

    # Expanded context terms for PIPEDA/SIN detection
    CONTEXT = [
        "sin", "social insurance", "pipeda", "personal information", 
        "personal data", "protection", "privacy", "electronic documents",
        "confidential", "identification", "tax id", "government id"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "CANADA_PIPEDA",
        check_luhn: bool = True  # Enable Luhn validation by default
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )
        self.check_luhn = check_luhn

    def analyze(
        self, text: str, entities: List[str], nlp_artifacts: Any = None
    ) -> List[RecognizerResult]:
        logger.info(f"Analyzing text for Canada PIPEDA: {text}")
        # First get results from the pattern recognizer
        pattern_results = super().analyze(text, entities, nlp_artifacts)
        final_results = []
        
        for result in pattern_results:
            # Extract the matched SIN string
            sin_text = text[result.start:result.end]
            logger.debug(f"Potential SIN detected: {sin_text}")
            
            # Normalize to digits only
            digits = re.sub(r"\D", "", sin_text)
            
            # Validate digit length
            if len(digits) != 9:
                logger.debug(f"Invalid digit count ({len(digits)}), skipping: {sin_text}")
                continue
                
            # Validate Luhn algorithm if enabled
            if self.check_luhn and not self._luhn_check(digits):
                logger.info(f"Luhn check failed for SIN: {sin_text}")
                continue
                
            # Check for context terms in surrounding text
            start_ctx = max(0, result.start - 100)
            end_ctx = min(len(text), result.end + 100)
            context_window = text[start_ctx:end_ctx].lower()
            
            # Boost confidence if context terms are present
            if any(keyword in context_window for keyword in self.CONTEXT):
                logger.info(f"Context found for SIN: {sin_text}")
                result.score = 1.0  # High confidence
            else:
                logger.debug(f"No context found for SIN: {sin_text}")
                # Keep base confidence (0.7) if no context
                
            final_results.append(result)
            
        return final_results

    @staticmethod
    def _luhn_check(digits: str) -> bool:
        """Validate SIN using Luhn algorithm"""
        if len(digits) != 9:
            return False
            
        total = 0
        # Process digits from right to left
        for i, char in enumerate(reversed(digits)):
            n = int(char)
            # Double every second digit (starting from rightmost+1)
            if i % 2 == 1:
                n *= 2
                # Sum digits of numbers > 9 (same as -9)
                if n > 9:
                    n -= 9
            total += n
            
        return total % 10 == 0

