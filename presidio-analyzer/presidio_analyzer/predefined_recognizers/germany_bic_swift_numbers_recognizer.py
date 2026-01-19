from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import re
import logging

logger = logging.getLogger("presidio-analyzer")

class GermanyBICSwiftRecognizer(PatternRecognizer):
    """
    Recognizes German BIC/SWIFT codes with enhanced validation:
    - Strict German country code (DE) requirement
    - Case-insensitive matching
    - Context-aware confidence scoring
    - Proximity-based context validation
    """
    
    logger.info("Initializing enhanced Germany BIC/SWIFT Number Recognizer...")

    # Strict patterns requiring German country code (DE)
    PATTERNS = [
        Pattern(
            "German BIC (8-char)",
            r"(?i)\b[A-Z]{4}DE[A-Z0-9]{2}\b",  # Case-insensitive, DE country code
            0.6
        ),
        Pattern(
            "German BIC (11-char)",
            r"(?i)\b[A-Z]{4}DE[A-Z0-9]{5}\b",  # Case-insensitive, DE country code
            0.6
        )
    ]

    # Context keywords with multi-language support
    CONTEXT = [
        "bic", "swift", "swift code", "bic code", "bank identifier code", 
        "bankleitzahl", "blz", "swift-nummer", "swift adresse",
        "código swift", "código bic", "código de identificación bancaria"
    ]

    # Words that invalidate the match if adjacent
    INVALIDATING_CONTEXT = [
        "phone", "mobile", "fax", "tel", "contact", "call", "email",
        "www", "http", "login", "password", "username"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "GERMANY_BIC_SWIFT",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else [kw.lower() for kw in self.CONTEXT]
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def analyze(
        self, text: str, entities: List[str], nlp_artifacts=None
    ) -> List[RecognizerResult]:
        """
        Analyze text with enhanced validation:
        1. Check for context keywords near the match
        2. Validate BIC format after match
        3. Check for invalidating context
        """
        logger.info(f"Analyzing text for Germany BIC/SWIFT: {text}")
        results = super().analyze(text, entities, nlp_artifacts)
        if not results:
            return results

        text_lower = text.lower()
        final_results = []
        
        for result in results:
            bic_swift = text[result.start:result.end]
            logger.debug(f"Potential BIC/SWIFT found: {bic_swift}")
            
            # Validate BIC structure
            if not self._is_valid_bic(bic_swift):
                logger.debug(f"Invalid BIC structure: {bic_swift}")
                continue
                
            # Check for invalidating context
            if self._has_invalidating_context(text, result):
                logger.debug(f"Invalidating context near: {bic_swift}")
                continue
                
            # Adjust confidence based on context proximity
            result.score = self._adjust_confidence(text, result, text_lower)
            final_results.append(result)
            
        return final_results

    def _is_valid_bic(self, bic: str) -> bool:
        """Validate BIC structure and country code"""
        # Normalize to uppercase and remove whitespace
        normalized = re.sub(r"\s", "", bic).upper()
        
        # Validate length
        if len(normalized) not in (8, 11):
            return False
            
        # Validate country code position (always positions 5-6)
        if normalized[4:6] != "DE":
            return False
            
        # Validate institution code (first 4 must be letters)
        if not normalized[:4].isalpha():
            return False
            
        return True

    def _adjust_confidence(
        self, 
        text: str, 
        result: RecognizerResult, 
        text_lower: str
    ) -> float:
        """
        Adjust confidence based on:
        - Presence of context keywords near match
        - Match position in text
        """
        # Check for context in proximity (20 characters before/after)
        start, end = result.start, result.end
        context_window = text_lower[max(0, start-20):min(len(text), end+20)]
        
        # Check for any context keyword in proximity
        has_context = any(
            context_word in context_window 
            for context_word in self.context
        )
        
        # Check for direct label prefix (e.g., "BIC: ABCDDEFF")
        prefix = text[max(0, start-10):start].lower()
        has_direct_prefix = any(
            prefix.endswith(keyword + punct)
            for keyword in ["bic", "swift"]
            for punct in [":", "#", " ", "-", "="]
        )
        
        # Boost confidence for direct prefix or nearby context
        if has_direct_prefix:
            logger.info(f"Direct prefix found for {text[start:end]}, confidence=1.0")
            return 1.0
        elif has_context:
            logger.info(f"Context found near {text[start:end]}, confidence=0.9")
            return 0.9
            
        # Medium confidence for standalone valid BICs
        logger.info(f"No context found for {text[start:end]}, confidence=0.7")
        return 0.7

    def _has_invalidating_context(
        self, 
        text: str, 
        result: RecognizerResult
    ) -> bool:
        """Check for invalidating terms near the match"""
        # Check 30 characters before and after
        start, end = result.start, result.end
        context_window = text.lower()[max(0, start-30):min(len(text), end+30)]
        
        return any(
            invalid_term in context_window
            for invalid_term in self.INVALIDATING_CONTEXT
        )
