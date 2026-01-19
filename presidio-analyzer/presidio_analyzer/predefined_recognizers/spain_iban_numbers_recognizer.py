from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import re
import logging

logger = logging.getLogger("presidio-analyzer")

class SpainIBANRecognizer(PatternRecognizer):
    logger.info("Initializing Spain IBAN Recognizer...")

    # Very broad pattern to catch anything that might be an IBAN
    PATTERNS = [
        Pattern(
            "Spain IBAN",
            r"\bES[0-9\s\-]+\b",  # Very broad pattern
            0.3  # Low initial confidence, will be validated later
        ),
    ]

    # Spanish and English context keywords for IBAN
    CONTEXT = [
        "IBAN", "international bank account number", "número de cuenta bancaria",
        "número IBAN", "IBAN de España", "account number", "bank account"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "es",
        supported_entity: str = "SPAIN_IBAN",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def normalize_iban(self, iban: str) -> str:
        """Remove all non-alphanumeric characters from IBAN"""
        return re.sub(r'[^A-Z0-9]', '', iban.upper())

    def is_valid_spanish_iban(self, iban: str) -> bool:
        """Check if the normalized string is a valid Spanish IBAN format"""
        normalized = self.normalize_iban(iban)
        
        # Spanish IBAN should be exactly 24 characters: ES + 22 digits
        if len(normalized) != 24:
            return False
            
        if not normalized.startswith('ES'):
            return False
            
        digits_part = normalized[2:]
        return digits_part.isdigit() and len(digits_part) == 22

    def analyze(
        self, text: str, entities: List[str], nlp_artifacts=None
    ) -> List[RecognizerResult]:
        # First get potential matches using the parent analyzer
        potential_results = super().analyze(text, entities, nlp_artifacts)
        valid_results = []
        
        for result in potential_results:
            # Extract the detected text
            detected_text = text[result.start:result.end]
            
            # Validate if it's actually a Spanish IBAN
            if self.is_valid_spanish_iban(detected_text):
                # Create a new result with higher confidence
                validated_result = RecognizerResult(
                    entity_type=result.entity_type,
                    start=result.start,
                    end=result.end,
                    score=0.8,  # Higher confidence for validated IBANs
                    analysis_explanation={
                        'original_value': detected_text,
                        'normalized_value': self.normalize_iban(detected_text)
                    }
                )
                
                # Increase the score if context keywords are found
                if any(keyword in text.lower() for keyword in self.CONTEXT):
                    validated_result.score = min(validated_result.score + 0.2, 1.0)
                
                valid_results.append(validated_result)
        
        return valid_results
