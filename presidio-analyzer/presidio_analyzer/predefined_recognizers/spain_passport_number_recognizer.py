from typing import Optional, List, Dict, Any
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import re
import logging

logger = logging.getLogger("presidio-analyzer")


class SpainPassportRecognizer(PatternRecognizer):
    logger.info("Initializing Spain Passport Recognizer...")

    # Tightened patterns with priority ordering
    PATTERNS = [
        Pattern(
            "Spain Passport (3 letters + 6 digits)",
            r"\b[A-Z]{3}[0-9]{6}\b",  # ACB654789 format
            0.9
        ),
        Pattern(
            "Spain Passport (2 letters + 6 digits)",
            r"\b[A-Z]{2}[0-9]{6}\b",  # AB654789 format
            0.9
        ),
        Pattern(
            "Spain Passport (with hyphen)",
            r"\b[A-Z]{2,3}-[0-9]{6}\b",  # AB-654789 format
            0.9
        )
        # Removed the generic pattern to avoid false positives
    ]

    # Expanded context keywords
    CONTEXT_KEYWORDS = [
        "passport", "passport number", "passport no", "passport #",
        "número de pasaporte", "spanish passport", "pasaporte",
        "spain passport", "spain passport number"
    ]

    # Date patterns
    DATE_PATTERN = r"\b(?:\d{2}-\d{2}-\d{4}|\d{2}/\d{2}/\d{4})\b"

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "SPAIN_PASSPORT",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT_KEYWORDS
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def analyze(
        self, text: str, entities: List[str], nlp_artifacts=None
    ) -> List[RecognizerResult]:
        # First get all potential matches
        preliminary_results = super().analyze(text, entities, nlp_artifacts)
        
        if not preliminary_results:
            return []

        # Deduplicate results - keep only the highest confidence match per entity
        unique_results: Dict[tuple, RecognizerResult] = {}
        for result in preliminary_results:
            key = (result.start, result.end)  # Position-based deduplication
            if key not in unique_results or result.score > unique_results[key].score:
                unique_results[key] = result

        final_results = list(unique_results.values())
        
        # Context analysis
        lower_text = text.lower()
        has_keyword = any(
            keyword in lower_text 
            for keyword in self.CONTEXT_KEYWORDS
        )
        has_date = re.search(self.DATE_PATTERN, text) is not None

        # Adjust confidence
        for result in final_results:
            if has_keyword and has_date:
                result.score = 1.0
            elif has_keyword:
                result.score = min(result.score + 0.2, 0.9)
            elif result.score > 0.7:
                result.score = 0.6  # Reduce confidence if no context

        return final_results
