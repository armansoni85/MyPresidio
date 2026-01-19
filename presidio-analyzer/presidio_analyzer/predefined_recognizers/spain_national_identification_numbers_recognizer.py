from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class SpainDNIRecognizer(PatternRecognizer):
    logger.info("Initializing Spain DNI Recognizer...")

    PATTERNS = [
        Pattern(
            "Spain DNI - Flexible Format",
            r"\b\d{8}[-\s]?[A-HJ-NP-TV-Z]\b",
            0.7  # Base confidence
        )
    ]

    CONTEXT = [
        "DNI", "Documento Nacional de Identidad", "national ID",
        "identificación nacional", "número de identificación",
        "identification number", "national identification",
        "Spain ID", "Spanish ID", "ID Nacional", "Número de DNI",
        "identidad", "identificador personal", "identificación personal"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "es",
        supported_entity: str = "SPAIN_DNI",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def analyze(
        self, text: str, entities: List[str], nlp_artifacts=None
    ) -> List[RecognizerResult]:
        results = super().analyze(text, entities, nlp_artifacts)
        for result in results:
            dni = text[result.start:result.end].replace("-", "").replace(" ", "")
            if self._validate_dni_checksum(dni):
                result.score = self._adjust_score_based_on_context(text, result)
            else:
                result.score = 0.0
        return results

    def _validate_dni_checksum(self, dni: str) -> bool:
        try:
            dni_number = dni[:-1]
            dni_letter = dni[-1].upper()
            valid_letters = "TRWAGMYFPDXBNJZSQVHLCKE"
            mod_value = int(dni_number) % 23
            return valid_letters[mod_value] == dni_letter
        except ValueError:
            return False

    def _adjust_score_based_on_context(self, text: str, result: RecognizerResult) -> float:
        if any(keyword.lower() in text.lower() for keyword in self.CONTEXT):
            return min(result.score + 0.3, 1.0)
        return result.score

