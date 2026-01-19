from presidio_analyzer import PatternRecognizer, Pattern, RecognizerResult
from typing import List, Optional
import re

class NetherlandsVATRecognizer(PatternRecognizer):
    # Microsoft Purview pattern: NL prefix + 9 digits + 'B' + 2 digits with optional separators
    PATTERNS = [
        Pattern(
            "Netherlands VAT Number",
            r"\b[Nn][Ll][\s.-]?\d{9}[\s.-]?[Bb]\d{2}\b",
            0.5  # Base score (medium)
        )
    ]

    CONTEXT_KEYWORDS = [
        "btw", "vat number", "vat id", "Netherlands VAT", "BTW-nummer", "btw-id",
        "btw nummer", "btw identificatienummer"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        super().__init__(
            supported_entity="NETHERLANDS_VAT_NUMBER",
            supported_language=supported_language,
            patterns=self.PATTERNS,
            context=self.CONTEXT_KEYWORDS
        )

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        results = super().analyze(text, entities, nlp_artifacts)
        enhanced_results = []

        for result in results:
            vat_number_raw = text[result.start:result.end]
            normalized = self._normalize_vat(vat_number_raw)

            if self._validate_checksum(normalized):
                # Boost score based on context
                result = self._enhance_confidence(text, result, high_base=True)
            else:
                result.score = 0.0

            enhanced_results.append(result)

        return enhanced_results

    def _normalize_vat(self, vat: str) -> str:
        """Remove separators and make uppercase."""
        return re.sub(r'[\s.-]', '', vat).upper()

    def _validate_checksum(self, vat_number: str) -> bool:
        """
        Validate using Modulus 11 on first 9 digits (NLxxxxxxxxxBxx).
        """
        match = re.match(r'^NL(\d{9})B\d{2}$', vat_number)
        if not match:
            return False

        digits = match.group(1)
        total = sum(int(digits[i]) * (9 - i) for i in range(9))
        return total % 11 == 0

    def _enhance_confidence(self, text: str, result: RecognizerResult, high_base: bool = False) -> RecognizerResult:
        """Boost score if context keyword found nearby."""
        window = 50
        surrounding = text[max(0, result.start - window):min(len(text), result.end + window)].lower()
        context_found = any(k.lower() in surrounding for k in self.CONTEXT_KEYWORDS)

        if context_found:
            result.score = 1.0 if high_base else 0.9
        else:
            result.score = 0.7 if high_base else 0.5

        return result

