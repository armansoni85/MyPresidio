import re
import logging
from typing import List, Optional
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from presidio_analyzer.nlp_engine import NlpArtifacts

logger = logging.getLogger("presidio-analyzer")
logger.setLevel(logging.DEBUG)


class DatotelCreditDebitCardRecognizer(PatternRecognizer):
    """
    Custom recognizer that:
    - Detects SSN and Credit/Debit Card numbers
    - Validates SSN structure and card numbers via Luhn algorithm
    - Returns both entities only if both are valid and score > 0.7
    - Otherwise assigns lower scores (<0.7)
    """

    SSN_REGEXES = [
        r"\b\d{3}-\d{2}-\d{4}\b",
        r"\b\d{3}\s\d{2}\s\d{4}\b",
        r"\b\d{9}\b",
    ]

    CARD_REGEXES = [
        r"\b(?:\d{4}[-\s]?){3}\d{4}\b",
        r"\b\d{16}\b",
        r"\b3[47]\d{2}[-\s]?\d{6}[-\s]?\d{5}\b",
        r"\b3[47]\d{13}\b",
    ]

    KNOWN_INVALID_SSNS = {
        "123456789", "078051120", "111111111", "999999999",
        "000000000", "123123123", "456456456", "789789789"
    }

    TEST_CARD_NUMBERS = {
        "4111111111111111", "5555555555554444", "378282246310005",
        "371449635398431", "6011111111111117", "30569309025904"
    }

    def __init__(self, supported_language="en"):
        # ✅ Provide dummy pattern to satisfy Presidio requirement
        dummy_pattern = Pattern(name="dummy", regex=r"^$", score=0.01)
        super().__init__(
            supported_entity="DATOTEL_CARD",
            supported_language=supported_language,
            patterns=[dummy_pattern],
        )

    def analyze(
        self,
        text: str,
        entities: List[str],
        nlp_artifacts: Optional[NlpArtifacts] = None
    ) -> List[RecognizerResult]:
        if not text:
            return []

        ssn_result = None
        card_result = None

        # --- Detect SSNs ---
        for pattern in self.SSN_REGEXES:
            for match in re.finditer(pattern, text):
                digits = re.sub(r"\D", "", match.group())
                if self.validate_ssn(digits):
                    ssn_result = RecognizerResult(
                        entity_type="SSN",
                        start=match.start(),
                        end=match.end(),
                        score=0.9,
                        recognition_metadata={"validity": "valid"},
                    )
                else:
                    ssn_result = RecognizerResult(
                        entity_type="SSN",
                        start=match.start(),
                        end=match.end(),
                        score=0.5,
                        recognition_metadata={"validity": "invalid"},
                    )

        # --- Detect Cards ---
        for pattern in self.CARD_REGEXES:
            for match in re.finditer(pattern, text):
                digits = re.sub(r"\D", "", match.group())
                if self.validate_card(digits):
                    card_result = RecognizerResult(
                        entity_type="Credit/Debit Card",
                        start=match.start(),
                        end=match.end(),
                        score=0.9,
                        recognition_metadata={"validity": "valid"},
                    )
                else:
                    card_result = RecognizerResult(
                        entity_type="Credit/Debit Card",
                        start=match.start(),
                        end=match.end(),
                        score=0.5,
                        recognition_metadata={"validity": "invalid"},
                    )

        # ✅ Only return both if valid and score > 0.7
        if ssn_result and card_result and ssn_result.score > 0.7 and card_result.score > 0.7:
            return [ssn_result, card_result]

        # ❌ If invalid or missing, lower scores (<0.7)
        results = []
        if ssn_result:
            ssn_result.score = min(ssn_result.score, 0.6)
            results.append(ssn_result)
        if card_result:
            card_result.score = min(card_result.score, 0.6)
            results.append(card_result)

        return results

    # ---------- HELPERS ----------

    def validate_ssn(self, digits: str) -> bool:
        if len(digits) != 9 or digits in self.KNOWN_INVALID_SSNS:
            return False
        area, group, serial = int(digits[:3]), int(digits[3:5]), int(digits[5:])
        if area < 1 or area > 899 or area == 666:
            return False
        if group < 1 or serial < 1:
            return False
        if len(set(digits)) == 1:
            return False
        return True

    def validate_card(self, digits: str) -> bool:
        if len(digits) not in (14, 15, 16):
            return False
        if digits in self.TEST_CARD_NUMBERS:
            return False
        return self._passes_luhn(digits)

    def _passes_luhn(self, number: str) -> bool:
        total = 0
        reverse_digits = number[::-1]
        for i, d in enumerate(reverse_digits):
            n = int(d)
            if i % 2 == 1:
                n *= 2
                if n > 9:
                    n -= 9
            total += n
        return total % 10 == 0

