import re
import logging
from typing import List, Optional, Tuple, Dict
from presidio_analyzer import PatternRecognizer, RecognizerResult, Pattern

logger = logging.getLogger("pci-dss-ccn")
logger.setLevel(logging.DEBUG)


class PCI_DSS_CreditCardAndTrackDataRecognizer(PatternRecognizer):
    """
    PCI DSS Credit Card & Track Data recognizer with:
    - LUHN-10 validation (logged)
    - test card blocking
    - strict word-boundary context (no substrings)
    - absolute score control
    """

    CREDIT_CARD_PATTERN = r"\b(?:\d{4}(?:[ -]\d{4}){2,4}|\d{13,19})\b"
    TRACK_DATA_PATTERN = r'"Credit_Card_Number"\s*s\s*"\s*(\d{13,19})\s*"'

    # 🔹 Expanded contexts + issuer names
    CONTEXT_TERMS = [
        # generic CC context
        "credit", "ccn", "card", "debit", "payment", "pay", "billing", "charge",
        "charged", "transaction", "txn", "purchase", "checkout",
        "subscription", "invoice", "account", "acct",
        "card number", "card no", "cvv", "cvc", "expiry", "expiration",

        # issuer names
        "visa",
        "mastercard", "master card",
        "american express", "amex",
        "discover",
        "diners", "diners club",
        "jcb",
        "unionpay", "union pay", "cup"
    ]

    TEST_CARDS = {
        "4111111111111111",
        "4012888888881881",
        "5555555555554444",
        "378282246310005",
        "371449635398431",
        "6011111111111117"
    }

    CARD_ISSUERS = [
        ("American Express", [("34",), ("37",)], [15]),
        ("Diner's Club", [("300-305",), ("36",), ("38",), ("39",)], [14, 16]),
        ("Discover", [("6011",), ("622126-622925",), ("644-649",), ("65",)], [16, 19]),
        ("JCB", [("3528-3589",)], [16]),
        ("MasterCard", [("51-55",), ("2221-2720",)], [16]),
        ("Visa", [("4",)], [13, 16, 19]),
        ("UnionPay", [("62",), ("81",)], [16, 17, 18, 19])
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("CREDIT_CARD", self.CREDIT_CARD_PATTERN, 0.01)
        ]
        super().__init__(
            supported_entity="PCI_DSS_CREDIT_CARD_OR_TRACK_DATA",
            patterns=patterns,
            supported_language=supported_language,
            context=[]
        )

    # -------------------------------------------------
    # Helpers
    # -------------------------------------------------

    def is_test_card(self, number: str) -> bool:
        return number in self.TEST_CARDS or len(set(number)) == 1

    def has_context(self, text: str, start: int, end: int) -> bool:
        window = text[max(0, start - 50): min(len(text), end + 50)].lower()

        for term in self.CONTEXT_TERMS:
            if re.search(rf"\b{re.escape(term)}\b", window):
                return True
        return False

    def luhn_checksum(self, number: str) -> bool:
        total = 0
        for i, d in enumerate(number[::-1]):
            n = int(d)
            if i % 2 == 1:
                n *= 2
                if n > 9:
                    n -= 9
            total += n

        valid = total % 10 == 0
        logger.debug(f"[LUHN] Card: {number} | {'VALID' if valid else 'INVALID'}")
        return valid

    def is_valid_issuer(self, ccn: str) -> bool:
        for _, iin_ranges, lengths in self.CARD_ISSUERS:
            if len(ccn) not in lengths:
                continue
            for spec in iin_ranges:
                if self._check_iin_range(ccn, spec):
                    return True
        return False

    def _check_iin_range(self, ccn: str, range_spec: Tuple[str]) -> bool:
        prefix = ccn[:6]
        for spec in range_spec:
            if "-" in spec:
                start, end = spec.split("-")
                size = max(len(start), len(end))
                test = prefix[:size]
                if int(start) <= int(test) <= int(end):
                    return True
            else:
                if ccn.startswith(spec):
                    return True
        return False

    def is_valid_credit_card(self, ccn: str) -> bool:
        if not (13 <= len(ccn) <= 19):
            return False
        if self.is_test_card(ccn):
            logger.debug(f"[BLOCKED] Test card detected: {ccn}")
            return False
        if not self.is_valid_issuer(ccn):
            logger.debug(f"[ISSUER] Invalid issuer: {ccn}")
            return False
        return self.luhn_checksum(ccn)

    # -------------------------------------------------
    # Analyze (absolute scoring)
    # -------------------------------------------------

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:

        raw_results = super().analyze(text, entities, nlp_artifacts)
        hits: Dict[str, Dict] = {}

        for r in raw_results:
            raw = text[r.start:r.end]
            digits = re.sub(r"\D", "", raw)

            if not self.is_valid_credit_card(digits):
                continue

            hits[digits] = {
                "result": r,
                "formatted": bool(re.search(r"[ -]", raw)),
                "context": self.has_context(text, r.start, r.end)
            }

        for m in re.finditer(self.TRACK_DATA_PATTERN, text):
            digits = m.group(1)

            if not self.is_valid_credit_card(digits):
                continue

            hits[digits] = {
                "result": RecognizerResult(
                    entity_type=self.supported_entity,
                    start=m.start(1),
                    end=m.end(1),
                    score=0.0
                ),
                "formatted": False,
                "context": True
            }

        multiple = len(hits) > 1
        final_results: List[RecognizerResult] = []

        for digits, data in hits.items():
            r = data["result"]

            if multiple:
                score = 0.75
            else:
                score = 0.6 if data["formatted"] else 0.4

            if data["context"]:
                score += 0.5

            r.score = min(score, 1.25)
            r.metadata = {
                "digits": digits,
                "formatted": data["formatted"],
                "context": data["context"]
            }

            final_results.append(r)

        return final_results
