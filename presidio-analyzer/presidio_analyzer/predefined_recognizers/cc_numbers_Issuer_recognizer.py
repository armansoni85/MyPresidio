import re
from typing import Optional, Dict, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult


class CreditCardIssuerRecognizer(PatternRecognizer):

    CONTEXT_TERMS = [
        "ccn", "issuer", "credit", "card", "debit", "visa", "mastercard",
        "amex", "american express", "discover", "jcb",
        "diners", "maestro", "unionpay",
        "card number", "card no", "cc#", "cvv", "cvc", "expiry", "expiration"
    ]

    TEST_CARDS = {
        "4111111111111111",
        "4012888888881881",
        "5555555555554444",
        "378282246310005",
        "371449635398431",
        "6011111111111117"
    }

    # It's issuer list 
    credit_card_issuers = [
        {"name": "American Express", "starts_with": ["34", "37"]},
        {"name": "Discover", "starts_with": ["6011", "622", "64", "65"]},
        {"name": "JCB", "starts_with": ["35"]},
        {"name": "Mastercard", "starts_with": ["51", "52", "53", "54", "55", "22", "27"]},
        {"name": "Visa", "starts_with": ["4"]},
        {"name": "China UnionPay", "starts_with": ["62"]},
        {"name": "Diners Club - Carte Blanche", "starts_with": ["300", "301", "302", "303", "304", "305"]},
        {"name": "Maestro", "starts_with": ["5018", "5020", "5038", "5893", "6304", "6759", "6761", "6762", "6763"]}
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("FORMATTED_CC", r'\b\d{4}(?:[ -]\d{4}){2,4}\b', 0.01),
            Pattern("UNFORMATTED_CC", r'\b\d{13,19}\b', 0.01),
        ]
        super().__init__(
            supported_entity="CREDIT_CARD_ISSUER",
            patterns=patterns,
            supported_language=supported_language,
            context=[]
        )

    # ---------------- helpers ----------------

    def luhn_checksum(self, number: str) -> bool:
        digits = [int(d) for d in number]
        checksum = 0
        parity = len(digits) % 2
        for i, d in enumerate(digits):
            if i % 2 == parity:
                d *= 2
                if d > 9:
                    d -= 9
            checksum += d
        return checksum % 10 == 0

    def determine_issuer(self, number: str) -> Optional[str]:
        for issuer in self.credit_card_issuers:
            for prefix in issuer["starts_with"]:
                if number.startswith(prefix):
                    return issuer["name"]
        return None

    def is_test_card(self, number: str) -> bool:
        return number in self.TEST_CARDS or len(set(number)) == 1

    def has_context(self, text: str, start: int, end: int) -> bool:
        window = text[max(0, start - 50): min(len(text), end + 50)].lower()
        for term in self.CONTEXT_TERMS:
            if re.search(rf"\b{re.escape(term)}\b", window):
                return True
        return False

    # ---------------- analyze ----------------

    def analyze(self, text, entities, nlp_artifacts=None):

        raw_results = super().analyze(text, entities, nlp_artifacts)
        hits: Dict[str, RecognizerResult] = {}
        meta: Dict[str, Dict] = {}

        for r in raw_results:
            raw = text[r.start:r.end]
            digits = re.sub(r"\D", "", raw)

            if not (13 <= len(digits) <= 19):
                continue
            if not self.luhn_checksum(digits):
                continue
            if self.is_test_card(digits):
                continue

            issuer = self.determine_issuer(digits)
            if issuer is None:
                continue

            hits[digits] = r
            meta[digits] = {
                "digits": digits,
                "issuer": issuer,
                "formatted": bool(re.search(r"[ -]", raw)),
                "context": self.has_context(text, r.start, r.end)
            }

        multiple = len(meta) > 1
        results = []

        for digits, r in hits.items():
            m = meta[digits]

            score = 0.75 if multiple else (0.6 if m["formatted"] else 0.4)
            if m["context"]:
                score += 0.5

            r.score = min(score, 1.25)
            r.metadata = m
            results.append(r)

        return results
