import re
from typing import List, Optional, Dict
from presidio_analyzer import PatternRecognizer, RecognizerResult, Pattern


class AllCreditCardNumberRecognizer(PatternRecognizer):

    CONTEXT_WINDOW = 80

    CC_CONTEXT = [
        # generic
        "credit", "debit", "card",
        "credit card", "debit card",
        "card number", "card no",
        "payment", "pay", "billing", "charge", "charged",
        "transaction", "txn", "purchase", "checkout",
        "subscription", "invoice", "account", "acct",
        "payment card", "bank card",

        # identifiers
        "ccn", "cc num", "cc#", "card #",

        # security
        "cvv", "cvc", "expiry", "expiration", "exp date",

        # issuer words (context only, NOT validation)
        "visa", "mastercard", "master card",
        "american express", "amex",
        "discover", "diners", "diners club",
        "jcb", "unionpay", "union pay", "cup"
    ]

    TEST_CARD_NUMBERS = {
        "4111111111111111",
        "5555555555554444",
        "4012888888881881",
        "4222222222222",
        "378282246310005",
        "6011111111111117",
    }

    DIGIT_BLOCK_REGEX = re.compile(
        r"""
        (?<!\d)
        (
                62\d{17}                     # UnionPay 19-digit
            | \d{13,16}                   # Other unformatted cards
            | \d{4}(?:[ -]\d{4}){3}       # Standard formatted cards
            | \d{4}[ -]\d{6}[ -]\d{5}     # Amex-style
        )
        (?!\d)
        """,
        re.VERBOSE
    )


    DUMMY_PATTERN = Pattern("DUMMY", r"$^", 0.0)

    def __init__(self, supported_language: Optional[str] = None):
        super().__init__(
            supported_entity="CREDIT_CARD",
            patterns=[self.DUMMY_PATTERN],
            context=[],
            supported_language=supported_language
        )

    # -------------------------------------------------
    # Analyze
    # -------------------------------------------------

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None):

        lowered = text.lower()
        hits = []

        for m in self.DIGIT_BLOCK_REGEX.finditer(text):
            start, end = m.span()
            raw = m.group()
            digits = re.sub(r"\D", "", raw)

            # ---- hard filters ----
            if digits in self.TEST_CARD_NUMBERS:
                continue
            if len(set(digits)) == 1:
                continue
            if not self._luhn_10_check(digits):
                continue   # ✅ LUHN-10 is the ONLY validity gate

            formatted = bool(re.search(r"[ -]", raw))
            context = self._has_context(lowered, start, end)

            hits.append({
                "start": start,
                "end": end,
                "formatted": formatted,
                "context": context
            })

        if not hits:
            return []

        multiple = len(hits) > 1
        results = []

        for h in hits:
            if multiple:
                score = 0.75
            else:
                score = 0.6 if h["formatted"] else 0.4

            if h["context"]:
                score += 0.5

            results.append(
                RecognizerResult(
                    entity_type="CREDIT_CARD",
                    start=h["start"],
                    end=h["end"],
                    score=min(score, 1.25)
                )
            )

        return results

    # -------------------------------------------------
    # Helpers
    # -------------------------------------------------

    def _has_context(self, text: str, start: int, end: int) -> bool:
        window = (
            text[max(0, start - self.CONTEXT_WINDOW):start]
            + " "
            + text[end:end + self.CONTEXT_WINDOW]
        )

        for term in self.CC_CONTEXT:
            if re.search(rf"\b{re.escape(term)}\b", window):
                return True

        return False

    def _luhn_10_check(self, number: str) -> bool:
        total = 0
        for i, d in enumerate(number[::-1]):
            n = int(d)
            if i % 2 == 1:
                n *= 2
                if n > 9:
                    n -= 9
            total += n
        return total % 10 == 0
