import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional, Dict

class CreditCardIssuerRecognizer(PatternRecognizer):
    """
    Recognizes credit card numbers from all major issuers with proper validation.
    Supported issuers: Visa, MasterCard, American Express, Diners Club, Discover, JCB, China UnionPay.
    """

    # Credit card issuer rules: (name, validation function)
    CREDIT_CARD_ISSUER_RULES = [
        ("Visa", lambda x: x.startswith('4')),
        ("MasterCard", lambda x: 
            (len(x) >= 2 and x[0] == '5' and x[1] in '12345') or  # 51-55
            (len(x) >= 4 and 2221 <= int(x[:4]) <= 2720)          # 2221-2720 (new range)
        ),
        ("American Express", lambda x: len(x) >= 2 and x[:2] in ['34','37']),
        ("Diners Club", lambda x: 
            x.startswith(('300','301','302','303','304','305','309','36','38','39'))
        ),
        ("Discover", lambda x: 
            (x.startswith('6011')) or
            (len(x) >= 6 and 622126 <= int(x[:6]) <= 622925) or  # China UnionPay co-branded
            (len(x) >= 3 and x.startswith('64') and x[2] in '456789') or  # 644-649
            (len(x) >= 2 and x.startswith('65'))),
        ("JCB", lambda x: 
            (x.startswith('2131')) or
            (x.startswith('1800')) or
            (len(x) >= 4 and x.startswith('35') and 3528 <= int(x[:4]) <= 3589)),
        ("China UnionPay", lambda x: 
            x.startswith('62') or  # Standard UnionPay
            (len(x) >= 6 and 810000 <= int(x[:6]) <= 817199)  # Co-branded cards
        )
    ]

    # Valid lengths per issuer
    CREDIT_CARD_LENGTHS: Dict[str, List[int]] = {
        "Visa": [13, 16, 19],
        "MasterCard": [16],
        "American Express": [15],
        "Diners Club": [14, 15, 16, 17, 18, 19],
        "Discover": [16, 17, 18, 19],
        "JCB": [15, 16, 17, 18, 19],
        "China UnionPay": [16, 17, 18, 19]
    }

    CONTEXT_TERMS: List[str] = [
        "credit", "card", "cc", "debit", "visa", "mastercard", "amex",
        "discover", "jcb", "unionpay", "cup", "card number", "card no",
        "cc#", "cardholder", "expiration", "expiry", "cvv", "cvc"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        # Match numbers with spaces/hyphens between digits
        patterns = [Pattern("Credit Card", r'\b(?:\d[ -]*?){13,19}\b', 0.85)]
        super().__init__(
            supported_entity="CREDIT_CARD_ISSUER",
            patterns=patterns,
            supported_language=supported_language,
            context=self.CONTEXT_TERMS
        )

    def luhn_checksum(self, card_number: str) -> bool:
        """Validate credit card number using Luhn algorithm."""
        def digits_of(n):
            return [int(d) for d in str(n)]
        
        digits = digits_of(card_number)
        odd_digits = digits[-1::-2]
        even_digits = digits[-2::-2]
        checksum = sum(odd_digits)
        for d in even_digits:
            checksum += sum(digits_of(d * 2))
        return checksum % 10 == 0

    def determine_issuer(self, card_number: str) -> Optional[str]:
        """Determine issuer based on card number prefix."""
        for issuer, rule in self.CREDIT_CARD_ISSUER_RULES:
            try:
                if rule(card_number):
                    return issuer
            except (ValueError, IndexError):
                continue
        return None

    def analyze(self, text, entities, nlp_artifacts=None):
        results = super().analyze(text, entities, nlp_artifacts)
        valid_results = []

        for result in results:
            # Extract matched text and remove non-digit characters
            matched_text = text[result.start:result.end]
            card_number = re.sub(r"\D", "", matched_text)
            length = len(card_number)

            # Skip if not within valid length range
            if not (13 <= length <= 19):
                continue

            # Determine issuer and validate length
            issuer = self.determine_issuer(card_number)
            if issuer is None:
                continue
            if length not in self.CREDIT_CARD_LENGTHS.get(issuer, []):
                continue

            # Validate with Luhn algorithm (except China UnionPay)
            if issuer != "China UnionPay" and not self.luhn_checksum(card_number):
                continue

            # Add issuer to metadata and keep result
            result.metadata = {"issuer": issuer}
            valid_results.append(result)

        return valid_results
