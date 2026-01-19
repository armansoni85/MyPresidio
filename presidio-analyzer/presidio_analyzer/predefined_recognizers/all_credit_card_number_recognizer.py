import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional, Dict, Any

class AllCreditCardNumberRecognizer(PatternRecognizer):
    """
    Recognizer to detect various credit card numbers with context-based scoring.
    Only validates with Luhn algorithm for card types specified in LUHN_CARD_TYPES.
    """

    # Card types requiring Luhn validation (from Trellix docs)
    LUHN_CARD_TYPES = {
        'American Express', 'Diner\'s Club', 'Discover',
        'JCB', 'Mastercard', 'VISA'
    }

    # Expanded context terms
    CC_NAME_TERMS = [
        'credit card', 'card number', 'CCN', 'ccnum', 'cc num',
        'card identification number', 'creditcard', 'cc#', 'cc #',
        'card no', 'card #', 'credit card no', 'card details',
        'payment card', 'bank card', 'card information',
        'cc details', 'credit card information'
    ]

    VERIFICATION_TERMS = [
        'card verification', 'cvn', 'cid', 'csc',
        'cvc2', 'cvv2', 'pin block', 'security code',
        'security number', 'verification code', 'cvv',
        'cvc', 'cv2', 'cid number', 'card code', 'ccv'
    ]

    # Comprehensive card brand terms
    CARD_BRAND_TERMS = [
        'visa', 'mastercard', 'amex', 'american express',
        'discover', 'diners club', 'jcb', 'china unionpay',
        'unionpay', "kohl's", 'kohls', 'mc', 'master card',
        'maestro', 'cirrus', 'paypal', 'discover card',
        'diners', 'jcb card', 'union pay'
    ]

    # Regex for date patterns (MM/YY, MM/YYYY, etc.)
    DATE_REGEX = re.compile(r'\b(0[1-9]|1[0-2])[-/](\d{2}|\d{4})\b')

    # Updated patterns with space support
    PATTERNS: Dict[str, List[Pattern]] = {
        'American Express': [
            Pattern("Amex", r'\b3[47]\d{2}(?:[ -]?)\d{6}(?:[ -]?)\d{5}\b', 0.0),
            Pattern("Amex", r'\b3[47]\d{13}\b', 0.0)
        ],
        'China UnionPay': [
            Pattern("China UnionPay", r'\b62\d{14,17}\b', 0.0),  # Extended length
            Pattern("China UnionPay", r'\b(?:62[2-8]|603601|603265|621977|603708|602969|601428|603367|603694)\d{10,13}\b', 0.0),
            Pattern("China UnionPay", r'\b62\d{2}(?:[ -]?)\d{4}(?:[ -]?)\d{4}(?:[ -]?)\d{4,7}\b', 0.0)
        ],
        'Diner\'s Club': [
            Pattern("Diner's", r'\b3(?:0[0-5]|[689]\d)\d{11}\b', 0.0),
            Pattern("Diner's", r'\b3[689]\d{2}(?:[ -]?)\d{6}(?:[ -]?)\d{4}\b', 0.0)
        ],
        'Discover': [
            Pattern("Discover", r'\b6(?:011|4[4-9]\d|5\d{2})(?:[ -]?)\d{4}(?:[ -]?)\d{4}(?:[ -]?)\d{4}\b', 0.0),
            Pattern("Discover", r'\b6(?:011|4[4-9]\d|5\d{2})\d{12}\b', 0.0)
        ],
        'JCB': [
            Pattern("JCB", r'\b35[2-8]\d{2}(?:[ -]?)\d{4}(?:[ -]?)\d{4}(?:[ -]?)\d{4}\b', 0.0),
            Pattern("JCB", r'\b35[2-8]\d{12}\b', 0.0)
        ],
        'Kohl\'s': [
            Pattern("Kohl's", r'\b439[1-9]\d{2}(?:[ -]?)\d{4}(?:[ -]?)\d{4}(?:[ -]?)\d{4}\b', 0.0),
            Pattern("Kohl's", r'\b439[1-9]\d{12}\b', 0.0)
        ],
        'Mastercard': [
            Pattern("Mastercard", r'\b5[1-5]\d{2}(?:[ -]?)\d{4}(?:[ -]?)\d{4}(?:[ -]?)\d{4}\b', 0.0),
            Pattern("Mastercard", r'\b2[2-7]\d{2}(?:[ -]?)\d{4}(?:[ -]?)\d{4}(?:[ -]?)\d{4}\b', 0.0),
            Pattern("Mastercard", r'\b(?:5[1-5]|2[2-7])\d{14}\b', 0.0)
        ],
        'VISA': [
            Pattern("VISA", r'\b4\d{3}(?:[ -]?)\d{4}(?:[ -]?)\d{4}(?:[ -]?)\d{4}\b', 0.0),
            Pattern("VISA", r'\b4\d{12}(?:\d{3})?\b', 0.0)  # Supports 13 or 16 digits
        ],
        'Generic Credit Card': [
            Pattern("Generic", r'\b(?:\d[ -]*?){13,19}\b', 0.0)  # Generic pattern with separators
        ]
    }

    IGNORED_PATTERNS: Dict[str, List[str]] = {
        # ... (keep your existing ignored patterns here)
    }

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [p for plist in self.PATTERNS.values() for p in plist]
        super().__init__(
            supported_entity="CREDIT_CARD",
            patterns=patterns,
            context=[],  # Not using parent class context handling
            supported_language=supported_language
        )
        self.ignored_patterns = {
            card_type: [re.compile(p) for p in patterns]
            for card_type, patterns in self.IGNORED_PATTERNS.items()
        }

    def analyze(self, text: str, entities: List[str], nlp_artifacts: Any = None) -> List[RecognizerResult]:
        """Analyze text with enhanced pattern matching and adjust scores for multiple cards"""
        # First pass: collect all valid matches
        matches = []
        for card_type, pattern_list in self.PATTERNS.items():
            for pattern in pattern_list:
                for match in re.finditer(pattern.regex, text):
                    start, end = match.span()
                    text_fragment = text[start:end]
                    clean_number = re.sub(r"[^\d]", "", text_fragment)

                    if self._is_ignored(card_type, clean_number):
                        continue
                    if not self._valid_card(card_type, clean_number):
                        continue

                    matches.append((start, end, card_type, clean_number))
        
        # Calculate base context score once (uses entire text)
        base_score = self._calculate_score(text) if matches else 0.0
        n_matches = len(matches)
        
        # Create results with score adjustment for multiple cards
        results = []
        for (start, end, card_type, clean_number) in matches:
            # Boost score if multiple valid cards found
            if n_matches >= 2:
                score = min(1.0, base_score + 0.2)
            else:
                score = base_score
                
            results.append(
                RecognizerResult(
                    entity_type="CREDIT_CARD",
                    start=start,
                    end=end,
                    score=score
                )
            )
        return results

    def _is_ignored(self, card_type: str, text: str) -> bool:
        """Check against ignored patterns for specific card type"""
        if card_type in self.ignored_patterns:
            for pattern in self.ignored_patterns[card_type]:
                if pattern.fullmatch(text):
                    return True
        return False

    def _valid_card(self, card_type: str, card_number: str) -> bool:
        """Conditional Luhn validation based on card type"""
        # Only validate Luhn for specified card types
        if card_type not in self.LUHN_CARD_TYPES:
            return True

        # Clean and validate number
        clean_number = re.sub(r"[\s-]", "", card_number)
        return clean_number.isdigit() and self._luhn_check(clean_number)

    def _luhn_check(self, card_number: str) -> bool:
        """Validate number using Luhn algorithm"""
        total = 0
        reverse_digits = card_number[::-1]
        for i, digit in enumerate(reverse_digits):
            num = int(digit)
            if i % 2 == 1:
                num *= 2
                if num > 9:
                    num -= 9
            total += num
        return total % 10 == 0

    def _calculate_score(self, text: str) -> float:
        """Calculate context score using entire text"""
        context_text = text.lower()
        
        has_name_term = any(term in context_text for term in self.CC_NAME_TERMS)
        has_verif_term = any(term in context_text for term in self.VERIFICATION_TERMS)
        has_brand_term = any(brand in context_text for brand in self.CARD_BRAND_TERMS)
        has_date = self.DATE_REGEX.search(context_text) is not None
        has_card_keyword = any(word in context_text for word in ['card', 'cc', 'credit', 'payment'])
        has_number_keyword = any(word in context_text for word in ['number', 'num', 'no', '#', 'digits'])

        if has_name_term and has_verif_term and has_date:
            return 1.0
        elif has_name_term or has_brand_term or has_verif_term:
            return 0.8
        elif has_card_keyword and has_number_keyword:
            return 0.8
        elif has_card_keyword or has_number_keyword:
            return 0.75
        else:
            return 0.7
