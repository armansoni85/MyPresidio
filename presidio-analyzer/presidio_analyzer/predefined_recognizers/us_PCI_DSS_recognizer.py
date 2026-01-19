import re
from presidio_analyzer import PatternRecognizer, RecognizerResult, Pattern
from typing import List, Optional, Tuple

class PCI_DSS_CreditCardAndTrackDataRecognizer(PatternRecognizer):
    """
    Recognizer for detecting Credit Card Numbers (CCN) and Credit Card Track Data.
    Supports major issuers: Amex, Diner's Club, Discover, JCB, MasterCard, Visa, UnionPay.
    A hit in either CCN or track data will trigger the policy.
    """

    # Enhanced pattern for all major card issuers with flexible delimiters
    CREDIT_CARD_PATTERN = r"\b(?:\d[ -]*?){12,18}\d\b"

    # Pattern to capture card numbers in track data (13-19 digits)
    TRACK_DATA_PATTERN = r'\"Credit_Card_Number\"\s*s\s*\d+\s*\"(\d{13,19})\"'

    # Card issuer definitions: (name, IIN ranges, valid lengths)
    CARD_ISSUERS = [
        # American Express
        ("American Express", [("34",), ("37",)], [15]),
        # Diner's Club International (includes some MasterCard)
        ("Diner's Club", [("300-305",), ("36",), ("38",), ("39",)], [14, 16]),
        # Discover Card
        ("Discover", [
            ("6011",), 
            ("622126-622925",), 
            ("644-649",), 
            ("65",),
            ("622926-622999",)  # Extended Discover range
        ], [16, 19]),
        # JCB
        ("JCB", [("3528-3589",)], [16]),
        # MasterCard
        ("MasterCard", [("51-55",), ("2221-2720",)], [16]),
        # Visa
        ("Visa", [("4",)], [13, 16, 19]),
        # China UnionPay
        ("UnionPay", [("62",), ("81",)], [16, 17, 18, 19])
    ]

    def __init__(self, supported_language: Optional[str] = None):
        patterns = [
            Pattern("Credit Card Number", self.CREDIT_CARD_PATTERN, 0.85)
        ]
        super().__init__(
            supported_entity="PCI_DSS_CREDIT_CARD_OR_TRACK_DATA",
            patterns=patterns,
            supported_language=supported_language
        )

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        """
        Analyze method to detect both credit card numbers and track data in the text.
        """
        results = super().analyze(text, entities, nlp_artifacts)
        validated_results = []

        # Validate base credit card matches
        for result in results:
            matched_text = text[result.start:result.end]
            cleaned_ccn = re.sub(r"\D", "", matched_text)
            if self.is_valid_credit_card(cleaned_ccn):
                validated_results.append(result)

        # Detect and validate track data
        track_data_results = self.detect_track_data(text)
        if track_data_results:
            validated_results.extend(track_data_results)

        # Return only the first relevant result
        return [validated_results[0]] if validated_results else []

    def detect_track_data(self, text: str) -> List[RecognizerResult]:
        """
        Detect credit card numbers embedded in structured track data.
        """
        track_data_results = []
        matches = re.finditer(self.TRACK_DATA_PATTERN, text)

        for match in matches:
            credit_card_number = match.group(1)
            start = match.start(1)
            end = match.end(1)

            if self.is_valid_credit_card(credit_card_number):
                track_data_results.append(
                    RecognizerResult(
                        entity_type=self.supported_entity,
                        start=start,
                        end=end,
                        score=0.9
                    )
                )

        return track_data_results

    def is_valid_credit_card(self, ccn: str) -> bool:
        """
        Validate credit card number using Luhn algorithm and issuer checks.
        """
        # Basic length check
        if not (13 <= len(ccn) <= 19):
            return False
        
        # Validate card issuer
        if not self.is_valid_issuer(ccn):
            return False
        
        # Validate with Luhn algorithm
        return self.luhn_checksum(ccn)

    def is_valid_issuer(self, ccn: str) -> bool:
        """
        Check if card matches any known issuer's IIN ranges and length requirements.
        """
        for issuer, iin_ranges, lengths in self.CARD_ISSUERS:
            # Check length first
            if len(ccn) not in lengths:
                continue
                
            # Check each IIN range
            for range_spec in iin_ranges:
                if self._check_iin_range(ccn, range_spec):
                    return True
        return False

    def _check_iin_range(self, ccn: str, range_spec: Tuple[str]) -> bool:
        """
        Check if card matches a specific IIN range specification.
        """
        # Get required prefix length
        prefix = ccn[:6]  # Check up to 6 digits for maximum IIN length
        
        for spec in range_spec:
            # Handle range specifications (e.g., "300-305")
            if '-' in spec:
                start, end = spec.split('-')
                # Handle variable length ranges
                test_len = max(len(start), len(end))
                test_prefix = prefix[:test_len]
                
                # Convert to integers if same length, otherwise string compare
                if len(start) == len(end):
                    start_int = int(start)
                    end_int = int(end)
                    test_int = int(test_prefix)
                    if start_int <= test_int <= end_int:
                        return True
                else:
                    # Handle variable-length prefixes (e.g., "644-649")
                    if start <= test_prefix <= end:
                        return True
            # Handle single value specifications
            else:
                # Check if the card starts with this prefix
                if ccn.startswith(spec):
                    return True
        return False

    def luhn_checksum(self, card_number: str) -> bool:
        """
        Validate credit card number using the Luhn algorithm.
        """
        digits = [int(d) for d in card_number]
        # Reverse the digits for easier processing
        reversed_digits = digits[::-1]
        total = 0
        for i, digit in enumerate(reversed_digits):
            if i % 2 == 1:  # Even positions in reversed (i.e., odd positions in original)
                # Double the digit and subtract 9 if >9
                doubled = digit * 2
                total += doubled if doubled < 10 else doubled - 9
            else:
                total += digit
        return total % 10 == 0
