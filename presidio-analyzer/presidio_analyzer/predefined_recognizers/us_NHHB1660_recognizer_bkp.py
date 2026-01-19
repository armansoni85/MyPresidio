import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Optional

class NewHampshirePolicyRecognizer(PatternRecognizer):
    """
    Recognizer for detecting sensitive personal data from New Hampshire including:
    - Credit Card Numbers (CCN)
    - CCN Track Data
    - Social Security Numbers (SSN)
    - Driver's License Numbers
    """

    # Define patterns for New Hampshire specific data
    CREDIT_CARD_PATTERN = r"^\d{4}(?: \d{4}){3}$|^\d{16}$"
    CCN_TRACK_DATA_PATTERN = r"Name\"\s*\w*\s*\"(?P<name>[\w\s\-]+)\".*?Credit_Card_Number\"\s*\d+\s*\"(?P<ccn>\d+)\".*?Expiry[_\s]Date\"\s*\d+\s*\"(?P<expiry>\d{2}\/\d{2})\""
    SSN_PATTERN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"
    DRIVER_LICENSE_PATTERN = r"\b[A-Z0-9]{9}\b"  # Simplified driver’s license number pattern

    # Context keywords for New Hampshire
    CONTEXT_TERMS: List[str] = [
        "New Hampshire", "NH", "credit card", "social security", "SSN", "driver's license", "DL"
    ]

    def __init__(self, supported_language: Optional[str] = None):
        # Create patterns for each sensitive entity
        patterns = [
            Pattern("New Hampshire Credit Card Number", self.CREDIT_CARD_PATTERN, 0.85),
            Pattern("New Hampshire CCN Track Data", self.CCN_TRACK_DATA_PATTERN, 0.9),
            Pattern("New Hampshire SSN", self.SSN_PATTERN, 0.9),
            Pattern("New Hampshire Driver's License", self.DRIVER_LICENSE_PATTERN, 0.8)
        ]
        super().__init__(
            supported_entity="US_NHHB1660",
            patterns=patterns,
            context=self.CONTEXT_TERMS,
            supported_language=supported_language
        )

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None):
        """
        Override analyze method to add strict filtering for special characters.
        """
        # Use the parent class to get initial results
        results = super().analyze(text, entities, nlp_artifacts)

        # Filter results: Allow only matches with digits and spaces
        filtered_results = []
        for result in results:
            # Extract the matched text
            matched_text = text[result.start:result.end]
            print(f"DEBUG: Matched Text - '{matched_text}'")  # Debugging log

            # Ensure the match conforms to the exact pattern
            if re.fullmatch(self.CREDIT_CARD_PATTERN, matched_text):
                filtered_results.append(result)
            else:
                print(f"DEBUG: Rejected Text - '{matched_text}'")  # Debugging log

        return filtered_results

    def validate_ccn(self, ccn: str) -> bool:
        """
        Validate credit card number using the Luhn algorithm.
        """
        def luhn_checksum(card_number: str) -> bool:
            def digits_of(n):
                return [int(d) for d in str(n)]
            digits = digits_of(card_number)
            odd_digits = digits[-1::-2]
            even_digits = digits[-2::-2]
            checksum = sum(odd_digits)
            for d in even_digits:
                checksum += sum(digits_of(d * 2))
            return checksum % 10 == 0

        cleaned_ccn = re.sub(r"\D", "", ccn)  # Remove non-digit characters
        return luhn_checksum(cleaned_ccn)


# Sample input text for testing
# text = """
# New Hampshire residents, Bob and Jane Smith, reported the following: 
# Their credit card number is 4316-0200-0063-0490. 
# Bob's New Hampshire driver's license number is 056698494.
# They also have SSNs: 260-53-2093, 260-56-4928.
# Thank you.
# """
