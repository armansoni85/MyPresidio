from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class ItalyVATRecognizer(PatternRecognizer):
    logger.info("Initializing Italy VAT Recognizer...")

    # Define patterns for Italy VAT numbers
    PATTERNS = [
        Pattern(
            "Italy VAT - Base Confidence",
            r"(?i)\b(?:IT[ -]?)?\d{11}\b",  # Matches "IT" with optional delimiter followed by 11 digits or just 11 digits
            0.5  # Initial base confidence score for the pattern match
        )
    ]

    # Keywords for Italy VAT numbers (e.g., Microsoft keyword list)
    CONTEXT = [
        "partita iva", "vat number", "italy vat", "italian vat", "vat identification number", 
        "vat code", "codice iva", "vat id"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "it",  # Supports Italian and English
        supported_entity: str = "ITALY_VAT_NUMBER",
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
        # Call the parent's analyze to use the regex pattern matching
        results = super().analyze(text, entities, nlp_artifacts)

        # Debugging log to show matched results
        logger.info(f"Found matches: {[text[result.start:result.end] for result in results]}")

        for result in results:
            # Extract the matched VAT number for checksum validation
            matched_text = text[result.start:result.end]
            vat_number = self._extract_digits(matched_text)

            logger.info(f"Extracted VAT number: {vat_number}")

            # Check if the VAT number is valid using the Luhn algorithm
            if self._validate_checksum(vat_number):
                logger.info(f"Valid checksum for VAT number: {vat_number}")
                # Set a base confidence score for a valid VAT number
                result.score = 0.5  
                
                # Increase score if keywords are found
                if any(keyword.lower() in text.lower() for keyword in self.CONTEXT):
                    logger.info(f"Keyword found in text: {text}")
                    result.score = 1.0  # High confidence for valid VAT number with keywords
                else:
                    result.score = 0.7  # Medium confidence if no keywords found
            else:
                logger.info(f"Invalid checksum for VAT number: {vat_number}")
                result.score = 0.0  # Invalid VAT number

            logger.info(f"Final score for VAT number {vat_number}: {result.score}")
        return results

    def _extract_digits(self, text: str) -> str:
        """
        Extracts digits from the given text, ignoring non-numeric characters.
        """
        return re.sub(r'[^0-9]', '', text)

    def _validate_checksum(self, vat_number: str) -> bool:
        """
        Validates the checksum of an Italian VAT number using the specific VAT checksum rules.
        """
        if len(vat_number) != 11:
            logger.info(f"VAT number length is incorrect: {vat_number}")
            return False

        total_sum = 0

        # Loop through the first 10 digits
        for i in range(10):
            digit = int(vat_number[i])
            
            # Italian VAT: Odd positions (1-based) are added directly, even positions are doubled
            if (i + 1) % 2 == 0:  # Even positions (2nd, 4th, 6th, etc.)
                digit *= 2
                if digit > 9:
                    digit -= 9
            
            total_sum += digit

        # Calculate the check digit
        check_digit = (10 - (total_sum % 10)) % 10
        logger.info(f"check_digit {check_digit} {int(vat_number[-1])}")
        # Compare calculated check digit with the 11th digit in the VAT number
        is_valid = check_digit == int(vat_number[-1])
        logger.info(f"Luhn validation result for {vat_number}: {is_valid}")
        return is_valid

    

    

