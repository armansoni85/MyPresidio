import re
import logging
from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer

# Enable detailed debug logs
logger = logging.getLogger("presidio-analyzer")
logger.setLevel(logging.DEBUG)


class CanadaDriversLicenceRecognizer(PatternRecognizer):
    """
    Recognizer for Canadian Driver's Licence numbers across all provinces and territories.
    Strictly validates patterns and rejects invalid data like Ontario 'A1234-2663210301-23'.
    """

    # ---------------------------
    # Regex Patterns (by Province)
    # ---------------------------
    PATTERNS = [
        # Alberta: 6 digits + hyphen + 3 digits OR 9 digits, cannot start with 0
        # Alberta: 6 digits + hyphen + 3 digits OR 5–9 digits (digits only), must be standalone
        Pattern("Alberta DL", r"(?<![A-Za-z]-)(?:\d{6}-\d{3}|\d{5,9})(?![A-Za-z0-9])", 0.85),
        # British Columbia: exactly 7 digits
        Pattern("British Columbia DL", r"\b\d{7}\b$", 0.75),

        # Manitoba: complex alphanumeric pattern (AA-AA-AA-A###AA)
        Pattern("Manitoba DL", r"(?<![A-Z0-9])[A-Z]{2}-?[A-Z]{2}-?[A-Z]{2}-?[A-Z]\d{3}[A-Z]{2}(?![A-Z0-9])", 0.85),

        # New Brunswick: 5–7 digits
        Pattern("New Brunswick DL", r"\b\d{5,7}\b$", 0.75),

        # Newfoundland and Labrador: 1 letter + 9 digits
        Pattern("Newfoundland and Labrador DL", r"(?<![A-Z0-9])[A-Z]\d{9}(?![A-Z0-9])", 0.9),

        # Nova Scotia: 5 letters + optional hyphen + 9 digits with internal constraints
        # 5 letters -? (0|1|2|3) digit, then digit, then (0|1), then 6 more digits
        Pattern("Nova Scotia DL", r"\b[A-Z]{5}-?[0-3]\d[01]\d{6}\b$", 0.9),

        # Ontario: 1 letter + 4 digits + optional hyphen + 5 digits + rules for trailing digits
        # Anchored to prevent false positives like "-23"
        Pattern("Ontario DL", r"(?<![A-Z0-9])[A-Z]\d{4}-?\d{5}\d[0156]\d[0123](?![A-Z0-9])", 0.9),

        # Prince Edward Island: 5–6 digits
        Pattern("Prince Edward Island DL", r"\b\d{5,6}\b$", 0.7),

        # Quebec: 1 letter + 12 digits
        Pattern("Quebec DL", r"\b[A-Z]\d{12}\b$", 0.9),

        # Saskatchewan: 8 digits
        Pattern("Saskatchewan DL", r"\b\d{8}\b$", 0.8),

        # Territories (Northwest Territories, Yukon, Nunavut): 6–9 alphanumeric, at least one digit
        Pattern("Territories DL", r"\b(?=.*\d)[A-Z0-9]{6,9}\b$", 0.7),
    ]

    CONTEXT = [
        "driver licence", "drivers license", "driving licence",
        "dl number", "canada licence", "provincial licence",
        "issued by", "transport", "vehicle licence", "dl no"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_entity: str = "CANADA_DRIVERS_LICENCE",
        supported_language: str = "en",
    ):
        patterns = patterns or self.PATTERNS
        context = context or self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    # ------------------------------------------
    # Validation and Scoring Logic per Match
    # ------------------------------------------
    def validate_result(self, pattern_text: str) -> bool:
        """
        Strict validation logic for each matched DL number.
        Returns True only if it fully matches a valid provincial format.
        """
        clean_text = re.sub(r"[^A-Za-z0-9]", "", pattern_text)
        logger.debug(f"[VALIDATE] Raw: '{pattern_text}' | Cleaned: '{clean_text}'")

        if len(clean_text) < 5 or len(clean_text) > 15:
            logger.debug(f"[INVALID] Length out of range ({len(clean_text)})")
            return False

        # Province-specific strict checks

        # Alberta strict: must be standalone & digits only (5–9) or 6-3 format
        if re.fullmatch(r"^(?:\d{6}-\d{3}|\d{5,9})$", clean_text):
            logger.debug("[VALID] Alberts format")
            return True

        # Manitoba strict
        if re.fullmatch(r"[A-Z]{2}[A-Z]{2}[A-Z]{2}[A-Z]\d{3}[A-Z]{2}", clean_text):
            logger.debug("[VALID] Manitoba format")
            return True


        if re.fullmatch(r"[A-Z]\d{9}", clean_text):
            logger.debug("[VALID] Newfoundland format")
            return True

        if re.fullmatch(r"[A-Z]\d{12}", clean_text):
            logger.debug("[VALID] Quebec format")
            return True

        if re.fullmatch(r"\d{7}", clean_text):
            logger.debug("[VALID] British Columbia or NB format")
            return True

        if re.fullmatch(r"\d{8}", clean_text):
            logger.debug("[VALID] Saskatchewan format")
            return True

        # Ontario strict
        if re.fullmatch(r"[A-Z]\d{4}-?\d{5}\d[0156]\d[0123]", clean_text):
            logging.debug("[VALID] Ontario format")
            return True

        # Nova Scotia strict
        if re.fullmatch(r"[A-Z]{5}-?[0-3]\d[01]\d{6}", pattern_text):
            logger.debug("[VALID] Nova Scotia format")
            return True

        logger.debug("[INVALID] Does not match any province rule")
        return False

    def _prefixed_or_attached_to_letters(self, text: str, start: int, end: int) -> bool:
        """
        Return True if the match is attached to letters (before or after),
        or preceded by letters + hyphen, e.g.:

          ABC-123456789   → True
          ABC123456789    → True
          123456789XYZ    → True
          A-12345         → True
          123456-789      → False
          511665231       → False
        """
        # Check character before start
        if start > 0:
            prev_char = text[start - 1]
            if prev_char.isalpha():
                return True  # directly attached letters
            if prev_char == "-" and start > 1 and text[start - 2].isalpha():
                return True  # letter + hyphen before match

        # Check character after end
        if end < len(text):
            next_char = text[end:end + 1]
            if next_char.isalpha():
                return True  # letters after match

        return False


    def analyze(self, text, entities, nlp_artifacts=None):
        results = super().analyze(text, entities, nlp_artifacts)
        for result in results:
            start, end = result.start, result.end
            matched_text = text[start:end]

            # Alberta check: reject if attached to letters or prefixed by letter-hyphen
            if re.fullmatch(r"\d{5,9}|\d{6}-\d{3}", matched_text):
                if self._prefixed_or_attached_to_letters(text, start, end):
                    logger.debug(f"[INVALID] Alberta – attached to letters: '{matched_text}'")
                    is_valid = False
                else:
                    is_valid = self.validate_result(matched_text)
            else:
                is_valid = self.validate_result(matched_text)

            old_score = result.score
            result.score = max(0.7, old_score) if is_valid else min(0.69, old_score - 0.1)
            logger.debug(
                f"[RESULT] '{matched_text}' | Valid={is_valid} | OldScore={old_score:.2f} → NewScore={result.score:.2f}"
            )

        return results

