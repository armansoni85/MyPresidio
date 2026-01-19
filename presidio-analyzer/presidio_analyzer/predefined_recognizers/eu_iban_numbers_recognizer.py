from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class EU_IBANRecognizer(PatternRecognizer):
    logger.info("Initializing EU IBAN Recognizer...")

    IBAN_LENGTHS = {
        "AT": 20, "BE": 16, "BG": 22, "HR": 21, "CY": 28, "CZ": 24,
        "DK": 18, "EE": 20, "FI": 18, "FR": 27, "DE": 22, "GR": 27,
        "HU": 28, "IE": 22, "IT": 27, "LV": 21, "LT": 20, "LU": 20,
        "MT": 31, "NL": 18, "PL": 28, "PT": 25, "RO": 24, "SK": 24,
        "SI": 19, "ES": 24, "SE": 24, "GB": 22  # Added GB
    }

    VALID_BANK_CODES = {
        "AT": ["BAWA", "OBKL", "RZBA"], "BE": ["BBRU", "KBCB"], "BG": ["BUIN", "UBBS"],
        "HR": ["PBZG", "ZABA"], "CY": ["BOCY", "USBK"], "CZ": ["CEKO", "KOMB", "FIOB"],
        "DK": ["DABA"], "EE": ["LHVB", "HABA"], "FI": ["OKOY", "HELS"],
        "FR": ["BNPP", "CCBP", "CEPA", "CMCI"], "DE": ["DEUT", "COBA", "DRES", "HYVE"],
        "GR": ["ETHN", "ALPH", "EUROB", "PIRE"], "HU": ["OTPV"], "IE": ["AIBK", "BOFI", "ULSB"],
        "IT": ["BCIT", "BROM", "BPPI"], "LV": ["HABA", "PARX"], "LT": ["SECB", "KUBL"],
        "LU": ["BCEE", "BGLL"], "MT": ["VALL", "APS"], "NL": ["ABNA", "INGB", "RABO", "SNSB"],
        "PL": ["BPKO", "BREX", "BPHK"], "PT": ["CGDP", "BCOM", "BBPI"], "RO": ["RZBR", "BRDE"],
        "SK": ["SLSP", "TATR"], "SI": ["BASI", "LJBASI"], "ES": ["BBVA", "BSCH", "CAIX", "SABM"],
        "SE": ["HAND", "NDEA", "SWED"], "GB": ["BUKB", "NWBK", "BARC", "LOYD"]
    }

    PATTERNS = [
        Pattern(
            "EU IBAN - General Pattern",
            r"\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b",
            0.5
        )
    ]

    CONTEXT = [
        "IBAN", "international bank account number", "numéro de compte",
        "nummer IBAN", "code IBAN", "identifiant bancaire"
    ]

    def __init__(self, patterns=None, context=None, supported_language="en", supported_entity="EU_IBAN"):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        logger.info(f"Analyzing text for EU IBAN: {text}")
        results = super().analyze(text, entities, nlp_artifacts)

        for result in results:
            iban_number = text[result.start:result.end]
            cleaned_iban = re.sub(r"\s+", "", iban_number)

            if self._is_valid_checksum(cleaned_iban):
                logger.info(f"Valid IBAN: {iban_number}")
                result.score = 1.0
            else:
                logger.warning(f"Invalid IBAN: {iban_number}")
                result.score = 0.0
        return results

    def _has_valid_length(self, iban: str) -> bool:
        country_code = iban[:2]
        expected_length = self.IBAN_LENGTHS.get(country_code)
        if expected_length is None:
            logger.warning(f"No length defined for country: {country_code}")
            return False
        return len(iban) == expected_length

    def _has_valid_bank_code(self, iban: str) -> bool:
        country_code = iban[:2]
        bank_code = iban[4:8]
        valid_bank_codes = self.VALID_BANK_CODES.get(country_code)
        if not valid_bank_codes:
            return True  # Skip if no bank list for country
        return bank_code in valid_bank_codes

    def _is_valid_checksum(self, iban: str) -> bool:
        try:
            if not self._has_valid_length(iban):
                logger.warning(f"Invalid length for IBAN: {iban}")
                return False

            # Only perform bank code check for countries with known 4-letter codes
            bank_codes = self.VALID_BANK_CODES.get(iban[:2])
            if bank_codes and iban[:2] in self.VALID_BANK_CODES:
                if not self._has_valid_bank_code(iban):
                    logger.warning(f"Unrecognized bank code for IBAN: {iban}")
                    # Don't return False — it's optional
                    pass

            # Rearrange for checksum validation
            rearranged = iban[4:] + iban[:4]
            numeric_iban = ''.join(str(int(ch, 36)) if ch.isalpha() else ch for ch in rearranged)

            return int(numeric_iban) % 97 == 1
        except Exception as e:
            logger.error(f"Checksum validation error for '{iban}': {e}")
            return False
