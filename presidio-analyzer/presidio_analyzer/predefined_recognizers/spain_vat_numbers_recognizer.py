import re
import logging
from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult

logger = logging.getLogger("presidio-analyzer")
logger.setLevel(logging.DEBUG)


class SpainVATRecognizer(PatternRecognizer):
    logger.info("Initializing Spain VAT Number Recognizer...")

    # Define patterns for Spanish VAT Numbers (CIF, NIF, NIE)
    PATTERNS = [
        Pattern(
            "Spain VAT - CIF",
            r"(?i)\b(?:ES[-\s]?)?[A-HJUV][-\s]?\d{7}[-\s]?[0-9A-J]\b",
            0.5,
        ),
        Pattern(
            "Spain VAT - NIF/NIE",
            r"(?i)\b(?:ES[-\s]?)?(?:\d{8}[A-Z]|[KLMXYZ][0-9]{7}[A-Z])\b",
            0.5,
        ),
        Pattern(
            "Spain VAT - Simple CIF",
            r"(?i)\b[A-HJUV]\d{7}[0-9A-J]\b",
            0.5,
        ),
    ]

    CONTEXT = ["VAT", "IVA", "número de IVA", "VAT number", "número de identificación fiscal"]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "es",
        supported_entity: str = "SPAIN_VAT_NUMBER",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    # ------------------------------
    # CIF VALIDATION (Checksum check)
    # ------------------------------
    def _is_valid_cif(self, cif: str) -> bool:
        """
        Validate Spanish CIF checksum according to AEAT rules.
        """
        # Normalize the CIF - remove ES prefix, dashes, spaces and convert to uppercase
        cif = cif.upper().replace("ES", "").replace("-", "").replace(" ", "")
        logger.debug(f"Normalized CIF: {cif}")

        # Validate format
        if not re.match(r"^[A-HJUV][0-9]{7}[0-9A-J]$", cif):
            logger.debug("CIF format invalid.")
            return False

        first_letter = cif[0]
        digits = cif[1:8]  # 7 digits
        control = cif[-1]

        total = 0
        for i, d in enumerate(digits):
            n = int(d)
            if i % 2 == 0:  # Even positions (0,2,4,6) in 0-based indexing
                n *= 2
                if n > 9:
                    n = (n // 10) + (n % 10)
            total += n

        control_digit = (10 - (total % 10)) % 10
        control_letter = "JABCDEFGHI"[control_digit]

        valid = False
        if first_letter in "ABEH":  # Normal entities
            valid = control == str(control_digit)
        elif first_letter in "KPQS":  # Public entities
            valid = control == control_letter
        else:  # Other entities (C, D, etc.)
            valid = control == str(control_digit) or control == control_letter

        logger.debug(f"CIF={cif}, Total={total}, Control digit={control_digit}, Control letter={control_letter}, Expected control={control_digit}/{control_letter}, Found control={control}, Valid={valid}")
        return valid

    # ------------------------------
    # NIF VALIDATION (Checksum check)
    # ------------------------------
    def _is_valid_nif(self, nif: str) -> bool:
        """
        Validate Spanish NIF checksum.
        """
        nif = nif.upper().replace("ES", "").replace("-", "").replace(" ", "")
        logger.debug(f"Normalized NIF: {nif}")

        # NIF validation (8 digits + 1 letter)
        if re.match(r"^\d{8}[A-Z]$", nif):
            digits = nif[:8]
            letter = nif[8]
            n = int(digits)
            calculated_letter = "TRWAGMYFPDXBNJZSQVHLCKE"[n % 23]
            valid = letter == calculated_letter
            logger.debug(f"NIF={nif}, Calculated letter={calculated_letter}, Valid={valid}")
            return valid

        return False

    # ------------------------------
    # NIE VALIDATION (Checksum check)
    # ------------------------------
    def _is_valid_nie(self, nie: str) -> bool:
        """
        Validate Spanish NIE checksum.
        """
        nie = nie.upper().replace("ES", "").replace("-", "").replace(" ", "")
        logger.debug(f"Normalized NIE: {nie}")

        # NIE validation (X/Y/Z + 7 digits + 1 letter)
        if re.match(r"^[KLMXYZ][0-9]{7}[A-Z]$", nie):
            # Replace first letter: X=0, Y=1, Z=2
            letter_map = {"X": "0", "Y": "1", "Z": "2", "K": "0", "L": "0", "M": "0"}
            first_digit = letter_map.get(nie[0], "0")
            digits = first_digit + nie[1:8]
            letter = nie[8]
            n = int(digits)
            calculated_letter = "TRWAGMYFPDXBNJZSQVHLCKE"[n % 23]
            valid = letter == calculated_letter
            logger.debug(f"NIE={nie}, Mapped digits={digits}, Calculated letter={calculated_letter}, Valid={valid}")
            return valid

        return False

    # ------------------------------
    # MAIN ANALYSIS METHOD
    # ------------------------------
    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        logger.info(f"Analyzing text: {text}")
        results = super().analyze(text, entities, nlp_artifacts)

        for result in results:
            vat_number = text[result.start:result.end]
            clean_vat = vat_number.upper().replace("ES", "").replace("-", "").replace(" ", "")
            logger.debug(f"Detected VAT Number: {vat_number}, Cleaned: {clean_vat}")

            score = 0.5  # Default score if pattern matches

            # Validate CIF (companies) - format: [A-HJUV] + 7 digits + control
            if re.match(r"^[A-HJUV][0-9]{7}[0-9A-J]$", clean_vat):
                if self._is_valid_cif(clean_vat):
                    score = 1.0
                    logger.debug(f"Valid CIF: {clean_vat}")
                else:
                    score = 0.3  # Invalid checksum
                    logger.debug(f"Invalid CIF checksum: {clean_vat}")

            # Validate NIF (individuals) - format: 8 digits + letter
            elif re.match(r"^\d{8}[A-Z]$", clean_vat):
                if self._is_valid_nif(clean_vat):
                    score = 1.0
                    logger.debug(f"Valid NIF: {clean_vat}")
                else:
                    score = 0.3  # Invalid checksum
                    logger.debug(f"Invalid NIF checksum: {clean_vat}")

            # Validate NIE (foreigners) - format: X/Y/Z + 7 digits + letter
            elif re.match(r"^[KLMXYZ][0-9]{7}[A-Z]$", clean_vat):
                if self._is_valid_nie(clean_vat):
                    score = 1.0
                    logger.debug(f"Valid NIE: {clean_vat}")
                else:
                    score = 0.3  # Invalid checksum
                    logger.debug(f"Invalid NIE checksum: {clean_vat}")

            # Adjust score based on context keywords
            if any(keyword.lower() in text.lower() for keyword in self.CONTEXT):
                if score >= 0.7:
                    score = 1.0
                else:
                    score = min(0.6, score + 0.1)  # Boost score slightly if context present

            result.score = score
            logger.info(f"Final VAT={vat_number}, Score={score}")

        return results
