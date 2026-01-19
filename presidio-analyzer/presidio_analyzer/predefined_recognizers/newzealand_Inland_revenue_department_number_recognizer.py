from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import re

class NewZealandInlandRevenueDepartmentNumberRecognizer(PatternRecognizer):

    # ---------------------------------------------------------------------
    # Define patterns for NZ IRD Numbers
    # ---------------------------------------------------------------------
    PATTERNS = [
        Pattern(
            "NZ_IRD_With_Delimiters",
            r"\b\d{2,3}[-\s]?\d{3}[-\s]?\d{3}\b",
            0.75,
        ),
        Pattern(
            "NZ_IRD_8_Digit",
            r"\b\d{8}\b",
            0.6,
        ),
        Pattern(
            "NZ_IRD_9_Digit",
            r"\b\d{9}\b",
            0.6,
        ),
    ]

    CONTEXT = [
        "ird", "ird number", "ird no", "ird no.", "ird no#", "ird #",
        "inland revenue", "inland revenue department",
        "new zealand ird", "new zealand inland revenue",
        "tax number", "tax file number", "nz tax", "nz tax number",
        "taxpayer number", "ird account"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "NewZealand_Inland_Revenue_Department_Number",
    ):
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns or self.PATTERNS,
            context=context or self.CONTEXT,
            supported_language=supported_language,
        )

    # ---------------------------------------------------------------------
    # Analyze Text
    # ---------------------------------------------------------------------
    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        print("\nAnalyzing text:", text)
        results = super().analyze(text, entities, nlp_artifacts)
        final_results = []

        has_context = self._has_context_keywords(text)
        print(f"→ Context keywords found: {has_context}")

        for result in results:
            original = text[result.start:result.end]
            cleaned = re.sub(r"[-\s]", "", original)
            print(f"\nFound possible IRD: '{original}' → cleaned: {cleaned}")

            valid_checksum = self._is_valid_ird_checksum(cleaned)
            print(f"→ Checksum valid: {valid_checksum}")

            new_score = self._calculate_score(has_context, valid_checksum, result.score)
            print(f"→ Final score: {new_score} (context={has_context}, valid={valid_checksum})")

            result.score = new_score
            final_results.append(result)

        return final_results

    # ---------------------------------------------------------------------
    # Check Context Keywords
    # ---------------------------------------------------------------------
    def _has_context_keywords(self, text: str) -> bool:
        text_lower = text.lower()
        for kw in self.CONTEXT:
            if re.search(r"\b" + re.escape(kw) + r"\b", text_lower):
                print(f"✓ Found context keyword: '{kw}'")
                return True
        return False

    # ---------------------------------------------------------------------
    # Scoring Logic
    # ---------------------------------------------------------------------
    def _calculate_score(self, has_context: bool, is_valid_checksum: bool, pattern_score: float) -> float:
        """
        Scoring rules:
          - Context + valid checksum    → 1.0 (High)
          - No context + valid checksum → 0.8 (Medium)
          - Context + invalid checksum  → 0.3 (Low)
          - No context + invalid        → 0.1 (Very Low)
        """
        if has_context and is_valid_checksum:
            return 1.0
        elif not has_context and is_valid_checksum:
            return 0.8
        elif has_context and not is_valid_checksum:
            return 0.3
        else:
            return 0.1

    # ---------------------------------------------------------------------
    # NZ IRD Modulus 11 Checksum Validation
    # ---------------------------------------------------------------------
    def _is_valid_ird_checksum(self, number: str) -> bool:
        """
        Validates NZ Inland Revenue Department (IRD) numbers using the correct modulus-11 algorithm.
        Supports 8-digit and 9-digit numbers.
        """

        # Remove any non-digit characters
        number = re.sub(r"\D", "", number)

        if len(number) == 8:
            digits = [int(d) for d in number]
            weights = [3, 2, 7, 6, 5, 4, 3, 2]
            check_digit = digits[-1]  # last digit
            total = sum(d * w for d, w in zip(digits, weights))
            remainder = total % 11
            print(f"8-digit IRD | Number: {number} | Total={total} | Remainder={remainder} | Check digit={check_digit}")

            if remainder == 0:
                return check_digit == 0
            elif remainder == 1:
                return False
            else:
                expected = 11 - remainder
                return check_digit == expected

        elif len(number) == 9:
            digits = [int(d) for d in number]
            # Standard primary weighting for 9-digit IRD
            primary = [3, 2, 7, 6, 5, 4, 3, 2]
            # Apply only to the first 8 digits
            total = sum(d * w for d, w in zip(digits[:8], primary))
            remainder = total % 11
            check_digit = digits[8]
            print(f"9-digit IRD | Number: {number} | Total={total} | Remainder={remainder} | Check digit={check_digit}")

            if remainder == 0:
                return check_digit == 0
            elif remainder == 1:
                # Try secondary weighting (legacy 9-digit numbers)
                secondary = [7, 4, 3, 2, 5, 2, 7, 6]
                total2 = sum(d * w for d, w in zip(digits[:8], secondary))
                remainder2 = total2 % 11
                expected2 = 11 - remainder2 if remainder2 not in (0, 1) else None
                print(f"Secondary 9-digit check | Total={total2} | Remainder={remainder2} | Expected={expected2}")
                if remainder2 == 0:
                    return check_digit == 0
                elif remainder2 == 1:
                    return False
                else:
                    return check_digit == expected2
            else:
                expected = 11 - remainder
                return check_digit == expected

        else:
            print(f"✗ Invalid IRD length: {len(number)}")
            return False


