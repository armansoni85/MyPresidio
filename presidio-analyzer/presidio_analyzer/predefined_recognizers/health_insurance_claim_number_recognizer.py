import re
from typing import Optional, List, Tuple
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult


class HICNRecognizer(PatternRecognizer):
    """
    Recognizes and validates both Health Insurance Claim Numbers (HICNs)
    and Medicare Beneficiary Identifiers (MBIs).

    Scoring rules:
        1. keywords + incorrect ID → score < 0.7
        2. no keywords + correct ID → score > 0.7
        3. keywords + correct ID → score > 0.8 or 1.0
    """

    # -------------------------------
    # Regex Patterns
    # -------------------------------
    PATTERNS = [
        # HICN legacy patterns (accepts hyphens/spaces)
        Pattern(
            "HICN (Legacy)",
            r"\b\d{3}[- ]?\d{2}[- ]?\d{4}[A-Z0-9]{1,2}\b",
            0.5,
        ),
        # MBI modern patterns (accepts hyphens/spaces)
        Pattern(
            "MBI (Modern)",
            r"\b[1-9][A-CE-HJ-NP-RT-WY][0-9][A-CE-HJ-NP-RT-WY]{2}[0-9][A-CE-HJ-NP-RT-WY]{2}[0-9]{2}\b"
            r"|\b[1-9][A-CE-HJ-NP-RT-WY][0-9][- ]?[A-CE-HJ-NP-RT-WY]{2}[- ]?[0-9][- ]?[A-CE-HJ-NP-RT-WY]{2}[- ]?[0-9]{2}\b",
            0.5,
        ),
    ]

    # Contextual keywords
    CONTEXT = [
        "medicare",
        "medicaid",
        "health insurance claim number",
        "hicn",
        "mbi",
        "cms",
        "medicare beneficiary",
        "social security",
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "HICN",
        replacement_pairs: Optional[List[Tuple[str, str]]] = None,
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    # -------------------------------
    # Add safe context checker
    # -------------------------------
    def _has_context(self, text: str, result: RecognizerResult) -> bool:
        """
        Detect whether any of the recognizer's context keywords appear
        within 100 characters before or after the match.
        """
        window = 100
        start = max(0, result.start - window)
        end = min(len(text), result.end + window)
        segment = text[start:end].lower()

        for kw in self.CONTEXT:
            if kw.lower() in segment:
                return True
        return False

    # -------------------------------
    # Validation Functions
    # -------------------------------
    def _is_valid_hicn(self, hicn: str) -> bool:
        """Check structural and basic integrity for legacy HICNs."""
        hicn = hicn.replace("-", "").replace(" ", "").upper()
        if not re.fullmatch(r"\d{9}[A-Z0-9]{1,2}", hicn):
            return False

        ssn = hicn[:9]
        # SSN validity checks
        if ssn.startswith(("000", "666")) or ssn[0] == "9":
            return False
        if ssn[3:5] == "00" or ssn[5:] == "0000":
            return False
        return True

    def _is_valid_mbi(self, mbi: str) -> bool:
        """Validate MBI format per CMS rules."""
        mbi = mbi.replace("-", "").replace(" ", "").upper()
        if len(mbi) != 11:
            return False

        if any(ch in "IOSLBZ" for ch in mbi):
            return False

        pattern = re.compile(
            r"^[1-9][A-CE-HJ-NP-RT-WY][0-9][A-CE-HJ-NP-RT-WY]{2}"
            r"[0-9][A-CE-HJ-NP-RT-WY]{2}[0-9]{2}$"
        )
        if not pattern.match(mbi):
            return False

        # Basic checksum-like integrity check
        digits = [int(d) for d in mbi if d.isdigit()]
        if not digits or sum(digits) % 10 == 0:
            return False

        return True

    # -------------------------------
    # Scoring Logic
    # -------------------------------
    def _compute_score(self, has_context: bool, valid: bool) -> float:
        """Apply custom scoring rules."""
        if has_context and not valid:
            return 0.5  # Case 1
        elif not has_context and valid:
            return 0.8  # Case 2
        elif has_context and valid:
            return 1.0  # Case 3
        else:
            return 0.4  # fallback

    # -------------------------------
    # Override analyze()
    # -------------------------------
    def analyze(
        self,
        text: str,
        entities: Optional[List[str]] = None,
        nlp_artifacts=None,
    ) -> List[RecognizerResult]:
        base_results = super().analyze(text, entities, nlp_artifacts)
        validated_results = []

        for result in base_results:
            value = text[result.start:result.end].strip()
            norm_val = value.replace("-", "").replace(" ", "").upper()

            # Determine type (HICN vs MBI)
            if re.fullmatch(r"\d{9}[A-Z0-9]{1,2}", norm_val):
                valid = self._is_valid_hicn(value)
            else:
                valid = self._is_valid_mbi(value)

            # Safe context detection
            context_found = self._has_context(text, result)

            # Compute score
            result.score = self._compute_score(context_found, valid)
            validated_results.append(result)

        return validated_results

