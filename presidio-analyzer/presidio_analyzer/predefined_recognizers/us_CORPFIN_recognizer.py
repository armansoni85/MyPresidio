import re
import logging
from typing import List, Optional
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult

logger = logging.getLogger("presidio-analyzer")


class USCorporateFinancialRecognizer(PatternRecognizer):
    """
    Recognizer to detect US Corporate Financial terms (SOX/Sarbanes-Oxley type)
    and valid SSN/FEN numbers. Includes validation to exclude fake or demo data.
    """

    # Core dictionary of corporate financial terms
    FINANCIAL_TERMS = [
        "accounts receivable turnover", "adjusted gross margin", "adjusted operating expenses",
        "adjusted operating margin", "administrative expenses", "capital employed turnover",
        "cost of sales", "earnings per share", "ebitda", "ebit", "net profit",
        "financial expenses", "financial liabilities", "interest expense", "financial income",
        "interest income", "forward guidance", "earnings forecast", "gross margin", "gross profit",
        "income before taxes", "pre-tax income", "inventory turnover", "inventory ratio",
        "net income", "net earnings", "bottom line", "net revenue", "net sales",
        "operating expenses", "opex", "operating income", "operating profit", "operating margin",
        "margin ratio", "return on capital employed", "roce", "return on assets", "roa",
        "return on equity", "roe", "return on sales", "ros", "selling expenses", "sales cost",
        "net interest income", "nii", "loan loss provision", "provision for credit losses",
        "non-performing loans", "npls", "tier 1 capital", "common equity tier 1",
        "capital adequacy ratio", "net interest margin", "nim", "assets under management", "aum",
        "basel iii compliance", "basel requirements", "risk-weighted assets", "rwa",
        "stress test results", "dividend payout ratio", "interest coverage ratio",
        "debt to equity ratio", "current ratio", "quick ratio", "cash flow from operations",
        "free cash flow", "working capital", "capital expenditures", "book value per share",
        "market capitalization", "price to earnings ratio", "p/e ratio", "price to book ratio",
        "p/b ratio", "enterprise value", "total liabilities", "shareholder equity",
        "revenue growth", "profit margin", "gross profit margin", "operating cash flow",
        "financial leverage", "debt ratio"
    ]

    # Valid SSN & FEN regexes
    SSN_REGEX = r"\b(?!000|666|9\d\d)\d{3}[- ]?(?!00)\d{2}[- ]?(?!0{4})\d{4}\b"
    FEN_REGEX = r"\b(?!00)\d{2}[- ]?(?!0{7})\d{7}\b"

    INVALID_SSN_SET = {"123456789", "987654320", "078051120"}
    INVALID_FEN_SET = {"123456789", "112345678", "000000000"}

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "US_CORPORATE_FINANCIALS",
    ):
        # Dummy pattern to satisfy Presidio requirements
        dummy_pattern = Pattern(name="dummy", regex=r"US_CORPORATE_FINANCIALS_DUMMY", score=0.01)
        patterns = patterns if patterns else [dummy_pattern]
        context = context if context else [
            "financials", "corporate", "sox", "sarbanes-oxley", "reporting",
            "balance sheet", "financial statement", "audit", "compliance"
        ]

        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    # --- MAIN ANALYZE FUNCTION ---
    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        results: List[RecognizerResult] = []

        # Exclusion rule
        if re.search(r"\bterms\s+and\s+conditions\b", text, re.IGNORECASE):
            logger.debug("Skipping due to 'terms and conditions'.")
            return results

        normalized_text = text.lower()
        normalized_text = re.sub(r"\$([0-9]+(\.[0-9]+)?)", r" \$\1", normalized_text)
        normalized_text = re.sub(r"([0-9]+-[0-9-]+)", r" \1 ", normalized_text)

        # --- 1. Detect Financial Terms ---
        for term in self.FINANCIAL_TERMS:
            escaped_term = re.escape(term)
            regex = rf"\b{escaped_term}\b"
            for match in re.finditer(regex, normalized_text, flags=re.IGNORECASE):
                start, end = match.span()
                matched_text = text[start:end]
                score = 0.7
                if self._has_context(text, start, end):
                    score = min(score + 0.2, 1.0)

                results.append(
                    RecognizerResult(
                        entity_type=self.supported_entity,
                        start=start,
                        end=end,
                        score=score,
                    )
                )
                logger.debug(f"Detected term: '{matched_text}' score={score}")

        # --- 2. Detect & Validate SSNs ---
        for match in re.finditer(self.SSN_REGEX, text):
            ssn_raw = match.group()
            if not self._is_valid_ssn(ssn_raw):
                continue
            start, end = match.span()
            score = 0.85
            if self._has_context(text, start, end):
                score = min(score + 0.1, 1.0)
            results.append(
                RecognizerResult(
                    entity_type=self.supported_entity,
                    start=start,
                    end=end,
                    score=score,
                )
            )
            logger.debug(f"Detected valid SSN: {ssn_raw}")

        # --- 3. Detect & Validate FEN/EINs ---
        for match in re.finditer(self.FEN_REGEX, text):
            fen_raw = match.group()
            if not self._is_valid_fen(fen_raw):
                continue
            start, end = match.span()
            score = 0.85
            if self._has_context(text, start, end):
                score = min(score + 0.1, 1.0)
            results.append(
                RecognizerResult(
                    entity_type=self.supported_entity,
                    start=start,
                    end=end,
                    score=score,
                )
            )
            logger.debug(f"Detected valid FEN/EIN: {fen_raw}")

        return results

    # --- VALIDATORS ---
    def _is_valid_ssn(self, ssn: str) -> bool:
        digits = re.sub(r"\D", "", ssn)
        if len(digits) != 9:
            return False
        if digits in self.INVALID_SSN_SET:
            return False
        if digits[:3] in {"000", "666"} or digits[3:5] == "00" or digits[5:] == "0000":
            return False
        return True

    def _is_valid_fen(self, fen: str) -> bool:
        digits = re.sub(r"\D", "", fen)
        if len(digits) != 9:
            return False
        if digits in self.INVALID_FEN_SET:
            return False
        if digits[:2] == "00" or digits[2:] == "0000000":
            return False
        return True

    # --- CONTEXT DETECTION ---
    def _has_context(self, text: str, start: int, end: int) -> bool:
        window = 80
        snippet = text[max(0, start - window): min(len(text), end + window)]
        return any(ctx.lower() in snippet.lower() for ctx in self.context)
