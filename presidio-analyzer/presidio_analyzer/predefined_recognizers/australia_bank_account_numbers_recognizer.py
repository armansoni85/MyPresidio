from typing import Optional, List, Set, Tuple
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import re
import csv

class AustraliaBankAccountRecognizer(PatternRecognizer):
    """
    Recognizer for Australian bank account + BSB pairs.

    Rules implemented:
     1. If keyword + correct account + correct BSB found -> score = 1.0
     2. If keyword + wrong account and/or wrong BSB found -> score = 0.6 (i.e. < 0.7)
     3. If NO keyword + correct account + correct BSB found -> score = 0.8 (i.e. > 0.7)
     4. Both account and BSB must be present in the text, otherwise no detection.
    """

    # Default BSB numbers (fallback if CSV not available)
    KNOWN_BSB_NUMBERS: Set[str] = {
        "012-785", "012-911", "013-961", "016-936", "016-985", "032-139", "033-141", "035-825",
        "062-136", "062-707", "062-904", "064-159", "085-645", "087-600", "105-069", "105-083",
        "105-110", "105-133", "105-141", "105-146", "105-152", "257-019", "257-028", "257-037",
        "257-046", "257-055", "257-064", "257-073", "257-082", "257-091", "257-100", "257-109",
        "257-118", "257-127", "257-136", "257-145", "257-154", "257-163", "257-172", "257-181",
        "257-190", "257-199", "257-208", "257-217", "257-235", "257-244", "257-253", "257-262",
        "257-271", "257-280", "257-289", "257-298", "482-158", "482-160", "484-095", "484-113",
        "484-121", "484-122", "484-123", "484-129", "484-133", "484-191", "484-192", "484-193",
        "484-194", "484-482", "484-552", "484-799", "484-888", "484-911", "484-915", "732-139",
        "733-141", "762-136", "762-707", "762-904", "764-159", "802-887", "803-110", "803-136",
        "803-420", "803-421", "807-125", "807-126", "808-269", "808-270", "808-271", "808-272",
        "808-273", "808-274", "808-275", "808-276", "808-277"
    }

    # Patterns
    PATTERNS = [
        Pattern(
            "Australia BSB+Account contiguous",
            r"\b(\d{3}[- ]?\d{3})[- ]?(\d{6,10})\b",
            0.85,
        ),
        Pattern(
            "Australia Account only",
            r"\b\d{6,10}\b",
            0.5,
        ),
        Pattern(
            "Australia BSB only",
            r"\b\d{3}[- ]?\d{3}\b",
            0.8,
        ),
    ]

    # Expanded context keywords
    CONTEXT = [
        "bank account", "account number", "acct num", "a/c", "account", "bsb",
        "australia bank account", "account number with bsb", "acc no.", "bank details",
        "fund transfer", "banking information", "account holder", "a/c number",
        "account info", "bank info", "banking info", "banking acct", "account info bsb",
        "account number bsb"
    ]

    NEGATIVE_CONTEXT = [
        "driver", "license", "licence", "permit", "identification"
    ]

    DEFAULT_PAIR_WINDOW = 120

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "AUSTRALIA_BANK_ACCOUNT",
        known_bsb_numbers: Optional[Set[str]] = None,
        csv_file_path: Optional[str] = "usr/share/data/bsb_code.csv",  # Add CSV path parameter
        pair_window: int = DEFAULT_PAIR_WINDOW,
    ):
        patterns = patterns if patterns is not None else self.PATTERNS

        # Load BSB numbers from CSV if path provided
        if csv_file_path:
            self.known_bsb_numbers = self._load_bsb_from_csv(csv_file_path)
        else:
            # Use provided set or fallback to default
            self.known_bsb_numbers = known_bsb_numbers if known_bsb_numbers is not None else self.KNOWN_BSB_NUMBERS

        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,  # Pass context as-is to parent
            supported_language=supported_language,
        )

        self.supported_entity = supported_entity
        self.pair_window = pair_window

        # Use the context that was passed or default to CLASS CONTEXT
        effective_context = context if context is not None else self.CONTEXT

        # lowercase keywords for robust detection - use these internally for your logic
        self.lower_context_set = set(kw.lower() for kw in effective_context)
        self.lower_negative_set = set(kw.lower() for kw in self.NEGATIVE_CONTEXT)

    def _load_bsb_from_csv(self, csv_file_path: str) -> Set[str]:
        """Load BSB numbers from the first column of CSV file"""
        bsb_numbers = set()
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                for row in reader:
                    if row and len(row) > 0:
                        bsb = row[0].strip().strip('"')
                        bsb_numbers.add(bsb)
            print(f"Loaded {len(bsb_numbers)} BSB numbers from {csv_file_path}")
        except FileNotFoundError:
            print(f"CSV file {csv_file_path} not found, using default BSB numbers")
            return self.KNOWN_BSB_NUMBERS
        except Exception as e:
            print(f"Error loading BSB from CSV {csv_file_path}: {e}, using default BSB numbers")
            return self.KNOWN_BSB_NUMBERS
        
        return bsb_numbers

    def _normalize_bsb(self, raw_bsb: str) -> Optional[str]:
        """Normalize forms like '123456', '123 456', '123-456' -> '123-456'"""
        if not raw_bsb:
            return None
        digits = re.sub(r"\D", "", raw_bsb)
        if len(digits) == 6:
            return f"{digits[:3]}-{digits[3:]}"
        return None

    def analyze(self, text: str, entities: Optional[List[str]] = None, nlp_artifacts=None) -> List[RecognizerResult]:
        results: List[RecognizerResult] = []
        if not text:
            return results

        text_lower = text.lower()
        account_iter = list(re.finditer(r"\b\d{6,10}\b", text))
        bsb_iter = list(re.finditer(r"\b\d{3}[- ]?\d{3}\b", text))

        # Rule 4: both account and BSB must be present
        if not account_iter or not bsb_iter:
            return results

        # check keywords robustly
        has_keyword = any(kw in text_lower for kw in self.lower_context_set)
        negative_present = any(neg in text_lower for neg in self.lower_negative_set)

        seen_spans: Set[Tuple[int, int]] = set()

        for acc_m in account_iter:
            acc_val = acc_m.group()
            acc_start, acc_end = acc_m.start(), acc_m.end()
            valid_account = acc_val.isdigit() and 6 <= len(acc_val) <= 10

            for bsb_m in bsb_iter:
                raw_bsb = bsb_m.group()
                bsb_start, bsb_end = bsb_m.start(), bsb_m.end()

                # correct distance calculation
                distance = max(acc_end, bsb_end) - min(acc_start, bsb_start)
                if distance > self.pair_window:
                    continue

                normalized_bsb = self._normalize_bsb(raw_bsb)
                valid_bsb = normalized_bsb in self.known_bsb_numbers if normalized_bsb else False

                # Apply rules
                if has_keyword and valid_account and valid_bsb:
                    score = 1.0
                    reason = "keyword + valid account + valid BSB (rule 1)"
                elif has_keyword and (not valid_account or not valid_bsb):
                    score = 0.6
                    reason = "keyword + invalid account or invalid BSB (rule 2)"
                elif (not has_keyword) and valid_account and valid_bsb:
                    score = 0.8
                    reason = "no keyword + valid account + valid BSB (rule 3)"
                else:
                    continue

                # negative context reduces score except for rule 1
                if negative_present and not (has_keyword and valid_account and valid_bsb):
                    score *= 0.15
                    reason += " + negative context applied"

                start = min(acc_start, bsb_start)
                end = max(acc_end, bsb_end)

                if (start, end) in seen_spans:
                    continue
                seen_spans.add((start, end))

                results.append(
                    RecognizerResult(
                        entity_type=self.supported_entity,
                        start=start,
                        end=end,
                        score=score,
                        analysis_explanation=f"{reason} -> account={acc_val}, bsb={normalized_bsb}"
                    )
                )

        return results
