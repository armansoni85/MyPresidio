import re
import logging
from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult, AnalysisExplanation

logger = logging.getLogger("presidio-analyzer")


class BFSInvestCustomAccountRecognizer(PatternRecognizer):
    """
    Custom recognizer for BFSInvest account numbers loaded from a file.
    """

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "BFSInvest_CUSTOM_ACCOUNT",
        account_list_file: str = "/usr/share/data/bgf_custom_acc.txt",
        enable_case_insensitivity: bool = True,
    ):
        # Define early to avoid AttributeError before super()
        self.supported_entity = supported_entity
        self.supported_language = supported_language
        self.context = context or ["account", "id", "bfsinvest", "custom acc"]
        self.enable_case_insensitivity = enable_case_insensitivity

        # Load account list
        self.account_numbers = self._load_account_numbers(account_list_file)
        if not self.account_numbers:
            logger.warning(f"No valid account numbers loaded from {account_list_file}")

        # Build regex pattern
        account_pattern = "|".join(re.escape(acc) for acc in self.account_numbers) or "PLACEHOLDER"
        flags = re.IGNORECASE if enable_case_insensitivity else 0
        regex = re.compile(
            rf"""\b(?:account|id|bfsinvest)[\s:#-]*({account_pattern})\b""",
            re.VERBOSE | flags,
        )

        pattern = Pattern(name=supported_entity, regex=regex, score=0.85)

        # Call super init AFTER defining self fields
        super().__init__(
            supported_entity=supported_entity,
            patterns=[pattern],
            context=self.context,
            supported_language=supported_language,
        )

    def _load_account_numbers(self, file_path: str) -> List[str]:
        """Load and validate account numbers."""
        try:
            with open(file_path, "r") as file:
                return [line.strip() for line in file if line.strip()]
        except FileNotFoundError:
            logger.error(f"Account list file not found: {file_path}")
            return []
        except Exception as e:
            logger.exception(f"Error loading accounts: {str(e)}")
            return []

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        if not self.account_numbers:
            logger.debug("Skipping analysis - no account numbers available")
            return []

        if len(self.account_numbers) > 1000:
            return self._analyze_large_list(text)

        return super().analyze(text, entities, nlp_artifacts)

    def _analyze_large_list(self, text: str) -> List[RecognizerResult]:
        results = []
        account_set = {acc.lower() for acc in self.account_numbers}
        words = re.split(r"\s+", text)

        for word in words:
            clean = re.sub(r"^\W+|\W+$", "", word)
            if not clean:
                continue
            candidate = clean.lower() if self.enable_case_insensitivity else clean

            if candidate in account_set:
                start = text.find(clean)
                end = start + len(clean)

                # Build proper analysis explanation object
                explanation = AnalysisExplanation(
                    recognizer=self.__class__.__name__,
                    original_score=self.patterns[0].score,
                    textual_explanation=f"Matched account number '{clean}' from loaded list.",
                    pattern_name=self.patterns[0].name,
                )
                explanation.set_supportive_context_word(clean)

                logger.debug(f"[BFSInvestCustomAccountRecognizer] Matched account: {clean}")

                results.append(
                    RecognizerResult(
                        entity_type=self.supported_entity,
                        start=start,
                        end=end,
                        score=self.patterns[0].score,
                        analysis_explanation=explanation,
                    )
                )
        return results

    def validate_result(self, pattern_text: str) -> bool:
        """Validate matched account against loaded list."""
        if self.enable_case_insensitivity:
            return any(pattern_text.lower() == acc.lower() for acc in self.account_numbers)
        return pattern_text in self.account_numbers

