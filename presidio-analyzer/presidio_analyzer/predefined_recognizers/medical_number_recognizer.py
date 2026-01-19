from typing import Optional, List
import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult


class MedicalNumberRecognizer(PatternRecognizer):
    """
    Recognizes Medical Numbers (NPI or OPNI).

    This recognizer identifies:
      - NPI or OPNI followed by 10 digits
      - Accepts spaces or dashes between digits
      - Adjusts score based on keyword and format correctness:
          1. Keyword + correct 10-digit number → score = 1.0
          2. Keyword + wrong number format → score < 0.7
          3. No keyword but correct 10-digit number → score > 0.7
    """

    # Regex to capture optional spaces/dashes
    # Example matches:
    # NPI1234567890, NPI 1234567890, NPI 123-456-7890, OPNI 123 456 7890
    PATTERNS = [
        Pattern(
            name="NPI_pattern",
            regex=r"\bNPI[-\s]*([0-9][-\s]*){10}\b",
            score=0.8,
        ),
        Pattern(
            name="OPNI_pattern",
            regex=r"\bOPNI[-\s]*([0-9][-\s]*){10}\b",
            score=0.8,
        ),
        # Optional pattern for numbers without prefix (fallback)
        Pattern(
            name="MEDICAL_NUMBER_no_keyword",
            regex=r"\b(\d[-\s]*){10}\b",
            score=0.6,
        ),
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        supported_entity: str = "MEDICAL_NUMBER",
        supported_language: str = "en",
    ):
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns or self.PATTERNS,
            supported_language=supported_language,
        )

    def analyze(
        self,
        text: str,
        entities: Optional[List[str]] = None,
        nlp_artifacts=None,
    ) -> List[RecognizerResult]:
        """
        Override analyze() to apply custom scoring rules.
        """
        results = super().analyze(text, entities, nlp_artifacts)

        adjusted_results = []
        for result in results:
            entity_text = text[result.start:result.end]

            # Normalize by removing spaces and dashes
            digits_only = re.sub(r"[-\s]", "", entity_text)

            has_keyword = bool(re.search(r"\b(NPI|OPNI)\b", entity_text))
            is_valid_length = bool(re.match(r".*(\d{10}).*", digits_only))

            # --- Dynamic Scoring Rules ---
            if has_keyword and is_valid_length:
                result.score = 1.0
            elif has_keyword and not is_valid_length:
                result.score = 0.65
            elif not has_keyword and is_valid_length:
                result.score = 0.8
            else:
                result.score = 0.5  # fallback

            adjusted_results.append(result)

        return adjusted_results

