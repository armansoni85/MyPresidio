from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List
import re


class HICNRecognizer(PatternRecognizer):

    ENTITY_TYPE = "HICN"

    HICN_KEYWORDS = {
        "hicn",
        "health insurance claim number",
        "medicare hicn",
        "medicare number"
    }

    PATTERNS = [
        Pattern(
            name="hicn_no_delimiter",
            regex=r"\b[A-Z]{0,3}[0-9]{9}[0-9A-Z]{1,3}\b",
            score=0.7
        ),
        Pattern(
            name="hicn_space_delimited",
            regex=r"\b[A-Z]{0,3}[0-9]{3} [0-9]{6}[0-9A-Z]{1,3}\b",
            score=0.7
        ),
        Pattern(
            name="hicn_space_hyphen_delimited",
            regex=r"\b[A-Z]{0,3}-[0-9]{3} [0-9]{2}-[0-9]{4}-[0-9A-Z]{1,3}\b",
            score=0.7
        ),
        Pattern(
            name="hicn_hyphen_delimited",
            regex=r"\b[A-Z]{0,3}-[0-9]{3}-[0-9]{2}-[0-9]{4}-[0-9A-Z]{1,3}\b",
            score=0.7
        ),
    ]

    def __init__(self, supported_language: str = "en"):
        super().__init__(
            supported_entity=self.ENTITY_TYPE,
            patterns=self.PATTERNS,
            supported_language=supported_language
        )

    def _is_inside_url(self, text: str, start: int, end: int) -> bool:
        for match in re.finditer(r"https?://\S+", text):
            if start >= match.start() and end <= match.end():
                return True
        return False

    def _keyword_nearby(self, text: str, start: int, window: int = 50) -> bool:
        context = text.lower()[max(0, start - window): start + window]
        return any(keyword in context for keyword in self.HICN_KEYWORDS)

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None):
        results = []

        for pattern in self.patterns:
            for match in re.finditer(pattern.regex, text):
                start, end = match.start(), match.end()

                if self._is_inside_url(text, start, end):
                    continue

                score = 1.0 if self._keyword_nearby(text, start) else 0.7

                results.append(
                    RecognizerResult(
                        entity_type=self.ENTITY_TYPE,
                        start=start,
                        end=end,
                        score=score
                    )
                )

        return results
