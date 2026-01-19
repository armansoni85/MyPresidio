from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class BGFinincCustomWordlistRecognizer(PatternRecognizer):
    logger.info("Initializing Custom Wordlist Recognizer...")

    # Define the custom wordlist
    WORDLIST = [
        "W2", "1099", "1096", "W4", "1040", "1065", "1120s", "1120", "990", 
        "1040x", "Income taxes", "Payroll taxes", "Financials", "Profit and loss", 
        "Income statement", "Balance sheet", "Social security", "Tax id", "Federal id", "EIN"
    ]

    def __init__(
        self,
        patterns: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "BGFininc_CUSTOM_WORDLIST",
    ):
        # Compile regex pattern for the wordlist
        wordlist_pattern = r"\b(?:{})\b".format("|".join(re.escape(word) for word in self.WORDLIST))
        patterns = [Pattern("Custom Wordlist Pattern", wordlist_pattern, 1.0)]  # High confidence for any match
        super().__init__(supported_entity=supported_entity, patterns=patterns, supported_language=supported_language)

    def analyze(
        self, text: str, entities: List[str], nlp_artifacts=None
    ) -> List[RecognizerResult]:
        logger.info(f"Analyzing text for Custom Wordlist: {text}")
        results = super().analyze(text, entities, nlp_artifacts)
        
        for result in results:
            logger.info(f"Detected keyword: {text[result.start:result.end]} with high confidence.")
        return results