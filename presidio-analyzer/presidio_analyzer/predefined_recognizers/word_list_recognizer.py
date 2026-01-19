from typing import Optional, List

from presidio_analyzer import Pattern, PatternRecognizer


class WordlistRecognizer(PatternRecognizer):
    """
    Recognizes various wordlist phrases.

    This recognizer identifies specific phrases commonly found in wordlists,
    such as tax forms, financial statements, and identifiers.

    :param patterns: List of patterns to be used by this recognizer
    :param context: List of context words to increase confidence in detection
    :param supported_language: Language this recognizer supports
    :param supported_entity: The entity this recognizer can detect
    """

    PATTERNS = [
        Pattern("W2", r"\b(W2)\b", 0.8),
        Pattern("1099", r"\b(1099)\b", 0.8),
        Pattern("1096", r"\b(1096)\b", 0.8),
        Pattern("W4", r"\b(W4)\b", 0.8),
        Pattern("1040", r"\b(1040)\b", 0.8),
        Pattern("1065", r"\b(1065)\b", 0.8),
        Pattern("1120s", r"\b(1120s)\b", 0.8),
        Pattern("1120", r"\b(1120)\b", 0.8),
        Pattern("990", r"\b(990)\b", 0.8),
        Pattern("1040x", r"\b(1040x)\b", 0.8),
        Pattern("Income taxes", r"\b(Income\s+taxes)\b", 0.8),
        Pattern("Payroll taxes", r"\b(Payroll\s+taxes)\b", 0.8),
        Pattern("Financials", r"\b(Financials)\b", 0.8),
        Pattern("Profit and loss", r"\b(Profit\s+and\s+loss)\b", 0.8),
        Pattern("Income statement", r"\b(Income\s+statement)\b", 0.8),
        Pattern("Balance sheet", r"\b(Balance\s+sheet)\b", 0.8),
        Pattern("Social security", r"\b(Social\s+security)\b", 0.8),
        Pattern("Tax id", r"\b(Tax\s+id)\b", 0.8),
        Pattern("Federal id", r"\b(Federal\s+id)\b", 0.8),
        Pattern("EIN", r"\b(EIN)\b", 0.8),
    ]

    CONTEXT = [
        "wordlist",
        "tax forms",
        "financial statements",
        "identifiers",
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "WORDLIST",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )