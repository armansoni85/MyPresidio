from typing import Optional, List, Tuple
from presidio_analyzer import Pattern, PatternRecognizer

class FranceNationalIdRecognizer(PatternRecognizer):
    """
    Recognizes France National Identification Numbers (INSEE).

    The general format of the French National Identification Number (INSEE) is:
    SYYMMDDCCCOOOK, where:
    - S: Gender and citizenship information
    - YY: Last two digits of the year of birth
    - MM: Month of birth
    - DD: Department of origin
    - CCC: Commune of origin
    - OOO: Order number
    - KK: Control key or check digit

    This recognizer identifies INSEE numbers using regex patterns and context words.

    :param patterns: List of patterns to be used by this recognizer
    :param context: List of context words to increase confidence in detection
    :param supported_language: Language this recognizer supports
    :param supported_entity: The entity this recognizer can detect
    :param replacement_pairs: List of tuples with potential replacement values
    for different strings to be used during pattern matching.
    """

    PATTERNS = [
        Pattern(
            "France National ID (High)",
            r"\b[12]\d{2}\d{2}\d{2}\d{3}\d{3}\d{2}\b",  # SYYMMDDCCCOOOK (12 digits)
            0.85,
        ),
        Pattern(
            "France National ID (Medium)",
            r"\b[12]\d{2}\d{2}\d{2}[A-Za-z\d]{3}\d{3}\d{2}\b",  # SYYMMDD with combination of letters/digits
            0.7,
        ),
        Pattern(
            "France National ID (Low)",
            r"\b[12]\d{2}\d{2}\d{2}[A-Za-z\d]{6}\b",  # Nine digits and letters combination
            0.5,
        ),
    ]

    CONTEXT = [
        "carte nationale d'identité",
        "carte nationale d'identité no",
        "cni",
        "cni#",
        "national identification number",
        "national identity",
        "nationalidno#",
        "numéro d'assurance maladie",
        "numéro de carte vitale",
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "FR_NATIONAL_ID",
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
