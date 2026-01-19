from typing import Optional, List, Tuple
from presidio_analyzer import Pattern, PatternRecognizer

class AustraliaTaxFileNumberRecognizer(PatternRecognizer):
    """
    Recognizes Australian Tax File Numbers (TFNs).

    Australian TFNs are numeric codes formatted as:
    - Eight to nine digits typically presented with spaces or hyphens
    - Format: NNN NNN NNN, NNN-NNN-NNN, or a continuous 9-digit number

    This recognizer includes checksum validation and more specific context words
    to avoid overlap with other entities like BSB codes.
    """

    PATTERNS = [
        Pattern(
            "TFN (Continuous)",
            r"\b\d{8,9}\b",
            1.0,
        ),
        Pattern(
            "TFN (Spaces)",
            r"\b\d{3} \d{3} \d{2,3}\b",
            1.0,
        ),
        Pattern(
            "TFN (Hyphens)",
            r"\b\d{3}-\d{3}-\d{2,3}\b",
            1.0,
        ),
    ]

    CONTEXT = [
        "tax file number",
        "tfn",
        "tax",
        "australian tax",
        "tax return",
        "taxpayer",
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "AU_TFN",
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

    def validate_result(self, pattern_text: str) -> bool:
        """
        Validates the TFN using the checksum logic.
        """

        # Remove any spaces or hyphens
        tfn = pattern_text.replace(" ", "").replace("-", "")

        # Australian TFNs should be 9 digits long
        if len(tfn) != 9:
            return False

        # Define the weighting factors
        weights = [1, 4, 3, 7, 5, 8, 6, 9]

        # Calculate the checksum
        checksum = sum(int(tfn[i]) * weights[i] for i in range(8))

        # Valid TFNs have a checksum that is divisible by 11
        return checksum % 11 == 0