from typing import List, Optional, Tuple
import re
from presidio_analyzer import Pattern, PatternRecognizer


class BusinessTerminologyRecognizer(PatternRecognizer):
    """
    Recognize business terminology based on specific patterns and logic from a rules file.

    The recognizer matches business terms defined in the `logic.txt` file according to
    specific matching and exclusion rules.

    """

    # Define patterns to match business terminology
    # Each pattern corresponds to a line in the logic.txt file
    PATTERNS = [
        Pattern(
            "accounts receivable turnover",
            r"\baccounts receivable turnover\b(?!.*\bcontract\b.*terms\s+\w{1,2}\s+conditions\b)",
            0.85,
        ),
        Pattern(
            "adjusted gross margin",
            r"\badjusted gross margin\b(?!.*\bcontract\b.*terms\s+\w{1,2}\s+conditions\b)",
            0.85,
        ),
        Pattern(
            "adjusted operating expenses",
            r"\badjusted operating expenses\b(?!.*\bcontract\b.*terms\s+\w{1,2}\s+conditions\b)",
            0.85,
        ),
        # Repeat for all patterns following the same approach...
    ]

    # Define patterns for terms that must appear in proximity to any of the above
    CONTEXT_PATTERNS = [
        Pattern(
            "confidential and not within 6 words",
            r"\bconfidential\b(?!\b.{0,50}\b(?:may contain|privileged|health information|individual)\b)",
            0.9,
        ),
        Pattern(
            "internal use only",
            r"\binternal use only\b",
            0.9,
        ),
        Pattern(
            "proprietary",
            r"\bproprietary\b",
            0.9,
        ),
    ]

    CONTEXT = [
        "business",
        "financial",
        "statement",
        "policy",
        "terminology",
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "BUSINESS_TERMINOLOGY",
    ):
        patterns = patterns if patterns else self.PATTERNS + self.CONTEXT_PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def validate_result(self, pattern_text: str) -> bool:
        """
        Validate the result by ensuring the pattern matches the business terminology rules.

        :param pattern_text: Text extracted by the pattern matching.
        :return: Boolean indicating if the result is valid.
        """
        # Logic to validate result based on extracted text and exclusion/inclusion rules.
        # Placeholder logic, implement as needed.
        return True

# Example usage
# if __name__ == "__main__":
#     recognizer = BusinessTerminologyRecognizer()

#     test_texts = [
#         "The adjusted gross margin should be considered in the financial statements.",
#         "Internal use only documents related to accounts receivable turnover.",
#         "This confidential document may contain sensitive information about sales.",
#         "Adjusted operating expenses are considered, but check contract terms and conditions.",
#     ]

#     for text in test_texts:
#         results = recognizer.analyze(text)
#         for result in results:
#             print(
#                 f"Text: {text}, Detected: {result.entity_type}, Start: {result.start}, End: {result.end}, Score: {result.score}"
#             )
