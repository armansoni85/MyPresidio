from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer
import re

class FerpaRecognizer(PatternRecognizer):
    """
    Recognizer for FERPA-related sensitive student data:
    - Student ID + Number (with numeric check)
    - Name + ID
    - Name + DOB
    - GPA + Transcript
    - Disciplinary records with number
    """

    # Words to ignore only if found within 8 words (~50 characters)
    NEARBY_IGNORE_WORDS = [
        "member", "parcel", "invoice", "sra", "pa", "tx", "vat", "vin", "vehicle",
        "insurance", "transaction", "medicade", "seller", "benefit", "caller",
        "tax", "taxpayer", "employer", "employee", "loan", "sample", "docket"
    ]

    PATTERNS = [
        # Student ID/number with actual number
        Pattern(
            name="FERPA Student ID or Number",
            regex=r"(?i)\bFERPA\b.*?\b(student|id|identification)\s*(number|num|no|nbr)?[^0-9]{0,5}(\d{4,})\b",
            score=0.85
        ),
        # ID + name + number
        Pattern(
            name="FERPA Student ID with Name",
            regex=r"(?i)(student\s*(id|number|no|num|nbr)[^0-9]{0,5}\d{4,}).*?\b(first name|last name|student name|record)\b",
            score=0.85
        ),
        # Student name + date of birth with actual date
        Pattern(
            name="FERPA Student Name with Date of Birth",
            regex=r"(?i)(student name|student id|identification).*(date of birth|birthdate).*(19\d{2}|20\d{2}|\d{1,2}[-/]\d{1,2}[-/]\d{2,4})",
            score=0.85
        ),
        # GPA with transcript/academic record
        Pattern(
            name="FERPA GPA with Transcript or Academic Record",
            regex=r"(?i)\b(gpa|grade point average)\b.{0,50}?\b(transcript|academic record)\b",
            score=0.85
        ),
        # Disciplinary + student ID with number
        Pattern(
            name="FERPA Disciplinary Records",
            regex=r"(?i)\b(disciplinary|discipline)\b.*?\b(student|id|identification)[^0-9]{0,5}(\d{4,})\b",
            score=0.85
        )
    ]

    CONTEXT = [
        "FERPA", "student", "student ID", "identification number",
        "birthdate", "date of birth", "GPA", "transcript",
        "academic record", "disciplinary record"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "FERPA",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List:
        results = super().analyze(text, entities, nlp_artifacts)
        filtered_results = []

        for result in results:
            if not self._has_nearby_ignored_words(text, result.start, result.end):
                filtered_results.append(result)

        return filtered_results

    def _has_nearby_ignored_words(self, text: str, start: int, end: int) -> bool:
        """
        Return True if any disqualifying keyword appears within 8 words (~50 chars) of the match.
        """
        window = 50  # Roughly 8 words
        context_window = text[max(0, start - window): min(len(text), end + window)].lower()

        for word in self.NEARBY_IGNORE_WORDS:
            if re.search(rf'\b{word}\b', context_window):
                return True
        return False

