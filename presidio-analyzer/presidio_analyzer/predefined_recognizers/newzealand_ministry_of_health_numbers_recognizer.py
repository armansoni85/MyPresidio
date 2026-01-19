from typing import Optional, List, Dict, Any
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import logging
import re

logger = logging.getLogger("presidio-analyzer")

class NewZealandHealthNumberRecognizer(PatternRecognizer):
    # Define patterns with clear names
    PATTERNS = [
        Pattern(
            "NZ_Health_Number_Old",
            r"(?i)\b[A-HJ-NP-Z]{3}\d{4}\b",
            0.4
        ),
        Pattern(
            "NZ_Health_Number_New",
            r"(?i)\b[A-HJ-NP-Z]{3}\d{2}[A-HJ-NP-Z]\d\b",
            0.6
        ),
        Pattern(
            "HPI_Number_Format",
            r"(?i)\b[A-HJ-NP-Z]{3}\d{4}\b",
            0.4
        ),
        Pattern(
            "HPI_CPN_Format",
            r"(?i)\b\d{2,3}[A-HJ-NP-Z]{3,4}\b",
            0.5
        )
    ]

    CONTEXT = [
        # NHI context
        "health number", "nhi number", "nhi", "national health index",
        "ministry of health", "new zealand health number", "nz health number",
        "health id", "medical number", "patient id", "medical record",
        "health system", "national health identifier", "health index",
        
        # HPI context
        "hpi", "health practitioner index", "practitioner number",
        "doctor id", "nurse id", "health practitioner id", "provider id",
        "practitioner identifier", "nurse identifier", "doctor identifier",
        "health professional", "medical practitioner", "clinician id",
        "healthcare provider", "practitioner index",
        
        # HPI-CPN specific
        "hpi-cpn", "cpn", "common person number", "common person identifier",
        "health provider index", "te whatu ora", "hpi number", "cpi"
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "NZ_Health_Number",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def analyze(
        self, text: str, entities: List[str], nlp_artifacts=None
    ) -> List[RecognizerResult]:
        # First get base results
        base_results = super().analyze(text, entities, nlp_artifacts)
        enhanced_results = []
        
        # Create mapping from pattern regex to pattern name
        pattern_mapping = {pattern.regex: pattern.name for pattern in self.patterns}
        
        # Precompute context presence
        lower_text = text.lower()
        context_present = any(keyword in lower_text for keyword in self.CONTEXT)

        # Process each result
        for result in base_results:
            number = text[result.start:result.end]
            
            # Initialize recognition metadata if needed
            if result.recognition_metadata is None:
                result.recognition_metadata = {}
                
            # Store original score
            original_score = result.score
            
            # Identify pattern type using regex mapping
            pattern_name = None
            for regex, name in pattern_mapping.items():
                if re.match(regex, number):
                    pattern_name = name
                    result.recognition_metadata["pattern_name"] = name
                    break
            
            # Process based on pattern type
            if pattern_name == "NZ_Health_Number_Old" or pattern_name == "NZ_Health_Number_New":
                if self._is_valid_nhi_checksum(number):
                    result.score = 0.7  # Valid NHI
                    logger.info(f"Valid NHI checksum: {number}")
                else:
                    result.score = 0.1  # Invalid checksum
                    logger.warning(f"Invalid NHI checksum: {number}")
            
            elif pattern_name == "HPI_Number_Format":
                result.score = 0.5  # HPI base score
                logger.info(f"HPI number detected: {number}")
            
            elif pattern_name == "HPI_CPN_Format":
                if self._is_valid_cpn_format(number):
                    result.score = 0.6  # Valid HPI-CPN
                    logger.info(f"Valid HPI-CPN format: {number}")
                else:
                    result.score = 0.2  # Invalid format
                    logger.warning(f"Invalid HPI-CPN format: {number}")
            
            # Apply context boost
            if context_present:
                new_score = min(result.score + 0.3, 1.0)
                logger.info(f"Context boost for {number}: {result.score} -> {new_score}")
                result.score = new_score
            
            enhanced_results.append(result)
            
        return enhanced_results

    def _is_valid_nhi_checksum(self, number: str) -> bool:
        """Validate NHI checksum (both old and new formats)"""
        weights = [7, 6, 5, 4, 3, 2]
        letters_to_digits = {chr(i): i - 64 for i in range(65, 91) 
                            if chr(i) not in ['I', 'O']}
        
        try:
            num_upper = number.upper()
            
            if len(num_upper) == 7:  # Old NHI
                digits = [
                    letters_to_digits[num_upper[0]],
                    letters_to_digits[num_upper[1]],
                    letters_to_digits[num_upper[2]],
                    int(num_upper[3]), 
                    int(num_upper[4]), 
                    int(num_upper[5])
                ]
                provided_check = int(num_upper[6])
                
            elif len(num_upper) == 8:  # New NHI
                digits = [
                    letters_to_digits[num_upper[0]],
                    letters_to_digits[num_upper[1]],
                    letters_to_digits[num_upper[2]],
                    int(num_upper[3]), 
                    int(num_upper[4]),
                    letters_to_digits[num_upper[5]]
                ]
                provided_check = int(num_upper[7])
                
            else:
                return False

            total = sum(d * w for d, w in zip(digits, weights))
            remainder = total % 11
            check_digit = (11 - remainder) % 11
            
            if check_digit == 10:
                return False
                
            return check_digit == provided_check
            
        except (KeyError, ValueError, IndexError):
            return False

    def _is_valid_cpn_format(self, number: str) -> bool:
        """Validate HPI-CPN format requirements"""
        # Must be 6 characters total
        if len(number) != 6:
            return False
            
        # Check for invalid letters (I or O)
        if re.search(r"[IOio]", number):
            return False
            
        # Validate digit-letter structure
        if not re.match(r"^\d{2,3}[A-HJ-NP-Z]{3,4}$", number, re.IGNORECASE):
            return False
            
        return True
