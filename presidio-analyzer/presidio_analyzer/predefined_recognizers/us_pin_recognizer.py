import re
from presidio_analyzer import Pattern, PatternRecognizer

# Define the recognizer class for patient identifiers
class PatientIDRecognizer(PatternRecognizer):
    """Recognize 4+ digit numbers in proximity to specific patient identification keywords."""
    
    # Define patterns to detect 4+ digit numbers near specific keywords
    PATTERNS = [
        # Pattern to detect 4+ digit numbers in proximity to patient identification keywords
        Pattern(
            name="PatientID (medium)", 
            regex=r"(?i)\b(?:Patient Identification Number|Patient Id|Patient Number|Patient #|PIN)\b.{0,20}\b([0-9]{4,})\b", 
            score=0.5
        )
    ]
    
    CONTEXT = [
        "patient",
        "identification",
        "id",
        "number",
        "pin"
    ]
    
    def __init__(self, patterns=None, context=None, supported_language="en", supported_entity="PATIENT_ID"):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

# Example usage of the recognizer
# text = """
# The patient's Patient Identification Number is 123456 and their Patient Id is 7890.
# Additionally, their PIN is 12345 and Patient # is 67890.
# """

# # Instantiate the recognizer
# recognizer = PatientIDRecognizer()

# # Analyze the text
# results = recognizer.analyze(text, entities=["PATIENT_ID"], language="en")

# # Print the results
# for result in results:
#     print({
#         "entity_type": result.entity_type,
#         "start": result.start,
#         "end": result.end,
#         "score": result.score,
#         "text": text[result.start:result.end]
#     })
