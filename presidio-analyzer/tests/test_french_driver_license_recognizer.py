import pytest
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.predefined_recognizers import FrenchDriverLicenseRecognizer

@pytest.fixture(scope="module")
def recognizer():
    return FrenchDriverLicenseRecognizer()

@pytest.fixture(scope="module")
def entities():
    return ["FRENCH_DRIVER_LICENSE"]

@pytest.mark.parametrize(
    "text, expected_len, expected_positions",
    [
        # fmt: off
        # valid French driver's license numbers
        ("Permis de Conduire: 123456789012", 1, ((20, 32),),),
        ("My French driver's license number is 123456789012", 1, ((35, 47),),),
        ("123456789012 is a sample French driver's license number.", 1, ((0, 12),),),
        # invalid French driver's license numbers
        ("This is not a valid license number: 1234A5678901", 0, ()),
        ("Another invalid example: 123456789A", 0, ()),
        # fmt: on
    ],
)
def test_when_all_french_driver_license_numbers_then_succeed(
    text, expected_len, expected_positions, recognizer, entities
):
    # Initialize the AnalyzerEngine
    analyzer = AnalyzerEngine()
    # Add the custom recognizer to the analyzer
    analyzer.registry.add_recognizer(recognizer)
    # Analyze the text
    results = analyzer.analyze(text=text, language="fr")
    # Assertions
    assert len(results) == expected_len
    for res, (st_pos, fn_pos) in zip(results, expected_positions):
        assert res.entity_type == entities[0]
        assert res.start == st_pos
        assert res.end == fn_pos
        assert res.score > 0.8  # Assuming a threshold score for high confidence
