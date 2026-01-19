import pytest
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.predefined_recognizers import FranceNationalIDRecognizer
@pytest.fixture(scope="module")
def recognizer():
    return FranceNationalIDRecognizer()

@pytest.fixture(scope="module")
def entities():
    return ["FRANCE_NATIONAL_ID"]

@pytest.mark.parametrize(
    "text, expected_len, expected_positions",
    [
        # fmt: off
        # valid French National Identification Numbers
        ("Numéro de Sécurité Sociale: 1123456789012", 1, ((27, 39),),),
        ("Le numéro d'identité est 2120120330076, et la carte nationale d'identité est valide.", 1, ((26, 38),),),
        ("The identification number is 3120123456789.", 1, ((28, 40),),),
        # invalid French National Identification Numbers
        ("This number 123456789 is not a valid French National ID.", 0, ()),
        ("Invalid format: ABC1234567XYZ.", 0, ()),
        ("Another invalid example: 123456789012345.", 0, ()),
        # fmt: on
    ],
)
def test_when_all_french_national_ids_then_succeed(
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
