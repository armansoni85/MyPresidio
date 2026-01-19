import pytest

from presidio_analyzer import AnalyzerEngine, RecognizerResult
from presidio_analyzer.predefined_recognizers import CanadaSINRecognizer


@pytest.fixture(scope="module")
def recognizer():
    return CanadaSINRecognizer()


@pytest.fixture(scope="module")
def entities():
    return ["CANADA_SIN"]


@pytest.mark.parametrize(
    "text, expected_len, expected_position, expected_score",
    [
        ("SIN: 989798889", 1, (5, 14), 0.85),
        ("My Social Insurance Number is 123-456-789.", 1, (27, 38), 0.85),
        ("Social Insurance Number: 123 456 789", 1, (25, 36), 0.85),
        ("This text contains an invalid SIN: 12345678.", 0, (), 0),
        ("Another invalid SIN 123-45-6789.", 0, (), 0),
        ("Here is a valid SIN: 123456789.", 1, (20, 29), 0.85),
    ],
)
def test_when_sin_in_text_then_all_sins_found(
    text, expected_len, expected_position, expected_score, recognizer, entities
):
    analyzer = AnalyzerEngine()
    analyzer.registry.add_recognizer(recognizer)
    results = analyzer.analyze(text, entities)

    assert len(results) == expected_len
    if results:
        result = results[0]
        assert result.entity_type == entities[0]
        assert result.start == expected_position[0]
        assert result.end == expected_position[1]
        assert result.score == expected_score


def test_metadata_list_lengths():
    """
    Tests for static counts of each metadata lists defined in CanadaSocialInsuranceNumberRecognizer
    :return: True/False
    """
    assert len(CanadaSINRecognizer.CONTEXT) == 15  # Example length

