import json
import os
from presidio_analyzer import Pattern, PatternRecognizer

base_path = '/usr/share/data'

def load_data_lazy(filename, loader_function):
    """Load data lazily, only when needed."""
    if not hasattr(load_data_lazy, "cache"):
        load_data_lazy.cache = {}
    if filename not in load_data_lazy.cache:
        load_data_lazy.cache[filename] = loader_function()
    return load_data_lazy.cache[filename]

# Data loader function for ingredients
def load_ingredients():
    with open(os.path.join(base_path, 'Ingredients.json')) as f:
        return set(json.load(f))

# Lazy-load ingredient data
ingredients = load_data_lazy('Ingredients.json', load_ingredients)

class IngredientRecognizer(PatternRecognizer):
    """Recognize ingredients using patterns."""

    def __init__(
        self,
        supported_language: str = "en",
        supported_entity: str = "HIPAA_HITECH_INGREDIENT",
    ):
        patterns = [Pattern(f"Ingredient: {ingredient}", rf"\b{ingredient}\b", 0.9) for ingredient in ingredients]
        context = [
            "ingredient", "component", "substance", "compound"
        ]
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )
