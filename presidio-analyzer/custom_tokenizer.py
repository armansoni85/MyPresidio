# custom_tokenizer.py
import spacy
from spacy.tokens import Token

class CustomTokenizer:
    def __init__(self, nlp):
        pass

    def __call__(self, doc):
        # Merge possessive forms (e.g., "Alice's" -> "Alice's")
        for i, token in enumerate(doc[:-1]):
            if token.text.endswith("'") and doc[i+1].text in {"s", "re", "ve", "d", "ll", "m"}:
                doc.merge(i, i+2)
        return doc
