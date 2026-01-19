import pandas as pd
import os
import time
import json
import pickle
import logging
import re
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor
from ahocorasick import Automaton
from .config import cache_enabled

logger = logging.getLogger("presidio-analyzer.ICD10Recognizer")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

base_path = '/usr/share/data'
json_file = os.path.join(base_path, 'ValidICD10-Jan2024.json')
preprocessed_patterns_file = os.path.join(base_path, 'icd10_patterns.pkl')

_cached_patterns = None
_automaton = None  # Global cache for Aho-Corasick automaton

def load_preprocessed_patterns() -> Tuple[List[Pattern], Automaton]:
    """Load preprocessed patterns and automaton."""
    global _cached_patterns, _automaton
    if cache_enabled and _cached_patterns and _automaton:
        return _cached_patterns, _automaton

    start = time.time()
    try:
        with open(preprocessed_patterns_file, 'rb') as f:
            data = pickle.load(f)

        logger.info(f"Loaded preprocessed data in {time.time() - start:.4f}s")
        if cache_enabled:
            _cached_patterns = data['patterns']
            _automaton = data['automaton']
        return data['patterns'], data['automaton']
    except Exception as e:
        logger.error(f"Pattern load failed: {e}")
        return [], Automaton()

def preprocess_and_save_patterns():
    """Optimized preprocessing with parallelization."""
    try:
        start = time.time()
        with open(json_file, 'r') as f:
            data = json.load(f)
        icd10_df = pd.DataFrame(data).fillna("")

        # Create patterns in parallel
        with ThreadPoolExecutor() as executor:
            code_patterns = list(executor.map(
                lambda c: Pattern(f"ICD10-Code:{c}", rf"\b{re.escape(c)}\b", 1.0),
                icd10_df['CODE'].unique()
            ))

        # Build Aho-Corasick automaton
        auto = Automaton()
        for desc in icd10_df['SHORT DESCRIPTION (VALID ICD-10 FY2024)']:
            if desc: auto.add_word(desc.lower(), (desc, "SHORT_DESC"))
        for desc in icd10_df['LONG DESCRIPTION (VALID ICD-10 FY2024)']:
            if desc: auto.add_word(desc.lower(), (desc, "LONG_DESC"))
        auto.make_automaton()

        # Save optimized data
        with open(preprocessed_patterns_file, 'wb') as f:
            pickle.dump({
                'patterns': code_patterns,
                'automaton': auto
            }, f)

        logger.info(f"Preprocessed in {time.time() - start:.2f}s")
    except Exception as e:
        logger.error(f"Preprocessing failed: {e}")

class ICD10Recognizer(PatternRecognizer):
    """Optimized recognizer with hybrid pattern matching."""

    def __init__(self, supported_language: str = "en", supported_entity: str = "ICD10_CODE"):
        start_init = time.time()

        # Load patterns and automaton
        self.patterns, self.auto = load_preprocessed_patterns()
        if not self.patterns and os.path.exists(preprocessed_patterns_file):
            preprocess_and_save_patterns()
            self.patterns, self.auto = load_preprocessed_patterns()

        # Compile code patterns
        self.code_regexes = [re.compile(p.regex) for p in self.patterns]

        # Initialize parent with minimal patterns
        super().__init__(
            supported_entity=supported_entity,
            patterns=self.patterns[:1],  # Use first pattern for parent req
            context=["icd-10", "code", "diagnosis"],
            supported_language=supported_language
        )

        logger.info(f"Initialized in {time.time() - start_init:.4f}s | Codes: {len(self.patterns)}")

    def analyze(self, text, entities, nlp_artifacts=None):
        """Hybrid analysis with regex codes + Aho-Corasick descriptions."""
        start = time.time()
        results = []
        text_lower = text.lower()

        # Match ICD-10 codes
        for regex in self.code_regexes:
            for match in regex.finditer(text):
                results.append(RecognizerResult(
                    entity_type="ICD10_CODE",
                    start=match.start(),
                    end=match.end(),
                    score=1.0
                ))

        # Match descriptions
        seen = set()
        for end, (original, desc_type) in self.auto.iter(text_lower):
            start_idx = end - len(original) + 1
            if text[start_idx:end+1] == original and (start_idx, end+1) not in seen:
                results.append(RecognizerResult(
                    entity_type=f"ICD10_{desc_type}",
                    start=start_idx,
                    end=end+1,
                    score=0.85 if desc_type == "SHORT_DESC" else 0.7
                ))
                seen.add((start_idx, end+1))

        logger.info(f"Analyzed {len(text)} chars in {time.time() - start:.5f}s")
        return self._filter_overlaps(results)

    def _filter_overlaps(self, results: List[RecognizerResult]) -> List[RecognizerResult]:
        """Deduplicate overlapping matches."""
        results.sort(key=lambda x: (x.start, -x.score))
        filtered = []
        last_end = 0

        for res in results:
            if res.start >= last_end:
                filtered.append(res)
                last_end = res.end
        return filtered
