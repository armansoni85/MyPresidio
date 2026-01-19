from typing import Optional, List
from presidio_analyzer import Pattern, PatternRecognizer, RecognizerResult
import re
from datetime import datetime, date
import calendar


class DateOfBirthRecognizer(PatternRecognizer):
    """
    Fixed and enhanced Date of Birth recognizer for Presidio.

    Key fixes applied from the original version:
    - Compatible handling of supported_language argument (backwards compatible with older Presidio).
    - Month-name pattern uses word boundaries and is cached.
    - Regex patterns use non-capturing groups to avoid shifting match indexes.
    - Date-string cleaning normalizes separators and removes common prefixes like "DOB:", "born", etc.
    - Localized month names are mapped to English month names before parsing.
    - Validates that years are in a sensible range (1900..current year).
    - Provides a configurable fallback when context words are missing but confidence is high.
    - Safer confidence bounding.
    """

    MONTHS = {
        'english': {
            'full': ['january', 'february', 'march', 'april', 'may', 'june',
                     'july', 'august', 'september', 'october', 'november', 'december'],
            'abbr': ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                     'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
        },
        'french': {
            'full': ['janvier', 'février', 'mars', 'avril', 'mai', 'juin',
                     'juillet', 'août', 'septembre', 'octobre', 'novembre', 'décembre'],
            'abbr': ['janv', 'févr', 'mars', 'avr', 'mai', 'juin',
                     'juil', 'août', 'sept', 'oct', 'nov', 'déc']
        },
        'spanish': {
            'full': ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
                     'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'],
            'abbr': ['ene', 'feb', 'mar', 'abr', 'may', 'jun',
                     'jul', 'ago', 'sep', 'oct', 'nov', 'dic']
        },
        'german': {
            'full': ['januar', 'februar', 'märz', 'april', 'mai', 'juni',
                     'juli', 'august', 'september', 'oktober', 'november', 'dezember'],
            'abbr': ['jan', 'feb', 'mär', 'apr', 'mai', 'jun',
                     'jul', 'aug', 'sep', 'okt', 'nov', 'dez']
        },
        'italian': {
            'full': ['gennaio', 'febbraio', 'marzo', 'aprile', 'maggio', 'giugno',
                     'luglio', 'agosto', 'settembre', 'ottobre', 'novembre', 'dicembre'],
            'abbr': ['gen', 'feb', 'mar', 'apr', 'mag', 'giu',
                     'lug', 'ago', 'set', 'ott', 'nov', 'dic']
        }
    }

    # Cache for compiled month pattern
    _MONTH_PATTERN: Optional[str] = None

    # Default: require context unless the detected match already has high confidence
    CONTEXT_REQUIRED_FALLBACK_CONFIDENCE = 0.85

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "DATE_OF_BIRTH",
    ):
        # compute patterns once
        patterns = patterns if patterns else self._build_patterns()
        context = context if context else self.CONTEXT

        # Backwards-compatible handling of supported_language
        kwargs = {}
        try:
            # If PatternRecognizer __init__ accepts supported_language, pass it
            if 'supported_language' in PatternRecognizer.__init__.__code__.co_varnames:
                kwargs['supported_language'] = supported_language
        except Exception:
            # If introspection fails, skip passing it (older Presidio)
            pass

        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            **kwargs,
        )

        # Precompute month pattern for reuse
        if not self._MONTH_PATTERN:
            self._MONTH_PATTERN = self._build_month_pattern()

    def _build_month_pattern(self) -> str:
        """Build a regex pattern for month names (word-boundary protected)."""
        all_months = set()
        for lang in self.MONTHS.values():
            all_months.update([m.lower() for m in lang['full']])
            all_months.update([m.lower() for m in lang['abbr']])

        # sort longest first to protect against prefix collisions
        sorted_months = sorted(all_months, key=len, reverse=True)
        # Join and wrap with word boundaries
        joined = '|'.join(re.escape(m) for m in sorted_months)
        return r'\b(?:' + joined + r')\b'

    def _build_patterns(self) -> List[Pattern]:
        """Return a list of Pattern objects (non-capturing groups used)."""
        month_pattern = self._MONTH_PATTERN or self._build_month_pattern()

        patterns = [
            Pattern(
                "DOB Numeric DD/MM/YYYY",
                rf"(?:^|\s|[:])(?:0?[1-9]|[12][0-9]|3[01])[-./\\_~•](?:0?[1-9]|1[0-2])[-./\\_~•](?:19|20)\d{{2}}(?:\s|$|[,.;])",
                0.85,
            ),
            Pattern(
                "DOB Numeric MM/DD/YYYY",
                rf"(?:^|\s|[:])(?:0?[1-9]|1[0-2])[-./\\_~•](?:0?[1-9]|[12][0-9]|3[01])[-./\\_~•](?:19|20)\d{{2}}(?:\s|$|[,.;])",
                0.85,
            ),
            Pattern(
                "DOB Numeric YYYY/MM/DD",
                rf"(?:^|\s|[:])(?:19|20)\d{{2}}[-./\\_~•](?:0?[1-9]|1[0-2])[-./\\_~•](?:0?[1-9]|[12][0-9]|3[01])(?:\s|$|[,.;])",
                0.85,
            ),

            Pattern(
                "DOB Numeric DD/MM/YY",
                rf"(?:^|\s|[:])(?:0?[1-9]|[12][0-9]|3[01])[-./\\_~•](?:0?[1-9]|1[0-2])[-./\\_~•]\d{{2}}(?:\s|$|[,.;])",
                0.6,
            ),
            Pattern(
                "DOB Numeric MM/DD/YY",
                rf"(?:^|\s|[:])(?:0?[1-9]|1[0-2])[-./\\_~•](?:0?[1-9]|[12][0-9]|3[01])[-./\\_~•]\d{{2}}(?:\s|$|[,.;])",
                0.6,
            ),

            Pattern(
                "DOB Written International",
                rf"(?:^|\s|[:])(?:0?[1-9]|[12][0-9]|3[01])(?:st|nd|rd|th)?\s+{month_pattern}\s+(?:19|20)?\d{{2,4}}(?:\s|$|[,.;])",
                0.8,
            ),

            Pattern(
                "DOB Written US",
                rf"(?:^|\s|[:]){month_pattern}\s+(?:0?[1-9]|[12][0-9]|3[01])(?:st|nd|rd|th)?,?\s+(?:19|20)?\d{{2,4}}(?:\s|$|[,.;])",
                0.8,
            ),

            Pattern(
                "DOB Year First Written",
                rf"(?:^|\s|[:])(?:19|20)?\d{{2,4}}\s+{month_pattern}\s+(?:0?[1-9]|[12][0-9]|3[01])(?:st|nd|rd|th)?(?:\s|$|[,.;])",
                0.7,
            ),

            Pattern(
                "DOB Hybrid",
                rf"(?:^|\s|[:])(?:0?[1-9]|[12][0-9]|3[01])[-.\s](?:{month_pattern})[-.\s](?:19|20)?\d{{2,4}}(?:\s|$|[,.;])",
                0.75,
            ),

            Pattern(
                "DOB Hybrid US",
                rf"(?:^|\s|[:])(?:{month_pattern})[-.\s](?:0?[1-9]|[12][0-9]|3[01])[-.\s](?:19|20)?\d{{2,4}}(?:\s|$|[,.;])",
                0.75,
            ),

            Pattern(
                "DOB Continuous 8-digit",
                rf"(?:^|\s|[:])(?:19|20)\d{{2}}(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01])(?:\s|$|[,.;])",
                0.7,
            ),
            Pattern(
                "DOB Continuous 6-digit",
                rf"(?:^|\s|[:])\d{{2}}(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01])(?:\s|$|[,.;])",
                0.5,
            ),

            Pattern(
                "DOB ISO8601",
                rf"(?:^|\s|[:])(?:19|20)\d{{2}}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])T?\d{{2}}:\d{{2}}:\d{{2}}Z?(?:\s|$|[,.;])",
                0.9,
            ),

            Pattern(
                "DOB Natural Language",
                r"(?:^|\s)(?:born|birth|dob|d\.o\.b|date of birth)[:\-\s]*([^\.\n]{5,50}?\d{2,4})(?:\s|$|[,.;])",
                0.6,
            ),
        ]

        return patterns

    CONTEXT = [
        "date of birth", "dob", "d.o.b", "birth date", "birthday",
        "date of service", "dos", "d.o.s", "service date",
        "born", "birth", "geburt", "naissance", "nascimento", "nacimiento",
        "geboren", "né", "nato", "nacido", "born on", "patient dob",
        "client dob", "member dob", "insured dob", "my dob", "my date of birth",
        "i was born", "birth certificate", "dob:", "date of birth:", "birth:", "born:", "d.o.b.:"
    ]

    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        results = super().analyze(text, entities, nlp_artifacts)
        filtered_results: List[RecognizerResult] = []

        for result in results:
            matched_text = text[result.start:result.end].strip()
            # quick sanity: if parse fails skip
            if not self._is_valid_date(matched_text):
                continue

            # if context present accept; otherwise allow when pattern confidence is high
            has_ctx = self._has_context(text, result.start, result.end)
            if has_ctx or result.score >= self.CONTEXT_REQUIRED_FALLBACK_CONFIDENCE:
                result.score = self._adjust_confidence_based_on_pattern(text, result)
                # enforce bounds
                result.score = min(max(result.score, 0.0), 1.0)
                filtered_results.append(result)

        return filtered_results

    def _is_valid_date(self, date_str: str) -> bool:
        try:
            parsed = self._parse_date(date_str)
            if not parsed:
                return False

            # must be date (not datetime) and not in future
            if parsed > datetime.now().date():
                return False

            # sensible year range
            current_year = datetime.now().year
            if parsed.year < 1900 or parsed.year > current_year:
                return False

            return True
        except Exception:
            return False

    def _normalize_date_string(self, date_str: str) -> str:
        """Normalize separators, remove common prefixes and map localized months to English."""
        s = date_str.strip()
        # Remove common labels/prefixes
        s = re.sub(r'(?i)\b(?:dob|d\.o\.b|date of birth|birth date|born)\b[:\-\s]*', '', s)

        # Normalize common separators to spaces or slashes for strptime patterns
        s = s.replace('•', '/').replace('_', '/').replace('~', '/').replace('\u00A0', ' ')

        # Remove stray characters except allowed ones (letters, digits, space, /-.,,)
        s = re.sub(r"[^\w\s/\-\.,]", '', s)

        # Map localized month names to English to help datetime.strptime
        s = self._map_localized_months_to_english(s)
        return s

    def _map_localized_months_to_english(self, s: str) -> str:
        lower = s.lower()
        # Build mapping once
        mapping = {}
        for lang_vals in self.MONTHS.values():
            for idx, mon in enumerate(lang_vals['full'], start=1):
                mapping[mon.lower()] = calendar.month_name[idx]
            for idx, mon in enumerate(lang_vals['abbr'], start=1):
                mapping[mon.lower()] = calendar.month_abbr[idx]

        # Replace longest month names first to avoid partial replacement
        for key in sorted(mapping.keys(), key=len, reverse=True):
            pattern = re.compile(re.escape(key), re.IGNORECASE)
            s = pattern.sub(mapping[key], s)

        return s

    def _parse_date(self, date_str: str) -> Optional[date]:
        clean_date = self._normalize_date_string(date_str)

        # Prepare formats to try
        formats = [
            '%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y', '%d %m %Y',
            '%m/%d/%Y', '%m-%d-%Y', '%m.%d.%Y', '%m %d %Y',
            '%Y/%m/%d', '%Y-%m-%d', '%Y.%m.%d', '%Y %m %d',
            '%d/%m/%y', '%d-%m-%y', '%d.%m.%y',
            '%m/%d/%y', '%m-%d-%y', '%m.%d.%y',
            '%Y%m%d', '%d%m%Y', '%m%d%Y',
        ]

        # Try each format
        for fmt in formats:
            try:
                return datetime.strptime(clean_date, fmt).date()
            except ValueError:
                continue

        # Try written month formats (with english month names after mapping)
        written_formats = [
            '%d %B %Y', '%d %b %Y', '%B %d %Y', '%b %d %Y',
            '%d %B %y', '%d %b %y', '%B %d %y', '%b %d %y',
            '%Y %B %d', '%Y %b %d',
        ]
        for fmt in written_formats:
            try:
                return datetime.strptime(clean_date, fmt).date()
            except ValueError:
                continue

        # Try ordinal day like 1st July 1990
        ord_match = re.search(r'(\d+)(?:st|nd|rd|th)\s+(\w+)\s+(\d{4})', clean_date, re.IGNORECASE)
        if ord_match:
            day, mon, yr = ord_match.groups()
            try:
                return datetime.strptime(f"{day} {mon} {yr}", '%d %B %Y').date()
            except Exception:
                pass

        # US written with comma e.g. July 1, 1990
        us_match = re.search(r'(\w+)\s+(\d+),\s*(\d{4})', clean_date, re.IGNORECASE)
        if us_match:
            mon, day, yr = us_match.groups()
            try:
                return datetime.strptime(f"{mon} {day} {yr}", '%B %d %Y').date()
            except Exception:
                pass

        return None

    def _has_context(self, text: str, start: int, end: int) -> bool:
        window_size = 150
        context_start = max(0, start - window_size)
        context_end = min(len(text), end + window_size)
        full_context = text[context_start:context_end]

        for context_word in self.CONTEXT:
            pattern = rf"\b{re.escape(context_word)}\b"
            if re.search(pattern, full_context, re.IGNORECASE):
                return True
        return False

    def _adjust_confidence_based_on_pattern(self, text: str, result: RecognizerResult) -> float:
        original_confidence = result.score
        matched_text = text[result.start:result.end].lower()

        # boost for 4-digit year
        if re.search(r'(?:19|20)\d{2}', matched_text):
            original_confidence = min(original_confidence + 0.05, 0.95)

        # reduce for ambiguous 2-digit years
        if re.search(r'\d{1,2}[-./]\d{1,2}[-./]\d{2}(?:\s|$)', matched_text) and not re.search(r'(?:19|20)\d{2}', matched_text):
            original_confidence = max(original_confidence - 0.1, 0.3)

        # boost for written months
        flattened_months = [m for lang in self.MONTHS.values() for m in (lang['full'] + lang['abbr'])]
        if any(m.lower() in matched_text for m in flattened_months):
            original_confidence = min(original_confidence + 0.1, 0.95)

        # boost for ISO
        if re.search(r'\d{4}-\d{2}-\d{2}', matched_text):
            original_confidence = min(original_confidence + 0.1, 0.95)

        return min(max(original_confidence, 0.0), 1.0)

