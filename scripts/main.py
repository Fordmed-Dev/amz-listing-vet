import argparse
import re
import sqlite3
from dataclasses import dataclass
from typing import Any, Optional, List, Tuple

import pandas as pd


# =============================================================================
# BASIC HELPERS
# =============================================================================

def _s(x: Any) -> str:
    """Convert to string, handling None."""
    return "" if x is None or pd.isna(x) else str(x)


def lc(x: Any) -> str:
    """Lowercase and strip."""
    return _s(x).strip().lower()


def to_num(series: pd.Series) -> pd.Series:
    """Convert series to numeric, coercing errors to NaN."""
    return pd.to_numeric(series, errors="coerce")


def to_percent(series: pd.Series) -> pd.Series:
    """Convert percentage strings to decimal (15% -> 0.15)."""
    s = series.astype("string").str.strip().str.replace("%", "", regex=False)
    v = pd.to_numeric(s, errors="coerce")
    return v.where(v <= 1, v / 100.0)


def connect_db(path: str) -> sqlite3.Connection:
    """Create optimized SQLite connection."""
    con = sqlite3.connect(path)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA temp_store=MEMORY;")
    con.execute("PRAGMA cache_size=-200000;")
    return con


# =============================================================================
# CODE NORMALIZATION
# =============================================================================

def norm_code(x: Any) -> Optional[str]:
    """
    Normalize product codes (MPN, UPC, EAN, etc.)
    Filters out junk values that cause candidate explosion.
    """
    s = _s(x).strip()
    if not s:
        return None

    # Remove non-alphanumeric, uppercase
    s = re.sub(r"[^A-Za-z0-9]", "", s).upper()
    if not s:
        return None

    # Filter garbage values
    if s in {"NA", "NAN", "NONE", "NULL", "UNKNOWN", "N/A", "TBD", "TBA"}:
        return None
    if set(s) <= {"0"}:  # All zeros
        return None
    if len(s) < 4:  # Too short to be useful
        return None
    if not any(ch.isdigit() for ch in s):  # Must have at least one digit
        return None

    return s


CODE_SPLIT_RE = re.compile(r"[,\s;/|]+")


def iter_norm_codes(value: Any) -> List[str]:
    """Split and normalize multiple codes from a single field."""
    s = _s(value).strip()
    if not s:
        return []
    out: List[str] = []
    for tok in CODE_SPLIT_RE.split(s):
        c = norm_code(tok)
        if c:
            out.append(c)
    return out


# =============================================================================
# TEXT NORMALIZATION
# =============================================================================

# Unicode translation table (using ordinals to avoid encoding issues)
_TRANS = str.maketrans({
    0x00D7: "x",   # × multiplication sign
    0x201C: '"',   # " left double quote
    0x201D: '"',   # " right double quote
    0x2033: '"',   # ″ double prime
    0x2032: "'",   # ′ single prime
})
_WS_RE = re.compile(r"\s+")


def norm_text(s: str) -> str:
    """Normalize text for searching/matching."""
    s = _s(s).translate(_TRANS).lower()
    s = re.sub(r"[-_]+", " ", s)
    s = re.sub(r"[^\w\s]", " ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


# =============================================================================
# TITLE CLEANING (for fuzzy matching)
# =============================================================================

STOPWORDS = {
    "a", "an", "the", "and", "or", "with", "without", "for", "of", "to", "from",
    "in", "on", "at", "by", "as", "is", "are", "was", "were", "be", "been", "being",
    "this", "that", "these", "those", "there", "where", "when", "what", "which",
    "who", "whom", "whose", "into", "onto", "via", "per", "its", "it", "our", "your",
    # --- Medical / product descriptor stopwords (added) ---
    # These are inconsistently used across catalog vs Amazon titles and
    # hurt fuzzy scores without adding meaningful signal.
    "sterile", "non-sterile", "nonsterile", "disposable", "reusable",
    "latex", "latexfree", "free", "individually", "wrapped", "sealed",
    "medical", "grade", "professional", "hospital",
}

# --- Abbreviation normalization map (added) ---
# Applied before stopword filtering so tokens like "bx" are expanded
# to "box" and can then match Amazon titles that spell them out.
ABBREV_MAP = {
    "bx":  "box",
    "cs":  "case",
    "ea":  "each",
    "pk":  "pack",
    "ct":  "count",
    "ctn": "carton",
    "bg":  "bag",
    "rl":  "roll",
    "pr":  "pair",
    "dz":  "dozen",
    "sp":  "spool",
}

UNIT_TOKENS = {
    # length
    "in", "inch", "inches", "cm", "mm", "ft", "feet", "yd", "yds", "yard", "yards",
    "m", "meter", "meters",
    # medical
    "fr", "french", "ga", "gauge",
    # volume
    "ml", "milliliter", "milliliters", "l", "liter", "liters", "cc", "oz", "fl",
    # weight
    "lb", "lbs", "pound", "pounds", "ounce", "ounces", "g", "gram", "grams",
    "kg", "kilogram", "kilograms",
}

_TITLE_ALLOWED_RE = re.compile(r"[^a-z0-9\s.\-/x]+")
_NUM_OR_FRAC = r"(?:\d+(?:\.\d+)?|\d+\s*/\s*\d+|\d+\s*[-]\s*\d+\s*/\s*\d+)"
_INCH_QUOTE_STRIP_RE = re.compile(rf'(?ix)\b({_NUM_OR_FRAC})\s*"')


def clean_title(text: Any) -> Optional[str]:
    """
    Clean title for fuzzy matching:
    - Normalize common abbreviations (bx->box, cs->case, etc.)
    - Remove punctuation and stopwords (including medical descriptors)
    - Remove unit words but keep numbers
    - Normalize whitespace
    """
    raw = _s(text).strip()
    if not raw:
        return None

    s = raw.translate(_TRANS)
    s = re.sub(r"\([^)]*\)", " ", s)  # Remove parentheticals
    s = _INCH_QUOTE_STRIP_RE.sub(r"\1", s)  # 20" -> 20

    s = s.lower()
    s = s.replace("-", " ")
    s = s.replace(",", " ").replace(";", " ").replace(":", " ")
    s = s.replace("(", " ").replace(")", " ").replace("[", " ").replace("]", " ")
    s = s.replace("{", " ").replace("}", " ").replace("|", " ").replace("/", " / ")
    s = _TITLE_ALLOWED_RE.sub(" ", s)
    s = re.sub(r"\bx\b", " ", s)  # Remove lone x separators
    s = _WS_RE.sub(" ", s).strip()

    if not s:
        return None

    toks: List[str] = []
    for tok in s.split():
        # Expand abbreviations before any filtering (added)
        tok = ABBREV_MAP.get(tok, tok)
        if tok in STOPWORDS:
            continue
        if tok in UNIT_TOKENS:
            continue
        toks.append(tok)

    out = " ".join(toks).strip()
    if not out:
        return None

    # Capitalize words that aren't numbers
    return " ".join(
        w.capitalize() if not any(ch.isdigit() for ch in w) else w
        for w in out.split()
    )


# =============================================================================
# UNIT EXCEPTIONS CONFIG (added)
# Products where the innermost count represents sheets/strips/pads, not
# sellable eaches. Add new product types here without touching parsing logic.
# =============================================================================

UNIT_EXCEPTIONS: dict[str, list[str]] = {
    "wipes":    [r"\bwipes?\b"],
    "bandages": [r"\bbandages?\b", r"\bband\s*aids?\b", r"\badhesive\s+bandages?\b"],
}

# Pre-compile all exception patterns for performance
_UNIT_EXCEPTION_PATTERNS: list[re.Pattern] = [
    re.compile(pat, re.IGNORECASE)
    for keywords in UNIT_EXCEPTIONS.values()
    for pat in keywords
]


def _is_sheet_unit_product(title: str) -> bool:
    """
    Check if a product's innermost pack count represents sheets/strips/pads
    rather than sellable eaches (wipes, bandages, etc.).
    When True, hierarchical qty parsing ignores the innermost count.
    Add new product types to UNIT_EXCEPTIONS above.
    """
    return any(p.search(title) for p in _UNIT_EXCEPTION_PATTERNS)


# =============================================================================
# QUANTITY EXTRACTION (Primary focus for matching)
# =============================================================================

@dataclass
class QuantityResult:
    """Result of quantity extraction."""
    quantity: Optional[int]
    unit_type: Optional[str]  # pack, case, box, each, etc.
    confidence: str  # "high", "medium", "low", "none"
    source: str  # "regex", "ai", "field"


# Compound quantity pattern: "4 Boxes of 60", "4 Pack of 20", "10 Packs of 2"
# Captures total = X * Y
_COMPOUND_QTY_RE = re.compile(
    r"(?i)\b(\d+)\s+(?:packs?|boxes|cases?|bags?|cartons?)\s+of\s+(\d+)\b"
)

# Comprehensive quantity patterns - order matters (most specific first)
QTY_PATTERNS = [
    # Explicit "X of Y" patterns
    (re.compile(r"(?i)\bpack\s*of\s*(\d+)\b"), "pack"),
    (re.compile(r"(?i)\bcase\s*of\s*(\d+)\b"), "case"),
    (re.compile(r"(?i)\bbox\s*of\s*(\d+)\b"), "box"),
    (re.compile(r"(?i)\bbag\s*of\s*(\d+)\b"), "bag"),
    (re.compile(r"(?i)\bset\s*of\s*(\d+)\b"), "set"),
    (re.compile(r"(?i)\broll[s]?\s*of\s*(\d+)\b"), "roll"),
    (re.compile(r"(?i)\bcarton\s*of\s*(\d+)\b"), "carton"),

    # "X Per Case/Pack/Box" patterns: "2 Per Case", "50 Per Box"
    (re.compile(r"(?i)\b(\d+)\s+per\s+(?:case|cs)\b"), "case"),
    (re.compile(r"(?i)\b(\d+)\s+per\s+(?:box|bx)\b"), "box"),
    (re.compile(r"(?i)\b(\d+)\s+per\s+(?:pack|pk)\b"), "pack"),
    (re.compile(r"(?i)\b(\d+)\s+per\s+(?:bag|bg)\b"), "bag"),
    (re.compile(r"(?i)\b(\d+)\s+per\s+(?:carton|ctn)\b"), "carton"),
    
    # "X Sponges/Bag" - specific medical product pattern
    (re.compile(r"(?i)\b(\d+)\s+sponges\s*/\s*bag\b"), "bag"),
    
    # "X Bandages" at end of title
    (re.compile(r"(?i)\b(\d+)\s+bandages\b"), "bandage"),
    
    # "(X boxes)" - parenthetical with unit
    (re.compile(r"(?i)\((\d+)\s+boxes\)"), "box"),
    (re.compile(r"(?i)\((\d+)\s+bags\)"), "bag"),
    
    # "X Bottle / Case" or "X Bottle/Case"
    (re.compile(r"(?i)\b(\d+)\s+bottle\s*/\s*case\b"), "case"),
    
    # Compact patterns like "BGof10", "PRof2"
    (re.compile(r"(?i)\b[a-z]{2,}of(\d+)\b"), "compact_of"),
    
    # End-of-string patterns: "Each, PRof2"
    (re.compile(r"(?i),\s*[a-z]{2,}of(\d+)$"), "compact_of_end"),

    # Comma patterns: "50,box", "24,case" (common in structured data)
    (re.compile(r"(?i),(\d+),(?:box|bx)\b"), "box"),
    (re.compile(r"(?i),(\d+),(?:case|cs)\b"), "case"),
    (re.compile(r"(?i),(\d+),(?:pack|pk)\b"), "pack"),
    (re.compile(r"(?i),(\d+),(?:bag|bg)\b"), "bag"),
    (re.compile(r"(?i)\b(\d+),(?:box|bx)\b"), "box"),
    (re.compile(r"(?i)\b(\d+),(?:case|cs)\b"), "case"),
    (re.compile(r"(?i)\b(\d+),(?:pack|pk)\b"), "pack"),

    # Slash patterns: 100/box, 50/cs, 24/pk
    (re.compile(r"(?i)\b(\d+)\s*/\s*(?:box|bx)\b"), "box"),
    (re.compile(r"(?i)\b(\d+)\s*/\s*(?:case|cs)\b"), "case"),
    (re.compile(r"(?i)\b(\d+)\s*/\s*(?:pack|pk)\b"), "pack"),
    (re.compile(r"(?i)\b(\d+)\s*/\s*(?:bag|bg)\b"), "bag"),
    (re.compile(r"(?i)\b(\d+)\s*/\s*(?:ea|each)\b"), "each"),
    (re.compile(r"(?i)\b(\d+)\s*/\s*(?:ct|count)\b"), "count"),
    (re.compile(r"(?i)\b(\d+)\s*/\s*(?:roll|rl)\b"), "roll"),
    (re.compile(r"(?i)\b(\d+)\s*/\s*(?:carton|ctn)\b"), "carton"),
    (re.compile(r"(?i)\b(\d+)\s*/\s*(?:spool|sp)\b"), "spool"),

    # Reverse slash: bx/50, cs/24, pk/12
    (re.compile(r"(?i)\b(?:box|bx)\s*/\s*(\d+)\b"), "box"),
    (re.compile(r"(?i)\b(?:case|cs)\s*/\s*(\d+)\b"), "case"),
    (re.compile(r"(?i)\b(?:pack|pk)\s*/\s*(\d+)\b"), "pack"),
    (re.compile(r"(?i)\b(?:bag|bg)\s*/\s*(\d+)\b"), "bag"),
    (re.compile(r"(?i)\b(?:ea|each)\s*/\s*(\d+)\b"), "each"),
    (re.compile(r"(?i)\b(?:carton|ctn)\s*/\s*(\d+)\b"), "carton"),
    (re.compile(r"(?i)\b(?:spool|sp)\s*/\s*(\d+)\b"), "spool"),

    # "X Strips/unit" or "X Tests" patterns (medical products)
    (re.compile(r"(?i)\b(\d+)\s+strips?\s*/\s*(?:cs|case|bx|box|pk|pack|ctn|carton)\b"), "strip"),
    (re.compile(r"(?i)\b(\d+)\s+tests?\b"), "test"),

    # Hyphenated: 12-pack, 24-count, 6-ct
    (re.compile(r"(?i)\b(\d+)\s*-\s*(?:pack|pk)\b"), "pack"),
    (re.compile(r"(?i)\b(\d+)\s*-\s*(?:case|cs)\b"), "case"),
    (re.compile(r"(?i)\b(\d+)\s*-\s*(?:box|bx)\b"), "box"),
    (re.compile(r"(?i)\b(\d+)\s*-\s*(?:count|ct|cnt)\b"), "count"),
    (re.compile(r"(?i)\b(\d+)\s*-\s*(?:piece|pc|pcs)\b"), "piece"),
    (re.compile(r"(?i)\b(\d+)\s*-\s*(?:roll|rl)\b"), "roll"),
    (re.compile(r"(?i)\b(\d+)\s*-\s*(?:bag|bg)\b"), "bag"),
    (re.compile(r"(?i)\b(\d+)\s*-\s*(?:carton|ctn)\b"), "carton"),
    (re.compile(r"(?i)\b(\d+)\s*-\s*(?:spool|sp)\b"), "spool"),

    # Suffixed: 12pk, 24ct, 6cs, 100ea
    (re.compile(r"(?i)\b(\d+)\s*(?:pk|pack)\b"), "pack"),
    (re.compile(r"(?i)\b(\d+)\s*(?:cs|case)\b"), "case"),
    (re.compile(r"(?i)\b(\d+)\s*(?:bx|box)\b"), "box"),
    (re.compile(r"(?i)\b(\d+)\s*(?:ct|cnt|count)\b"), "count"),
    (re.compile(r"(?i)\b(\d+)\s*(?:ea|each)\b"), "each"),
    (re.compile(r"(?i)\b(\d+)\s*(?:pc|pcs|piece|pieces)\b"), "piece"),
    (re.compile(r"(?i)\b(\d+)\s*(?:roll|rolls|rl)\b"), "roll"),
    (re.compile(r"(?i)\b(\d+)\s*(?:bag|bags|bg)\b"), "bag"),
    (re.compile(r"(?i)\b(\d+)\s*(?:pair|pairs|pr)\b"), "pair"),
    (re.compile(r"(?i)\b(\d+)\s*(?:dozen|dz)\b"), "dozen"),
    (re.compile(r"(?i)\b(\d+)\s*(?:carton|ctn)\b"), "carton"),
    (re.compile(r"(?i)\b(\d+)\s*(?:spool|sp)\b"), "spool"),

    # "Qty X" or "Quantity: X"
    (re.compile(r"(?i)\bqty\.?\s*[:=]?\s*(\d+)\b"), "qty"),
    (re.compile(r"(?i)\bquantity\.?\s*[:=]?\s*(\d+)\b"), "qty"),

    # Parenthetical: (12), (24 count)
    (re.compile(r"(?i)\((\d+)\s*(?:count|ct|pk|pack|cs|case|ea|each|pc|pcs)?\)"), "paren"),

    # "X units" patterns
    (re.compile(r"(?i)\b(\d+)\s+(?:units?)\b"), "unit"),
    (re.compile(r"(?i)\b(\d+)\s+(?:items?)\b"), "item"),
]

# Patterns that look like qty but aren't (to avoid false positives)
NOT_QTY_PATTERNS = [
    re.compile(r"(?i)\b\d+\s*(?:in|inch|inches|cm|mm|ft|feet|m|meter)\b"),  # Dimensions
    re.compile(r"(?i)\b\d+\s*(?:ml|l|oz|fl|cc|gal|gallon)\b"),  # Volume
    re.compile(r"(?i)\b\d+\s*(?:lb|lbs|g|gram|kg|mg|oz|ounce)\b"),  # Weight
    re.compile(r"(?i)\b\d+\s*(?:fr|french|ga|gauge)\b"),  # Medical sizes
    re.compile(r"(?i)\b\d+\s*x\s*\d+"),  # Dimensions like 4x6
    re.compile(r"(?i)\b\d+(?:\.\d+)?\s*%"),  # Percentages
]


def extract_quantity(text: Any) -> QuantityResult:
    """
    Extract quantity from text (title or field).
    Returns QuantityResult with quantity, unit_type, confidence, source.
    """
    t = _s(text).strip()
    if not t:
        return QuantityResult(None, None, "none", "regex")

    # Check compound patterns first: "4 Boxes of 60" -> 240, "4 Pack of 20" -> 80
    cm = _COMPOUND_QTY_RE.search(t)
    if cm:
        try:
            outer = int(cm.group(1))
            inner = int(cm.group(2))
            total = outer * inner
            if 2 <= total <= 100000:
                return QuantityResult(total, "compound", "high", "regex")
        except (ValueError, IndexError):
            pass

    # Try each pattern
    for pattern, unit_type in QTY_PATTERNS:
        m = pattern.search(t)
        if m:
            try:
                qty = int(m.group(1))
                if 1 <= qty <= 100000:  # Sanity check
                    # Verify it's not actually a dimension
                    match_text = m.group(0)
                    is_false_positive = any(
                        fp.search(match_text) for fp in NOT_QTY_PATTERNS
                    )
                    if not is_false_positive:
                        return QuantityResult(qty, unit_type, "high", "regex")
            except (ValueError, IndexError):
                continue

    # Check for standalone numbers at end that might be qty (lower confidence)
    # e.g., "Product Name 12" where 12 is the pack size
    end_num_match = re.search(r"\b(\d+)\s*$", t)
    if end_num_match:
        qty = int(end_num_match.group(1))
        if 2 <= qty <= 1000:  # More restrictive for low-confidence
            return QuantityResult(qty, None, "low", "regex")

    return QuantityResult(None, None, "none", "regex")


# =============================================================================
# HIERARCHICAL QUANTITY PARSING (e.g., "2/pk, 6pk/cs" -> 12 total eaches)
# =============================================================================

_HIER_UNIT = r'(?:pk|pack|bx|box|bg|bag|bt|bottle|ea|each|rl|roll|sp|spool|ctn|carton|cs|case)'

# Two-level: "2/pk, 6pk/cs"
_HIER_QTY_2L_RE = re.compile(
    rf'(?i)(\d+)\s*/\s*{_HIER_UNIT}\s*[,;]\s*(\d+)\s*{_HIER_UNIT}\s*/\s*{_HIER_UNIT}'
)

# Three-level: "4/pk, 10 pk/bg, 1 bg/cs"
_HIER_QTY_3L_RE = re.compile(
    rf'(?i)(\d+)\s*/\s*{_HIER_UNIT}\s*[,;]\s*(\d+)\s*{_HIER_UNIT}\s*/\s*{_HIER_UNIT}\s*[,;]\s*(\d+)\s*{_HIER_UNIT}\s*/\s*{_HIER_UNIT}'
)


def parse_hierarchical_qty(title: Any) -> Optional[int]:
    """
    Parse hierarchical quantity from title.

    Supports:
      2-level: "2/pk, 6pk/cs" -> 2 * 6 = 12
      3-level: "4/pk, 10 pk/bg, 1 bg/cs" -> 4 * 10 * 1 = 40

    For sheet-unit products (wipes, bandages):
      The innermost count = individual sheets/strips, not sellable eaches.
      "100/pk, 5pk/cs" -> 5  (only outer count matters)

    Add new product types to UNIT_EXCEPTIONS at the top of this file.
    """
    t = _s(title).strip()
    if not t:
        return None

    # Check if innermost count should be ignored (wipes, bandages, etc.)
    is_sheet_unit = _is_sheet_unit_product(t)

    # Try 3-level first (most specific)
    m = _HIER_QTY_3L_RE.search(t)
    if m:
        level1 = int(m.group(1))  # eaches per inner unit
        level2 = int(m.group(2))  # inner units per mid unit
        level3 = int(m.group(3))  # mid units per outer unit
        if is_sheet_unit:
            return level2 * level3
        else:
            return level1 * level2 * level3

    # Try 2-level
    m = _HIER_QTY_2L_RE.search(t)
    if m:
        inner = int(m.group(1))  # eaches per inner unit
        outer = int(m.group(2))  # inner units per outer unit
        if is_sheet_unit:
            return outer
        else:
            return inner * outer

    return None


def looks_like_qty_field(text: Any) -> bool:
    """Check if a field value looks like a quantity (not a size)."""
    t = _s(text).strip().lower()
    if not t:
        return False

    # If it starts with a number and has qty-like suffix
    if re.match(r"^\d+\s*(?:count|ct|cnt|pk|pack|cs|case|ea|each|pc|pcs|box|bx|pair|pr)\b", t):
        return True

    return False


# =============================================================================
# COLOR EXTRACTION
# =============================================================================

COLOR_ALIASES = {
    "grey": "gray",
    "transparent": "clear",
    "off white": "white",
    "off-white": "white",
    "offwhite": "white",
    "multicolor": "multi",
    "multi color": "multi",
    "multi-color": "multi",
    "multicolored": "multi",
    "assorted colors": "assorted",
    "various": "assorted",
}

COLOR_LEXICON = [
    # Neutrals
    "black", "white", "gray", "silver", "charcoal",
    # Blues
    "navy", "blue", "light blue", "dark blue", "royal blue", "sky blue",
    "teal", "turquoise", "aqua", "cyan",
    # Greens
    "green", "lime", "olive", "sage", "mint", "forest green", "hunter green",
    # Reds
    "red", "maroon", "burgundy", "crimson", "scarlet", "wine",
    # Pinks
    "pink", "hot pink", "magenta", "rose", "blush", "coral",
    # Purples
    "purple", "violet", "lavender", "plum", "mauve",
    # Oranges/Yellows
    "orange", "yellow", "gold", "amber", "peach",
    # Browns
    "beige", "tan", "khaki", "brown", "chocolate", "mocha", "coffee",
    "copper", "bronze", "caramel",
    # Creams
    "cream", "ivory", "bone", "eggshell",
    # Transparent
    "clear", "transparent", "translucent",
    # Multi
    "multi", "assorted", "rainbow",
    # Natural/Other
    "natural", "nude", "sand", "almond", "smoke", "slate",
]

_COLOR_LABEL_RE = re.compile(r"(?i)\b(?:colou?r)\s*[:\-=]\s*([a-z][a-z ]{1,24})\b")


def _canonical_color(s: str) -> str:
    """Normalize color name."""
    s = s.strip().lower()
    s = _WS_RE.sub(" ", s)
    return COLOR_ALIASES.get(s, s)


def extract_color(text: Any) -> Optional[str]:
    """Extract color from title or field."""
    raw = _s(text).strip()
    if not raw:
        return None

    # Check for explicit "Color: X" pattern
    m = _COLOR_LABEL_RE.search(raw)
    if m:
        c = _canonical_color(norm_text(m.group(1)))
        if c:
            return c

    # Search for known colors
    t = norm_text(raw)
    for color in sorted(COLOR_LEXICON, key=lambda x: len(x.split()), reverse=True):
        canon = _canonical_color(color)
        if re.search(rf"(?:^| )({re.escape(canon)})(?:$| )", f" {t} "):
            return canon

    # Check last comma segment (often contains color)
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if parts:
        seg = norm_text(parts[-1])
        words = [w for w in seg.split() if w.isalpha()]
        if 1 <= len(words) <= 2:
            cand = _canonical_color(" ".join(words))
            if cand in {_canonical_color(x) for x in COLOR_LEXICON}:
                return cand

    return None


def norm_color(x: Any) -> Optional[str]:
    """Normalize a color field value."""
    s = _s(x).strip()
    if not s:
        return None
    return _canonical_color(norm_text(s))


# =============================================================================
# SIZE EXTRACTION
# =============================================================================

_UNIT = r'(?:cm|mm|in|inch|inches|ft|feet|yd|yard|yards|m|meter|meters|")'
_KEEP_SIZE_CHARS_RE = re.compile(r'[^a-z0-9\s.\-/\"x]+')

SIZE_WORDS = {
    "XXXS": ["xxxs", "3xs", "xxx-small", "xxx small"],
    "XXS": ["xxs", "2xs", "xx-small", "xx small"],
    "XS": ["xs", "x-small", "x small", "extra small"],
    "S": ["s", "sm", "small"],
    "M": ["m", "md", "med", "medium"],
    "L": ["l", "lg", "large"],
    "XL": ["xl", "x-large", "x large", "extra large"],
    "XXL": ["xxl", "2xl", "xx-large", "xx large"],
    "XXXL": ["xxxl", "3xl", "xxx-large", "xxx large"],
}

DIM_RE = re.compile(
    rf"""(?ix)\b
    (?P<a>{_NUM_OR_FRAC})\s*(?P<ua>{_UNIT})?\s*(?:w|d|h|l|dia|od|id)?\s*
    x\s*
    (?P<b>{_NUM_OR_FRAC})\s*(?P<ub>{_UNIT})?\s*(?:w|d|h|l|dia|od|id)?\s*
    (?:x\s*(?P<c>{_NUM_OR_FRAC})\s*(?P<uc>{_UNIT})?\s*(?:w|d|h|l|dia|od|id)?)?
    (?:\s*(?P<utail>cm|mm|in|inch|inches|ft|feet|yd|yard|yards|m|meter|meters|"))?
    \b""",
    re.VERBOSE,
)

MEAS_RE = re.compile(
    rf"(?ix)\b(?P<n>{_NUM_OR_FRAC})\s*(?P<u>cm|mm|in|inch|inches|ft|feet|yd|yard|yards|m|meter|meters)\b"
)

INCH_QUOTE_RE = re.compile(rf'(?ix)\b(?P<n>{_NUM_OR_FRAC})\s*"')

# Matches labeled dimension pairs like "3" Width, 8" Length" or "3in Width 8in Length".
# Handles comma-separated or space-separated, any unit on each dimension.
_LABELED_DIM_RE = re.compile(
    rf"""(?ix)
    \b(?P<a>{_NUM_OR_FRAC})\s*(?P<ua>(?:cm|mm|in|inch|inches|ft|feet|yd|yard|yards|m|meter|meters|"))?
    \s*\b(?:width|wide)\b
    [\s,;]+
    (?P<b>{_NUM_OR_FRAC})\s*(?P<ub>(?:cm|mm|in|inch|inches|ft|feet|yd|yard|yards|m|meter|meters|"))?
    \s*\b(?:length|long)\b
    """,
    re.VERBOSE,
)


def _expand_labeled_dims(m: re.Match) -> str:
    a  = m.group("a")
    ua = (m.group("ua") or "").strip()
    b  = m.group("b")
    # If second dim has no unit, inherit from first (e.g. both are inches)
    ub = (m.group("ub") or m.group("ua") or "").strip()
    return f"{a}{ua} x {b}{ub}"


def _canon_unit(u: Optional[str]) -> Optional[str]:
    """Canonicalize unit string."""
    if not u:
        return None
    u = u.strip().lower()
    if u == '"':
        return "IN"
    if u in {"in", "inch", "inches"}:
        return "IN"
    if u == "cm":
        return "CM"
    if u == "mm":
        return "MM"
    if u in {"ft", "feet"}:
        return "FT"
    if u in {"yd", "yard", "yards"}:
        return "YD"
    if u in {"m", "meter", "meters"}:
        return "M"
    return u.upper()


def _parse_num_token(tok: str) -> Optional[float]:
    """Parse numeric token including fractions."""
    t = tok.strip()
    if not t:
        return None

    # Mixed fraction: 4-1/2
    m = re.match(r"^\s*(\d+)\s*[-]\s*(\d+)\s*/\s*(\d+)\s*$", t)
    if m:
        whole = float(m.group(1))
        num = float(m.group(2))
        den = float(m.group(3))
        if den == 0:
            return None
        return whole + (num / den)

    # Simple fraction: 1/2
    m = re.match(r"^\s*(\d+)\s*/\s*(\d+)\s*$", t)
    if m:
        num = float(m.group(1))
        den = float(m.group(2))
        if den == 0:
            return None
        return num / den

    try:
        return float(t)
    except ValueError:
        return None


def _fmt_num(v: float) -> str:
    """Format number for display."""
    if float(v).is_integer():
        return str(int(v))
    return f"{v:.4f}".rstrip("0").rstrip(".")


def extract_size(title: Any) -> Optional[str]:
    """Extract primary size from title."""
    raw = _s(title).strip()
    if not raw:
        return None

    t = raw.translate(_TRANS).lower()
    t = re.sub(r"(?<!\d)-(?!\d)", " ", t)
    t = re.sub(r"[,;:()\[\]{}|]+", " ", t)
    t = _KEEP_SIZE_CHARS_RE.sub(" ", t)
    t = _WS_RE.sub(" ", t).strip()

    # Convert space-separated mixed fractions to decimal before DIM_RE runs.
    # e.g. "2x3 3/4" → "2x3.75",  "4 1/2 x 3" → "4.5 x 3"
    def _resolve_frac(m: re.Match) -> str:
        whole, num, den = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return _fmt_num(whole + num / den) if den else m.group(0)

    # (?<!\d) prevents matching mid-number; requires a "/" after space to avoid
    # false positives like "pack of 3 4 items".
    t = re.sub(r'(?<!\d)(\d+)\s+(\d+)\s*/\s*(\d+)', _resolve_frac, t)

    # Convert labeled dimensions ("3" Width, 8" Length") to "3" x 8"" so DIM_RE can parse them.
    t = _LABELED_DIM_RE.sub(_expand_labeled_dims, t)

    # Check for dimension patterns first (most reliable)
    for m in DIM_RE.finditer(t):
        a = m.group("a").replace(" ", "")
        b = m.group("b").replace(" ", "")
        c_raw = m.group("c")
        c = c_raw.replace(" ", "") if c_raw else None

        u = (
            _canon_unit(m.group("ua"))
            or _canon_unit(m.group("ub"))
            or _canon_unit(m.group("uc"))
            or _canon_unit(m.group("utail"))
        )

        base = f"{a} x {b}" + (f" x {c}" if c else "")
        return f"{base} {u}".strip().upper() if u else base.upper()

    # Check for single measurements
    for m in INCH_QUOTE_RE.finditer(t):
        v = _parse_num_token(m.group("n"))
        if v is not None:
            return f"{_fmt_num(v)} IN"

    for m in MEAS_RE.finditer(t):
        v = _parse_num_token(m.group("n"))
        if v is not None:
            u = _canon_unit(m.group("u"))
            return f"{_fmt_num(v)} {u}"

    # Check for size words (S/M/L/XL)
    # Flatten ALL variants across ALL canonicals sorted by length desc so that
    # longer patterns (e.g. "x large" → XL) are checked before shorter substrings
    # (e.g. "large" → L) that would otherwise match first due to dict order.
    padded = f" {t} "
    all_size_variants = sorted(
        ((vv, canon) for canon, variants in SIZE_WORDS.items() for vv in variants),
        key=lambda x: len(x[0]),
        reverse=True,
    )
    for vv, canon in all_size_variants:
        if re.search(r"(?:^| )" + re.escape(vv) + r"(?:$| )", padded):
            return canon

    return None


def norm_size(x: Any) -> Optional[str]:
    """Normalize a size field value for comparison.

    Strips dimension unit words (INCH/INCHES/IN) that follow numbers, since in
    medical/bandage products these units are implicit and create false mismatches
    between "4 X 4" and "4 INCH X 4 INCH" or "4 X 4 IN".
    Metric units (MM, CM, YD, etc.) are preserved because they carry meaning.
    Also strips trailing inch-mark (") when used as a unit.
    """
    s = _s(x).strip()
    if not s:
        return None
    t = s.translate(_TRANS).upper()
    # Remove " used as inch-mark after numbers (e.g. '4"' → '4')
    t = re.sub(r'(\d)"', r'\1', t)
    # Remove INCHES/INCH/IN when they immediately follow a number (with optional space).
    # Order matters: try longest match first so "INCH" doesn't get consumed as "IN" + "CH".
    t = re.sub(r'(\d)\s*(?:INCHES|INCH|IN)\b', r'\1', t)
    # Normalize compact DxD/NxN formats: add spaces around X between digits
    # e.g. "4X4" → "4 X 4", "4X 4" → "4 X 4", "4 X4" → "4 X 4"
    t = re.sub(r'(\d)\s*X\s*(\d)', r'\1 X \2', t)
    t = _WS_RE.sub(" ", t).strip()
    return t or None


# =============================================================================
# INPUT VALIDATION (added)
# =============================================================================

# Minimum required columns for each input file
_CATALOG_REQUIRED = {"mpn", "title"}
_KEEPA_REQUIRED   = {"asin", "title"}


def validate_inputs(
    cat_df: pd.DataFrame,
    keepa_df: pd.DataFrame,
    validate_only: bool = False,
) -> bool:
    """
    Validate that input files have the required columns.
    Prints friendly errors showing what was found vs what was expected.
    Returns True if valid, False otherwise.
    When validate_only=True, always prints a summary even on success.
    """
    ok = True

    for label, df, required in [
        ("Catalog", cat_df, _CATALOG_REQUIRED),
        ("Keepa",   keepa_df, _KEEPA_REQUIRED),
    ]:
        found = {c.lower().strip() for c in df.columns}
        missing = required - found

        if missing:
            ok = False
            print(f"\n[ERROR] {label} file is missing required columns:")
            for col in sorted(missing):
                print(f"  ✗ '{col}' — not found")
            print(f"\n  Columns found in file ({len(df.columns)} total):")
            for col in df.columns:
                print(f"    • {col}")
            print(
                f"\n  Tip: Column names are case-insensitive. "
                f"Make sure your file has {sorted(required)} columns."
            )
        elif validate_only:
            print(f"\n[OK] {label} file — all required columns present:")
            for col in sorted(required):
                # Show the actual column name as it appears in the file
                actual = next(c for c in df.columns if c.lower().strip() == col)
                print(f"  ✓ '{actual}'")
            print(f"  Rows: {len(df):,}")

    return ok


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def process_catalog(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process catalog dataframe.
    Returns (catalog_std, catalog_codes) dataframes.
    """
    print(f"Processing catalog: {len(df)} rows")

    # Standardize column access
    def col(name: str) -> pd.Series:
        for c in df.columns:
            if c.lower().replace(" ", "_") == name.lower().replace(" ", "_"):
                return df[c]
            if c.lower() == name.lower():
                return df[c]
        return pd.Series([None] * len(df))

    # Build standardized dataframe
    catalog_std = pd.DataFrame({
        "mpn": col("MPN").astype("string").str.strip(),
        "manufacturer": col("Manufacturer").astype("string").str.strip(),
        "catalog_title": col("Title").astype("string").str.strip(),
        "uom_unit": col("uom_unit").astype("string").str.strip(),
        "uom_quantity": to_num(col("uom_quantity")),
    })

    # Drop rows without MPN
    catalog_std["mpn"] = catalog_std["mpn"].replace({"": pd.NA})
    catalog_std = catalog_std[catalog_std["mpn"].notna()].copy()
    print(f"  After MPN filter: {len(catalog_std)} rows")

    # Parse titles
    print("  Parsing catalog titles...")
    catalog_std["catalog_title2"] = catalog_std["catalog_title"].map(clean_title)
    catalog_std["catalog_title2_lc"] = catalog_std["catalog_title2"].map(lc)

    # Extract attributes
    catalog_std["catalog_color"] = catalog_std["catalog_title"].map(extract_color)
    catalog_std["catalog_size"] = catalog_std["catalog_title"].map(extract_size)

    # Extract quantity from title
    qty_results = catalog_std["catalog_title"].map(extract_quantity)
    catalog_std["catalog_title_qty"] = qty_results.map(lambda x: x.quantity if x else None)
    catalog_std["catalog_title_qty_unit"] = qty_results.map(lambda x: x.unit_type if x else None)
    catalog_std["catalog_title_qty_confidence"] = qty_results.map(lambda x: x.confidence if x else "none")

    # Extract hierarchical quantity from title (e.g., "2/pk, 6pk/cs" -> 12)
    catalog_std["catalog_title_hier_qty"] = catalog_std["catalog_title"].map(parse_hierarchical_qty)

    # Use uom_quantity if available, else hierarchical title qty, else simple title qty
    catalog_std["catalog_qty"] = (
        catalog_std["uom_quantity"]
        .fillna(catalog_std["catalog_title_hier_qty"])
        .fillna(catalog_std["catalog_title_qty"])
    )

    # Default qty to 1 for single-unit UOMs (ea, each, pr, pair) when no qty was found
    single_unit_uoms = {"ea", "each", "pr", "pair"}
    uom_lc = catalog_std["uom_unit"].map(lc)
    is_single_unit = uom_lc.isin(single_unit_uoms) & catalog_std["catalog_qty"].isna()
    catalog_std.loc[is_single_unit, "catalog_qty"] = 1
    if is_single_unit.sum() > 0:
        print(f"  Defaulted {is_single_unit.sum()} rows to qty=1 (single-unit UOM)")

    # Fallback: parse manufacturer from title when column is empty/null.
    # Many catalog titles lead with the brand name (e.g. "CURITY GZE SPG 4X4", "KERLIX ROLL...").
    _MFR_TITLE_SKIP = {
        # Generic product-type words that appear first but aren't brands
        "fabric", "gauze", "sponge", "bandage", "dressing", "pad", "roll",
        "stocking", "wrap", "tape", "foam", "film", "compress", "sterile",
        "adhesive", "elastic", "conforming", "stretch", "non", "woven",
        # Common abbreviations
        "spg", "nw", "ldg", "bx", "cs", "ea", "pk", "rl", "pr", "ns", "lf",
    }

    def _mfr_from_title(title: Any) -> Optional[str]:
        s = _s(title).strip().lower()
        for word in s.split():
            clean = re.sub(r'[^a-z]', '', word)
            if len(clean) >= 4 and clean not in _MFR_TITLE_SKIP:
                return clean
        return None

    null_mfr = catalog_std["manufacturer"].isna() | (catalog_std["manufacturer"].fillna("") == "")
    if null_mfr.any():
        parsed = catalog_std.loc[null_mfr, "catalog_title"].map(_mfr_from_title)
        catalog_std.loc[null_mfr, "manufacturer"] = parsed
        filled = parsed.notna().sum()
        if filled:
            print(f"  Parsed manufacturer from title for {filled} rows")

    # Normalize for matching
    catalog_std["manufacturer_lc"] = catalog_std["manufacturer"].map(lc)
    catalog_std["mpn_code"] = catalog_std["mpn"].map(norm_code)
    catalog_std["catalog_color_norm"] = catalog_std["catalog_color"].map(norm_color)
    catalog_std["catalog_size_norm"] = catalog_std["catalog_size"].map(norm_size)

    # Create codes table
    cat_codes = (
        catalog_std[["mpn", "mpn_code"]]
        .dropna()
        .rename(columns={"mpn_code": "code"})
        .drop_duplicates()
    )

    print(f"  Catalog processing complete: {len(catalog_std)} products, {len(cat_codes)} codes")
    return catalog_std, cat_codes


def process_keepa(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process keepa dataframe.
    Returns (keepa_std, keepa_codes) dataframes.
    """
    print(f"Processing Keepa: {len(df)} rows")

    def col(name: str) -> pd.Series:
        for c in df.columns:
            if c.lower().replace(" ", "_") == name.lower().replace(" ", "_"):
                return df[c]
            if c.lower() == name.lower():
                return df[c]
        return pd.Series([None] * len(df))

    keepa_std = pd.DataFrame({
        "asin": col("ASIN").astype("string").str.strip(),
        "keepa_title": col("Title").astype("string").str.strip(),
        "keepa_brand": col("Brand").astype("string").str.strip(),
        "keepa_model": col("Model").astype("string").str.strip(),
        "variation_attributes": col("Variation Attributes").astype("string").str.strip(),
        "keepa_color": col("Color").astype("string").str.strip(),
        "keepa_size": col("Size").astype("string").str.strip(),
        "keepa_scent": col("Scent").astype("string").str.strip(),
        "keepa_number_of_items": to_num(col("Number of Items")),
        "keepa_package_qty": to_num(col("Package: Quantity")),
        "pick_pack_fee": to_num(col("FBA Pick&Pack Fee")),
        "referral_fee": to_num(col("Referral Fee based on current Buy Box price")),
        "referral_fee_pct": to_percent(col("Referral Fee %")),
        "buy_box_current": to_num(col("Buy Box: Current")),
        "buy_box_30d_avg": to_num(col("Buy Box: 30 days avg.")),
        "bought_past_month": to_num(col("Bought in past month")),
        "upc": col("Product Codes: UPC").astype("string").str.strip(),
        "ean": col("Product Codes: EAN").astype("string").str.strip(),
        "gtin": col("Product Codes: GTIN").astype("string").str.strip(),
        "partnumber": col("Product Codes: PartNumber").astype("string").str.strip(),
    })

    # Drop rows without ASIN
    keepa_std["asin"] = keepa_std["asin"].replace({"": pd.NA})
    keepa_std = keepa_std[keepa_std["asin"].notna()].copy()
    print(f"  After ASIN filter: {len(keepa_std)} rows")

    # Parse titles
    print("  Parsing Keepa titles...")
    keepa_std["keepa_title2"] = keepa_std["keepa_title"].map(clean_title)
    keepa_std["keepa_title2_lc"] = keepa_std["keepa_title2"].map(lc)

    # Handle Size field that's actually quantity
    keepa_std["keepa_size"] = keepa_std["keepa_size"].replace({"": pd.NA})
    size_is_qty = keepa_std["keepa_size"].notna() & keepa_std["keepa_size"].map(looks_like_qty_field)

    # Salvage qty from size field (only for rows where number_of_items is also missing)
    target_mask = size_is_qty & keepa_std["keepa_number_of_items"].isna()
    size_qty = keepa_std.loc[target_mask, "keepa_size"].map(extract_quantity)
    keepa_std.loc[target_mask, "keepa_number_of_items"] = pd.array(
        [float(x.quantity) if x and x.quantity is not None else None for x in size_qty],
        dtype="Float64"
    )
    keepa_std.loc[size_is_qty, "keepa_size"] = pd.NA

    # Extract from title
    keepa_std["keepa_color"] = keepa_std["keepa_color"].replace({"": pd.NA})
    keepa_std["keepa_color"] = keepa_std["keepa_color"].fillna(keepa_std["keepa_title"].map(extract_color))

    keepa_std["keepa_size"] = keepa_std["keepa_size"].fillna(keepa_std["keepa_title"].map(extract_size))

    # Extract quantity from title
    title_qty = keepa_std["keepa_title"].map(extract_quantity)
    keepa_std["keepa_title_qty"] = title_qty.map(lambda x: x.quantity if x else None)
    keepa_std["keepa_title_qty_unit"] = title_qty.map(lambda x: x.unit_type if x else None)
    keepa_std["keepa_title_qty_confidence"] = title_qty.map(lambda x: x.confidence if x else "none")

    # Use number_of_items if available, else title qty
    keepa_std["keepa_qty"] = keepa_std["keepa_number_of_items"].fillna(keepa_std["keepa_title_qty"])

    # Fix: when number_of_items=1 but title clearly says higher qty (e.g., "Pack of 200"),
    # trust the title over the metadata — the "1" likely means "1 case/pack" not "1 each"
    title_qty_vals = keepa_std["keepa_title_qty"].astype("Float64")
    has_misleading_one = (
        (keepa_std["keepa_number_of_items"] == 1)
        & (title_qty_vals > 1)
        & (keepa_std["keepa_title_qty_confidence"] == "high")
    )
    keepa_std.loc[has_misleading_one, "keepa_qty"] = title_qty_vals[has_misleading_one]
    if has_misleading_one.sum() > 0:
        print(f"  Corrected {has_misleading_one.sum()} rows: number_of_items=1 but title has higher qty")

    # Normalize for matching
    keepa_std["brand_lc"] = keepa_std["keepa_brand"].map(lc)
    keepa_std["title_lc"] = keepa_std["keepa_title"].map(lc)
    keepa_std["keepa_color_norm"] = keepa_std["keepa_color"].map(norm_color)
    keepa_std["keepa_size_norm"] = keepa_std["keepa_size"].map(norm_size)

    # Combined codes for matching
    keepa_std["codescombined"] = (
        keepa_std[["keepa_model", "partnumber", "upc", "ean", "gtin"]]
        .fillna("")
        .agg(" ".join, axis=1)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .replace("", None)
    )

    # Create codes table
    code_rows: List[Tuple[str, str]] = []
    for row in keepa_std[["asin", "keepa_model", "partnumber", "upc", "ean", "gtin"]].itertuples(index=False):
        asin = row[0]
        for v in row[1:]:
            for c in iter_norm_codes(v):
                code_rows.append((asin, c))

    keepa_codes = pd.DataFrame(code_rows, columns=["asin", "code"]).drop_duplicates()

    print(f"  Keepa processing complete: {len(keepa_std)} products, {len(keepa_codes)} codes")
    return keepa_std, keepa_codes


def main() -> int:
    ap = argparse.ArgumentParser(description="Build product matching database")
    ap.add_argument("--catalog", required=True, help="Catalog Excel file")
    ap.add_argument("--keepa", required=True, help="Keepa Excel file")
    ap.add_argument("--db", required=True, help="Output SQLite database")
    ap.add_argument("--catalog-sheet", default=None, help="Catalog sheet name")
    ap.add_argument("--keepa-sheet", default=None, help="Keepa sheet name")
    ap.add_argument(
        "--validate-only",
        action="store_true",
        help="Check input file columns and exit without processing",
    )
    args = ap.parse_args()

    # Load Excel files
    print(f"\nLoading catalog: {args.catalog}")
    cat_raw = pd.read_excel(args.catalog, sheet_name=args.catalog_sheet or 0, dtype=str)
    print(f"  Loaded {len(cat_raw)} rows, {len(cat_raw.columns)} columns")

    print(f"\nLoading Keepa: {args.keepa}")
    if args.keepa.endswith('.csv'):
        kee_raw = pd.read_csv(args.keepa, dtype=str)
    elif args.keepa_sheet:
        kee_raw = pd.read_excel(args.keepa, sheet_name=args.keepa_sheet, dtype=str)
    else:
        sheets = pd.read_excel(args.keepa, sheet_name=None, dtype=str)
        if len(sheets) > 1:
            print(f"  Found {len(sheets)} sheets: {list(sheets.keys())} — combining all")
            kee_raw = pd.concat(sheets.values(), ignore_index=True)
        else:
            kee_raw = next(iter(sheets.values()))
    print(f"  Loaded {len(kee_raw)} rows, {len(kee_raw.columns)} columns")

    # Validate inputs — always runs; exits early if --validate-only
    if not validate_inputs(cat_raw, kee_raw, validate_only=args.validate_only):
        print("\nAborting: fix the column issues above and re-run.")
        return 1

    if args.validate_only:
        print("\nValidation passed. Re-run without --validate-only to process.")
        return 0

    # Process
    catalog_std, cat_codes = process_catalog(cat_raw)
    keepa_std, keepa_codes = process_keepa(kee_raw)

    # Save to database
    print(f"\nWriting to database: {args.db}")
    con = connect_db(args.db)

    catalog_std.to_sql("catalog_std", con, if_exists="replace", index=False, chunksize=500)
    cat_codes.to_sql("catalog_codes", con, if_exists="replace", index=False, chunksize=500)
    keepa_std.to_sql("keepa_std", con, if_exists="replace", index=False, chunksize=500)
    keepa_codes.to_sql("keepa_codes", con, if_exists="replace", index=False, chunksize=500)

    # Create indexes
    print("  Creating indexes...")
    con.executescript("""
        CREATE INDEX IF NOT EXISTS ix_cat_mpn ON catalog_std(mpn);
        CREATE INDEX IF NOT EXISTS ix_cat_code ON catalog_codes(code);
        CREATE INDEX IF NOT EXISTS ix_keepa_asin ON keepa_std(asin);
        CREATE INDEX IF NOT EXISTS ix_keepa_code ON keepa_codes(code);
        CREATE INDEX IF NOT EXISTS ix_cat_title2 ON catalog_std(catalog_title2);
        CREATE INDEX IF NOT EXISTS ix_keepa_title2 ON keepa_std(keepa_title2);
    """)

    con.commit()
    con.close()

    print(f"\nDatabase built successfully!")
    print(f"  Catalog: {len(catalog_std)} products, {len(cat_codes)} codes")
    print(f"  Keepa: {len(keepa_std)} products, {len(keepa_codes)} codes")
    print(f"\nNext step: Run pipeline.sql with run_sql.py")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())