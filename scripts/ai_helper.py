# file: ai_helper.py
"""
OpenAI API utilities for batch title parsing and match verification.
Designed to be cost-effective with large datasets (10K-100K rows).
"""

import json
import os
import re
import time
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from openai import OpenAI

# Rate limiting settings
RATE_LIMIT_DELAY = 0.1  # seconds between calls
MAX_RETRIES = 3
BATCH_SIZE = 15         # titles per batch for parsing
MODEL = "gpt-4.1-nano"


@dataclass
class ParsedAttributes:
    """Attributes extracted from a product title."""
    quantity:     Optional[int]  = None
    color:        Optional[str]  = None
    size:         Optional[str]  = None
    material:     Optional[str]  = None
    brand:        Optional[str]  = None
    unit_type:    Optional[str]  = None  # pack, case, box, each
    raw_response: Optional[str]  = None


@dataclass
class MatchVerification:
    """Result of GPT verifying a product match."""
    confidence: float   # 0.0 – 1.0
    is_match:   bool
    reasoning:  str
    # Kept as optional for DB compatibility — will be None with simplified prompt
    dim_brand:               Optional[float] = None
    dim_sku_code:            Optional[float] = None
    dim_quantity:            Optional[float] = None
    dim_unit_type:           Optional[float] = None
    catalog_qty_interpreted: Optional[int]   = None
    keepa_qty_interpreted:   Optional[int]   = None


def get_openai_client() -> Optional["OpenAI"]:
    """Initialize OpenAI API client. Returns None if key not available."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    api_key = os.environ.get("CHATGPT_API_KEY")
    if not api_key:
        return None

    try:
        from openai import OpenAI as _OpenAI
        return _OpenAI(api_key=api_key)
    except ImportError:
        print("Warning: openai package not installed. Run: pip install openai")
        return None


def _safe_json_parse(text: str) -> Optional[dict]:
    """Parse JSON from GPT response, handling markdown code blocks."""
    text = text.strip()

    # Remove markdown code fences
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"```\s*$",          "", text, flags=re.MULTILINE)
    text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _chat_completion(client, prompt: str, max_tokens: int = 2000, json_mode: bool = False) -> Optional[str]:
    """Send a chat completion request to OpenAI with retry + exponential backoff."""
    kwargs = dict(
        model=MODEL,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
        timeout=60,
    )
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.chat.completions.create(**kwargs)
            return response.choices[0].message.content
        except Exception as e:
            wait = 2 ** attempt
            if attempt < MAX_RETRIES:
                print(f"  API error (attempt {attempt}/{MAX_RETRIES}): {e}")
                print(f"  Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"  API error after {MAX_RETRIES} attempts: {e}")
                return None


# =============================================================================
# TITLE PARSING
# =============================================================================

def batch_parse_titles(
    client,
    titles: list[tuple[str, str]],
    parse_type: str = "keepa"
) -> dict[str, ParsedAttributes]:
    """Parse multiple titles in a single GPT call for efficiency."""
    if not client or not titles:
        return {}

    results = {}
    for i in range(0, len(titles), BATCH_SIZE):
        batch = titles[i:i + BATCH_SIZE]
        results.update(_parse_title_batch(client, batch, parse_type))
        if i + BATCH_SIZE < len(titles):
            time.sleep(RATE_LIMIT_DELAY)

    return results


def _parse_title_batch(client, batch: list[tuple[str, str]], parse_type: str) -> dict[str, ParsedAttributes]:
    """Parse a single batch of titles."""
    titles_text = "\n".join(
        f"{idx+1}. [{id_}] {title}" for idx, (id_, title) in enumerate(batch)
    )

    prompt = f"""Extract product attributes from these {parse_type} titles. Focus on:
- quantity: The pack/case/box count (NOT dimensions). Examples: "Pack of 12" = 12, "100/Box" = 100
- unit_type: pack, case, box, bag, roll, each, set, pair, dozen
- color: Product color if mentioned
- size: Physical dimensions or size category (S/M/L/XL, or measurements like "4x6 inch")
- material: Primary material if mentioned
- brand: Brand name if identifiable

IMPORTANT:
- Distinguish quantity from dimensions: "12x12 inch" is size, "12-pack" is quantity
- "100 Count" = quantity 100, "100ml" = size not quantity
- Return null for attributes not clearly present

Titles:
{titles_text}

Return a JSON object with IDs as keys:
{{
  "ID1": {{"quantity": 12, "unit_type": "pack", "color": "blue", "size": "large", "material": null, "brand": "Acme"}},
  "ID2": {{"quantity": null, "unit_type": null, "color": null, "size": "4x6 inch", "material": "paper", "brand": null}},
  ...
}}"""

    try:
        response_text = _chat_completion(client, prompt)
        if not response_text:
            return {id_: ParsedAttributes() for id_, _ in batch}

        parsed = _safe_json_parse(response_text)
        if not parsed:
            print("Warning: Could not parse GPT response for batch")
            return {id_: ParsedAttributes(raw_response=response_text) for id_, _ in batch}

        results = {}
        for id_, _ in batch:
            if id_ in parsed:
                attrs = parsed[id_]
                results[id_] = ParsedAttributes(
                    quantity=attrs.get("quantity"),
                    color=attrs.get("color"),
                    size=attrs.get("size"),
                    material=attrs.get("material"),
                    brand=attrs.get("brand"),
                    unit_type=attrs.get("unit_type"),
                )
            else:
                results[id_] = ParsedAttributes()

        return results

    except Exception as e:
        print(f"OpenAI API error in batch parsing: {e}")
        return {id_: ParsedAttributes() for id_, _ in batch}


# =============================================================================
# MATCH VERIFICATION
# =============================================================================

def batch_verify_matches(
    client,
    matches: list[dict],
    batch_size: int = 5
) -> list[MatchVerification]:
    """Verify multiple matches using GPT."""
    if not client or not matches:
        return [MatchVerification(0.0, False, "No client") for _ in matches]

    results = []
    for i in range(0, len(matches), batch_size):
        batch = matches[i:i + batch_size]
        results.extend(_verify_match_batch(client, batch))
        if i + batch_size < len(matches):
            time.sleep(RATE_LIMIT_DELAY)
    return results


def _verify_single_fallback(client, m: dict) -> MatchVerification:
    """Retry a single match with a minimal prompt when batch JSON parsing fails."""
    mpn = m.get('id', '').split('|')[0] if '|' in m.get('id', '') else m.get('id', '?')
    prompt = (
        f'Are these the same product? Answer with JSON only: {{"confidence": 0.0-1.0, "is_match": true/false, "reasoning": "..."}}\n'
        f'Catalog: "{m.get("catalog_title", "")}"\n'
        f'Amazon:  "{m.get("keepa_title", "")}"\n'
        f'Code: {m.get("matched_code", "N/A")}'
    )
    response_text = _chat_completion(client, prompt, max_tokens=200, json_mode=True)
    if response_text:
        parsed = _safe_json_parse(response_text)
        if parsed and "confidence" in parsed:
            return MatchVerification(
                confidence=round(float(parsed["confidence"]), 4),
                is_match=bool(parsed.get("is_match", False)),
                reasoning=str(parsed.get("reasoning", "")),
            )
    print(f"  Warning: single-match fallback also failed for MPN {mpn}")
    return MatchVerification(0.65, False, "Parse error (fallback)")


def _verify_match_batch(client, batch: list[dict]) -> list[MatchVerification]:
    """
    Verify a single batch of matches.
    Asks GPT: is this catalog MPN the same product as this Amazon ASIN?
    Uses json_mode=True to guarantee valid JSON output.
    """
    comparisons = []
    for idx, m in enumerate(batch):
        mpn = m.get('id', '').split('|')[0] if '|' in m.get('id', '') else m.get('id', idx)
        cqty = m.get('catalog_qty')
        kqty = m.get('keepa_qty')
        qty_line = f"{cqty} vs {kqty}" if cqty or kqty else "unknown"
        comparisons.append(f"""
Match {idx + 1}:
  Catalog MPN   : {mpn}
  Catalog title : "{m.get('catalog_title', '')}"
  Amazon title  : "{m.get('keepa_title', '')}"
  Matched code  : {m.get('matched_code', 'N/A')}
  Quantities    : {qty_line}
  Title fuzzy   : {m.get('fuzzy_score', 'N/A')}""")

    prompt = f"""You are a product matching expert for a B2B medical/industrial supplier catalog.
For each pair, determine if the catalog product and the Amazon listing are the SAME physical item.

Rules:
- Catalog titles are terse internal codes. Common abbreviations: SPG=sponge, STR=sterile,
  NS=non-sterile, NW=non-woven, BDG=bandage, DRS=dressing, LF=latex-free, PLY=ply,
  GZE=gauze, CT=count, CS=case, PK=pack, RL=roll, PR=pair, EA=each.
  Amazon titles are fully descriptive. This asymmetry is EXPECTED and normal.
- Non-woven sponge and gauze sponge are the SAME product type in medical supplies.
- If the catalog title omits a sterility spec (sterile/non-sterile) that Amazon has, treat
  this as missing info NOT a mismatch. Only flag sterility as a conflict if BOTH titles
  explicitly state DIFFERENT sterility levels.
- If the Matched code appears in the Amazon title, treat that as strong evidence of identity.
- Title fuzzy score >= 50 means the titles are already substantially similar.
- Use titles primarily to RULE OUT clearly wrong matches (e.g. a compression stocking vs. an auto part).
- A matched code alone is NOT sufficient if the titles describe completely different product categories.
- Quantity differences are often just different pack levels of the same product — not a conflict.
- Confidence 0.9+ means you are nearly certain they are the same item.
- Confidence 0.0 means they are clearly different products.

{chr(10).join(comparisons)}

Return a JSON object with a "matches" key containing an array — one object per match, in the same order:
{{
  "matches": [
    {{
      "match_id"  : 1,
      "confidence": 0.95,
      "is_match"  : true,
      "reasoning" : "Titles describe the same product and MPN is present in the Amazon listing."
    }}
  ]
}}"""

    try:
        response_text = _chat_completion(client, prompt, json_mode=True)
        if not response_text:
            return [MatchVerification(0.5, False, "Empty response") for _ in batch]

        parsed = _safe_json_parse(response_text)
        matches_list = parsed.get("matches") if isinstance(parsed, dict) else None

        if not matches_list or not isinstance(matches_list, list):
            print(f"  Warning: Could not parse batch response — retrying {len(batch)} matches one-by-one")
            return [_verify_single_fallback(client, m) for m in batch]

        results = []
        for idx, m in enumerate(batch):
            if idx < len(matches_list):
                r = matches_list[idx]
                confidence = round(float(r.get("confidence", 0.5)), 4)
                results.append(MatchVerification(
                    confidence=confidence,
                    is_match=bool(r.get("is_match", False)),
                    reasoning=str(r.get("reasoning", "")),
                ))
            else:
                results.append(_verify_single_fallback(client, m))

        return results

    except Exception as e:
        print(f"Error processing verification results: {e}")
        return [MatchVerification(0.0, False, f"Error: {e}") for _ in batch]


def parse_single_title(client, title: str, context: str = "") -> ParsedAttributes:
    """Parse a single title. Use batch_parse_titles for efficiency when possible."""
    if not client or not title:
        return ParsedAttributes()

    prompt = f"""Extract product attributes from this title: "{title}"
{f'Context: {context}' if context else ''}

Return JSON:
{{
  "quantity": <pack/case count as integer or null>,
  "unit_type": <"pack"/"case"/"box"/"each"/etc or null>,
  "color": <color or null>,
  "size": <size/dimensions or null>,
  "material": <material or null>,
  "brand": <brand or null>
}}

IMPORTANT: quantity is pack/case count, NOT dimensions. "12x12" is size, "12-pack" is quantity=12."""

    try:
        response_text = _chat_completion(client, prompt, max_tokens=300)
        if not response_text:
            return ParsedAttributes()

        parsed = _safe_json_parse(response_text)
        if parsed:
            return ParsedAttributes(
                quantity=parsed.get("quantity"),
                color=parsed.get("color"),
                size=parsed.get("size"),
                material=parsed.get("material"),
                brand=parsed.get("brand"),
                unit_type=parsed.get("unit_type"),
            )
    except Exception as e:
        print(f"OpenAI API error: {e}")

    return ParsedAttributes()


def estimate_cost(num_titles_to_parse: int, num_matches_to_verify: int) -> dict:
    """Estimate OpenAI API costs."""
    parse_batches  = (num_titles_to_parse   + BATCH_SIZE - 1) // BATCH_SIZE
    verify_batches = (num_matches_to_verify + 4)           // 5

    parse_cost  = parse_batches  * 0.0002
    verify_cost = verify_batches * 0.0003

    return {
        "parse_batches":             parse_batches,
        "verify_batches":            verify_batches,
        "estimated_parse_cost_usd":  round(parse_cost,  4),
        "estimated_verify_cost_usd": round(verify_cost, 4),
        "estimated_total_cost_usd":  round(parse_cost + verify_cost, 4),
    }
