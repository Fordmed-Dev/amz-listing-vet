# file: ai_helper.py
"""
OpenAI GPT-4o-mini API utilities for batch title parsing and match verification.
Designed to be cost-effective with large datasets (10K-100K rows).
"""

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Optional

# Rate limiting settings
RATE_LIMIT_DELAY = 0.1  # seconds between calls
MAX_RETRIES = 3
BATCH_SIZE = 15  # titles per batch call
MODEL = "gpt-4.1-nano"


@dataclass
class ParsedAttributes:
    """Attributes extracted from a product title."""
    quantity: Optional[int] = None
    color: Optional[str] = None
    size: Optional[str] = None
    material: Optional[str] = None
    brand: Optional[str] = None
    unit_type: Optional[str] = None  # pack, case, box, each
    raw_response: Optional[str] = None


@dataclass
class MatchVerification:
    """Result of GPT verifying a product match."""
    confidence: float  # 0.0 to 1.0
    is_match: bool
    reasoning: str
    catalog_qty_interpreted: Optional[int] = None
    keepa_qty_interpreted: Optional[int] = None


def get_openai_client():
    """
    Initialize OpenAI API client.
    Returns None if API key not available.
    """
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    api_key = os.environ.get("CHATGPT_API_KEY")
    if not api_key:
        return None

    try:
        from openai import OpenAI
        return OpenAI(api_key=api_key)
    except ImportError:
        print("Warning: openai package not installed. Run: pip install openai")
        return None




def _safe_json_parse(text: str) -> Optional[dict]:
    """Parse JSON from GPT response, handling markdown code blocks."""
    text = text.strip()

    # Remove markdown code blocks
    if text.startswith("```"):
        lines = text.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)

    text = text.replace("```json", "").replace("```", "").strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _chat_completion(client, prompt: str, max_tokens: int = 2000) -> Optional[str]:
    """Send a chat completion request to OpenAI."""
    response = client.chat.completions.create(
        model=MODEL,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.choices[0].message.content


def batch_parse_titles(
    client,
    titles: list[tuple[str, str]],  # list of (id, title) tuples
    parse_type: str = "keepa"  # "keepa" or "catalog"
) -> dict[str, ParsedAttributes]:
    """
    Parse multiple titles in a single GPT call for efficiency.

    Args:
        client: OpenAI client
        titles: List of (id, title) tuples
        parse_type: "keepa" or "catalog" to adjust parsing hints

    Returns:
        Dict mapping id -> ParsedAttributes
    """
    if not client or not titles:
        return {}

    results = {}

    # Process in batches
    for i in range(0, len(titles), BATCH_SIZE):
        batch = titles[i:i + BATCH_SIZE]
        batch_results = _parse_title_batch(client, batch, parse_type)
        results.update(batch_results)

        # Rate limiting
        if i + BATCH_SIZE < len(titles):
            time.sleep(RATE_LIMIT_DELAY)

    return results


def _parse_title_batch(
    client,
    batch: list[tuple[str, str]],
    parse_type: str
) -> dict[str, ParsedAttributes]:
    """Parse a single batch of titles."""

    # Build the prompt
    titles_text = "\n".join(f"{idx+1}. [{id_}] {title}" for idx, (id_, title) in enumerate(batch))

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
            print(f"Warning: Could not parse GPT response for batch")
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
                    raw_response=None
                )
            else:
                results[id_] = ParsedAttributes()

        return results

    except Exception as e:
        print(f"OpenAI API error in batch parsing: {e}")
        return {id_: ParsedAttributes() for id_, _ in batch}


def batch_verify_matches(
    client,
    matches: list[dict],  # list of match dicts with catalog_title, keepa_title, etc.
    batch_size: int = 5
) -> list[MatchVerification]:
    """
    Verify multiple uncertain matches using GPT.

    Args:
        client: OpenAI client
        matches: List of dicts with keys: id, catalog_title, keepa_title,
                 catalog_qty, keepa_qty, matched_code, fuzzy_score

    Returns:
        List of MatchVerification results in same order as input
    """
    if not client or not matches:
        return [MatchVerification(0.0, False, "No client") for _ in matches]

    results = []

    for i in range(0, len(matches), batch_size):
        batch = matches[i:i + batch_size]
        batch_results = _verify_match_batch(client, batch)
        results.extend(batch_results)

        if i + batch_size < len(matches):
            time.sleep(RATE_LIMIT_DELAY)

    return results


def _verify_match_batch(client, batch: list[dict]) -> list[MatchVerification]:
    """Verify a single batch of matches."""

    # Build comparison text
    comparisons = []
    for idx, m in enumerate(batch):
        comparisons.append(f"""
Match {idx + 1} (ID: {m.get('id', idx)}):
  Catalog: "{m.get('catalog_title', '')}"
  Amazon:  "{m.get('keepa_title', '')}"
  Matched Code: {m.get('matched_code', 'N/A')}
  Catalog Qty: {m.get('catalog_qty', 'N/A')}
  Keepa Qty: {m.get('keepa_qty', 'N/A')}
  Fuzzy Score: {m.get('fuzzy_score', 'N/A')}""")

    prompt = f"""Analyze these potential product matches between a catalog and Amazon listings.
For each match, determine if they are THE SAME PRODUCT (same brand, same SKU, same quantity).

IMPORTANT RULES:
- Same product in different quantities = NOT a match (12-pack vs 6-pack)
- Same product, same quantity = MATCH
- Similar but different products = NOT a match
- If matched_code appears in both, that's strong evidence FOR a match
- Consider brand, size, color, material differences

{chr(10).join(comparisons)}

Return JSON array with one object per match:
[
  {{
    "match_id": 1,
    "confidence": 0.95,
    "is_match": true,
    "reasoning": "Same product code, same brand, quantities align",
    "catalog_qty_interpreted": 12,
    "keepa_qty_interpreted": 12
  }},
  ...
]

confidence: 0.0-1.0 (how sure are you?)
is_match: true/false (are these the same product?)
reasoning: Brief explanation (max 50 words)"""

    try:
        response_text = _chat_completion(client, prompt)
        if not response_text:
            return [MatchVerification(0.5, False, "Empty response", None, None) for _ in batch]

        parsed = _safe_json_parse(response_text)

        if not parsed or not isinstance(parsed, list):
            print(f"Warning: Could not parse GPT verification response")
            return [MatchVerification(0.5, False, "Parse error", None, None) for _ in batch]

        results = []
        for idx, m in enumerate(batch):
            if idx < len(parsed):
                r = parsed[idx]
                results.append(MatchVerification(
                    confidence=float(r.get("confidence", 0.5)),
                    is_match=bool(r.get("is_match", False)),
                    reasoning=str(r.get("reasoning", "")),
                    catalog_qty_interpreted=r.get("catalog_qty_interpreted"),
                    keepa_qty_interpreted=r.get("keepa_qty_interpreted")
                ))
            else:
                results.append(MatchVerification(0.5, False, "Missing from response", None, None))

        return results

    except Exception as e:
        print(f"OpenAI API error in match verification: {e}")
        return [MatchVerification(0.0, False, f"API error: {e}", None, None) for _ in batch]


def parse_single_title(client, title: str, context: str = "") -> ParsedAttributes:
    """
    Parse a single title (fallback for complex cases).
    Use batch_parse_titles for efficiency when possible.
    """
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
                unit_type=parsed.get("unit_type")
            )
    except Exception as e:
        print(f"OpenAI API error: {e}")

    return ParsedAttributes()


def estimate_cost(num_titles_to_parse: int, num_matches_to_verify: int) -> dict:
    """
    Estimate OpenAI GPT-4o-mini API costs.

    Returns dict with estimated costs in USD.
    """
    # GPT-4o-mini pricing: ~$0.15/MTok input, ~$0.60/MTok output
    # Average ~500 tokens input, ~200 tokens output per batch

    parse_batches = (num_titles_to_parse + BATCH_SIZE - 1) // BATCH_SIZE
    verify_batches = (num_matches_to_verify + 4) // 5

    # ~$0.0002 per batch for parsing, ~$0.0003 per batch for verification
    parse_cost = parse_batches * 0.0002
    verify_cost = verify_batches * 0.0003

    return {
        "parse_batches": parse_batches,
        "verify_batches": verify_batches,
        "estimated_parse_cost_usd": round(parse_cost, 4),
        "estimated_verify_cost_usd": round(verify_cost, 4),
        "estimated_total_cost_usd": round(parse_cost + verify_cost, 4)
    }
