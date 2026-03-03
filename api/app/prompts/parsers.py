"""
Response parsers for Arctic-Text2SQL-R1 model output.

extract_sql() extracts the final SQL query from <think>/<answer> tagged responses.
extract_think() extracts the reasoning trace for debugging and telemetry.
"""

import re
from typing import Optional


def extract_sql(response: str) -> str:
    """Extract the final SQL query from an Arctic-Text2SQL-R1 response.

    Priority order:
      1. Content inside <answer>...</answer> tags (official Arctic format)
      2. Last ```sql ... ``` code block anywhere in response (fallback)
      3. Last SELECT statement in response (last resort)
    """
    # Strip <think> block — never search inside it
    clean = re.sub(r"<think>.*?</think>", "", response, flags=re.DOTALL).strip()

    # Try <answer> tag extraction
    answer_matches = re.findall(r"<answer>(.*?)</answer>", clean, re.DOTALL)
    if answer_matches:
        answer_content = answer_matches[-1].strip()
        code_matches = re.findall(r"```(?:sql)?\s*(.*?)```", answer_content, re.DOTALL)
        if code_matches:
            return code_matches[-1].strip()
        return answer_content.strip()

    # Fallback — last code block anywhere in cleaned response
    code_matches = re.findall(r"```(?:sql)?\s*(.*?)```", clean, re.DOTALL | re.IGNORECASE)
    if code_matches:
        return code_matches[-1].strip()

    # Last resort — everything from last SELECT keyword
    idx = clean.rfind("SELECT")
    if idx != -1:
        return clean[idx:].strip()
    return clean.strip()


def extract_think(response: str) -> Optional[str]:
    """Extract the model's reasoning trace from <think> tags.

    Returns None if no <think> block is present (e.g. OmniSQL-style responses).
    """
    match = re.search(r"<think>(.*?)</think>", response, re.DOTALL)
    return match.group(1).strip() if match else None
