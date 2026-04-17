"""Resource-group rule evaluator.

Given a resource (dict form from discovery OR an ORM Resource) and a set of
ResourceGroup rows, decide which groups match. Used by discovery-worker to
auto-assign SLA policies via GroupPolicyAssignment — and reusable anywhere
else we need to resolve "which groups does this resource belong to?".

Rule shape (JSON stored on ResourceGroup.rules):
    {
      "field":   "NAME" | "EMAIL" | "DEPARTMENT" | "CITY" | "COUNTRY" |
                 "JOB_TITLE" | "RESOURCE_TYPE" | "EXTERNAL_ID" | "TAG_VALUE",
      "operator": "EQUALS" | "NOT_EQUALS" | "CONTAINS" | "NOT_CONTAINS" |
                  "STARTS_WITH" | "ENDS_WITH" | "IN",
      "value":   <string>   (for IN: comma-separated list)
    }

combinator on the group = "AND" / "OR" across the rules array.

Case handling: all string comparisons are case-insensitive. Empty rules = no match.
"""

from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional


# Mapping from canonical rule field names to attribute paths on the resource dict.
# Resources come in two shapes:
#   - ORM object with .display_name / .email / .type.value / .external_id / .extra_data
#   - Dict from discovery staging with "display_name"/"email"/"type"/"external_id"/"metadata"
# The extractor normalizes both.

def _get_field(resource: Any, field: str) -> Optional[str]:
    """Extract a string value for a canonical field from a resource.
    Returns None if the field is absent (which makes the rule non-matching)."""
    is_obj = not isinstance(resource, dict)
    get = (lambda k: getattr(resource, k, None)) if is_obj else (lambda k: resource.get(k))
    meta = get("metadata") if is_obj else resource.get("metadata") or resource.get("extra_data") or {}
    if meta is None:
        meta = {}

    if field == "NAME":
        return get("display_name") or get("name")
    if field == "EMAIL":
        return get("email")
    if field == "EXTERNAL_ID":
        return get("external_id")
    if field == "RESOURCE_TYPE":
        t = get("type")
        # Enum or raw string — normalize to string.
        return t.value if hasattr(t, "value") else (t if isinstance(t, str) else None)
    # Extended-attribute fields live in metadata (set by discovery):
    #   department, job_title / jobTitle, city, country, tags[].value
    if field == "DEPARTMENT":
        return meta.get("department") or meta.get("Department")
    if field == "JOB_TITLE":
        return meta.get("job_title") or meta.get("jobTitle")
    if field == "CITY":
        return meta.get("city") or meta.get("City")
    if field == "COUNTRY":
        return meta.get("country") or meta.get("usageLocation")
    if field == "TAG_VALUE":
        # Azure tag values: metadata.tags is a dict; we match any tag value
        tags = meta.get("tags") or {}
        if isinstance(tags, dict):
            return ",".join(str(v) for v in tags.values()) or None
    return None


def _apply_operator(lhs: Optional[str], operator: str, rhs: str) -> bool:
    if lhs is None:
        return operator in ("NOT_EQUALS", "NOT_CONTAINS")  # absence matches NOT-style rules
    left = str(lhs).lower().strip()
    right = str(rhs).lower().strip()
    if operator == "EQUALS":
        return left == right
    if operator == "NOT_EQUALS":
        return left != right
    if operator == "CONTAINS":
        return right in left
    if operator == "NOT_CONTAINS":
        return right not in left
    if operator == "STARTS_WITH":
        return left.startswith(right)
    if operator == "ENDS_WITH":
        return left.endswith(right)
    if operator == "IN":
        values = {v.strip().lower() for v in right.split(",") if v.strip()}
        return left in values
    return False


def resource_matches_group(resource: Any, rules: List[Dict[str, Any]], combinator: str) -> bool:
    """Evaluate a resource against a group's rules + combinator."""
    if not rules:
        return False
    combinator = (combinator or "AND").upper()
    outcomes = [
        _apply_operator(
            _get_field(resource, (r.get("field") or "").upper()),
            (r.get("operator") or "").upper(),
            r.get("value") or "",
        )
        for r in rules
    ]
    return all(outcomes) if combinator == "AND" else any(outcomes)


def find_matching_groups(
    resource: Any,
    groups: Iterable[Any],
) -> List[Any]:
    """Return enabled groups that the resource matches, sorted by priority ascending
    (lower priority number = higher precedence — matches the afi.ai convention).
    Accepts ResourceGroup ORM objects or dicts with the same shape."""
    matched = []
    for g in groups:
        enabled = getattr(g, "enabled", True) if not isinstance(g, dict) else g.get("enabled", True)
        if not enabled:
            continue
        rules = getattr(g, "rules", None) if not isinstance(g, dict) else g.get("rules", [])
        combinator = getattr(g, "combinator", "AND") if not isinstance(g, dict) else g.get("combinator", "AND")
        group_type = getattr(g, "group_type", "DYNAMIC") if not isinstance(g, dict) else g.get("group_type", "DYNAMIC")
        # Static groups bypass rule evaluation — they're explicit lists (outside this helper's scope).
        if group_type == "STATIC":
            continue
        if resource_matches_group(resource, rules or [], combinator):
            matched.append(g)

    def _prio(g):
        return getattr(g, "priority", 100) if not isinstance(g, dict) else g.get("priority", 100)

    matched.sort(key=_prio)
    return matched
