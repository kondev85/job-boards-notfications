#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Small, dependency-free Gemini client for evidence-based CV profiles."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_MODEL = "gemini-2.5-flash"
PROFILE_VERSION = "v1"
PROFILE_REQUIRED_KEYS = (
    "schema_version",
    "professional_summary",
    "seniority",
    "experience_areas",
    "industries",
    "skills",
    "technologies_tools_methodologies",
    "leadership_and_responsibility",
    "strengths",
    "gaps_or_limited_evidence",
    "transferable_capabilities",
)

_STRENGTHS = {"strong", "moderate", "limited", "incidental"}
_RECENCY = {"current", "recent", "older", "unknown"}
_DEPTH = {"deep", "moderate", "surface", "unknown"}
_SKILL_CATEGORIES = {
    "technical",
    "managerial",
    "operational",
    "analytical",
    "commercial",
    "communication",
    "leadership",
    "domain",
    "other",
}
_PROFICIENCY = {"strong", "working_knowledge", "exposure", "training_only"}
_TECHNOLOGY_TYPES = {
    "technology",
    "tool",
    "platform",
    "methodology",
    "certification",
    "other",
}
_GAP_ASSESSMENTS = {"limited_evidence", "no_clear_evidence"}


class GeminiProfileError(RuntimeError):
    """Base error for profile generation failures."""


class GeminiConfigurationError(GeminiProfileError):
    """Raised when Gemini is not configured."""


class GeminiAPIError(GeminiProfileError):
    """Raised when Gemini cannot complete the request."""


class GeminiResponseError(GeminiProfileError):
    """Raised when Gemini returns an unusable profile."""


Transport = Callable[[str, str, dict[str, Any], float], dict[str, Any]]


def cv_source_hash(cv_text: str) -> str:
    """Return a stable identifier for the CV without storing the CV in logs."""
    return hashlib.sha256(cv_text.encode("utf-8")).hexdigest()


def _default_transport(
    api_key: str,
    model_name: str,
    payload: dict[str, Any],
    timeout: float,
) -> dict[str, Any]:
    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model_name}:generateContent"
    )
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-goog-api-key": api_key,
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise GeminiAPIError(f"Gemini request failed with HTTP {exc.code}") from None
    except (URLError, TimeoutError, OSError) as exc:
        raise GeminiAPIError("Gemini request failed before receiving a response") from exc
    except json.JSONDecodeError as exc:
        raise GeminiAPIError("Gemini returned an invalid API response") from exc


def _profile_prompt(cv_text: str, existing_profile_text: str | None) -> str:
    previous = ""
    if existing_profile_text and existing_profile_text.strip():
        previous = (
            "\nAn earlier profile summary may be stale. Use it only as context and "
            "prefer evidence from the CV:\n---\n"
            f"{existing_profile_text.strip()}\n---\n"
        )
    return f"""
Analyze the following professional CV and return one evidence-based professional
profile. The person may work in any profession or industry. Discover their actual
experience; do not assume Project Management, Product Management, Technology, or
any other predefined career path.

Distinguish demonstrated experience from possible transferable capability. Do not
invent years, seniority, tools, industries, responsibilities, or skills. A missing
statement is not proof that the person lacks a skill. Use "limited evidence" or
"no clear evidence" where appropriate. Do not treat a job title alone as proof of
identical responsibilities, and do not treat one project or one technology mention
as deep experience.

Return valid JSON only, using exactly this top-level structure:
{{
  "schema_version": "v1",
  "professional_summary": "concise internal matching profile",
  "seniority": {{"level": "", "evidence": ""}},
  "experience_areas": [{{
    "area": "",
    "strength": "strong|moderate|limited|incidental",
    "years": null,
    "recency": "current|recent|older|unknown",
    "seniority": "",
    "evidence": ""
  }}],
  "industries": [{{
    "industry": "",
    "strength": "strong|moderate|limited|incidental",
    "years": null,
    "recency": "current|recent|older|unknown",
    "depth": "deep|moderate|surface|unknown",
    "evidence": ""
  }}],
  "skills": [{{
    "skill": "",
    "category": "technical|managerial|operational|analytical|commercial|communication|leadership|domain|other",
    "strength": "strong|moderate|limited|exposure",
    "evidence": ""
  }}],
  "technologies_tools_methodologies": [{{
    "name": "",
    "type": "technology|tool|platform|methodology|certification|other",
    "proficiency": "strong|working_knowledge|exposure|training_only",
    "evidence": ""
  }}],
  "leadership_and_responsibility": [{{
    "area": "",
    "strength": "strong|moderate|limited|evidence_only",
    "evidence": ""
  }}],
  "strengths": [{{"area": "", "reason": ""}}],
  "gaps_or_limited_evidence": [{{
    "area": "",
    "assessment": "limited_evidence|no_clear_evidence",
    "reason": ""
  }}],
  "transferable_capabilities": [{{
    "capability": "",
    "evidence": "",
    "potential_applications": []
  }}]
}}

The JSON must contain every top-level key, even when its value is an empty array.
Use null only for an experience or industry years value that is not reasonably
supported. The professional_summary must be concise and must not be a CV or cover
letter.
{previous}
CV:
---
{cv_text}
---
""".strip()


def _response_text(response: dict[str, Any]) -> str:
    try:
        candidates = response["candidates"]
        parts = candidates[0]["content"]["parts"]
        text = "".join(part["text"] for part in parts if "text" in part)
    except (KeyError, IndexError, TypeError) as exc:
        raise GeminiResponseError("Gemini response did not contain profile text") from exc
    if not text.strip():
        raise GeminiResponseError("Gemini response contained an empty profile")
    return text.strip()


def _parse_json(text: str) -> dict[str, Any]:
    candidate = text.strip()
    if candidate.startswith("```") and candidate.endswith("```"):
        candidate = candidate[3:-3].strip()
        if candidate.lower().startswith("json"):
            candidate = candidate[4:].lstrip()
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError as exc:
        raise GeminiResponseError("Gemini returned invalid profile JSON") from exc
    if not isinstance(parsed, dict):
        raise GeminiResponseError("Gemini profile JSON must be an object")
    return parsed


def _require_string(value: Any, field: str, *, allow_empty: bool = True) -> None:
    if not isinstance(value, str) or (not allow_empty and not value.strip()):
        raise GeminiResponseError(f"Profile field {field} must be a string")


def _require_list(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise GeminiResponseError(f"Profile field {field} must be an array")
    return value


def _validate_profile(profile: dict[str, Any]) -> dict[str, Any]:
    missing = [key for key in PROFILE_REQUIRED_KEYS if key not in profile]
    if missing:
        raise GeminiResponseError(
            "Profile JSON is missing required fields: " + ", ".join(missing)
        )
    _require_string(profile["schema_version"], "schema_version", allow_empty=False)
    _require_string(profile["professional_summary"], "professional_summary", allow_empty=False)

    seniority = profile["seniority"]
    if not isinstance(seniority, dict):
        raise GeminiResponseError("Profile field seniority must be an object")
    _require_string(seniority.get("level"), "seniority.level")
    _require_string(seniority.get("evidence"), "seniority.evidence")

    for index, item in enumerate(_require_list(profile["experience_areas"], "experience_areas")):
        if not isinstance(item, dict):
            raise GeminiResponseError(f"experience_areas[{index}] must be an object")
        for field in ("area", "strength", "recency", "seniority", "evidence"):
            _require_string(item.get(field), f"experience_areas[{index}].{field}")
        if item["strength"] not in _STRENGTHS:
            raise GeminiResponseError(f"Invalid experience strength at index {index}")
        if item["recency"] not in _RECENCY:
            raise GeminiResponseError(f"Invalid experience recency at index {index}")
        if item.get("years") is not None and not isinstance(
            item["years"], (int, float)
        ):
            raise GeminiResponseError(f"Invalid experience years at index {index}")

    for index, item in enumerate(_require_list(profile["industries"], "industries")):
        if not isinstance(item, dict):
            raise GeminiResponseError(f"industries[{index}] must be an object")
        for field in ("industry", "strength", "recency", "depth", "evidence"):
            _require_string(item.get(field), f"industries[{index}].{field}")
        if item["strength"] not in _STRENGTHS:
            raise GeminiResponseError(f"Invalid industry strength at index {index}")
        if item["recency"] not in _RECENCY or item["depth"] not in _DEPTH:
            raise GeminiResponseError(f"Invalid industry metadata at index {index}")
        if item.get("years") is not None and not isinstance(
            item["years"], (int, float)
        ):
            raise GeminiResponseError(f"Invalid industry years at index {index}")

    for index, item in enumerate(_require_list(profile["skills"], "skills")):
        if not isinstance(item, dict):
            raise GeminiResponseError(f"skills[{index}] must be an object")
        for field in ("skill", "category", "strength", "evidence"):
            _require_string(item.get(field), f"skills[{index}].{field}")
        if item["category"] not in _SKILL_CATEGORIES or item["strength"] not in _STRENGTHS | {"exposure"}:
            raise GeminiResponseError(f"Invalid skill metadata at index {index}")

    for index, item in enumerate(
        _require_list(
            profile["technologies_tools_methodologies"],
            "technologies_tools_methodologies",
        )
    ):
        if not isinstance(item, dict):
            raise GeminiResponseError(
                f"technologies_tools_methodologies[{index}] must be an object"
            )
        for field in ("name", "type", "proficiency", "evidence"):
            _require_string(
                item.get(field),
                f"technologies_tools_methodologies[{index}].{field}",
            )
        if item["type"] not in _TECHNOLOGY_TYPES or item["proficiency"] not in _PROFICIENCY:
            raise GeminiResponseError(f"Invalid technology metadata at index {index}")

    for index, item in enumerate(
        _require_list(
            profile["leadership_and_responsibility"],
            "leadership_and_responsibility",
        )
    ):
        if not isinstance(item, dict):
            raise GeminiResponseError(
                f"leadership_and_responsibility[{index}] must be an object"
            )
        for field in ("area", "strength", "evidence"):
            _require_string(
                item.get(field),
                f"leadership_and_responsibility[{index}].{field}",
            )
        if item["strength"] not in _STRENGTHS | {"evidence_only"}:
            raise GeminiResponseError(f"Invalid leadership metadata at index {index}")

    for index, item in enumerate(_require_list(profile["strengths"], "strengths")):
        if not isinstance(item, dict):
            raise GeminiResponseError(f"strengths[{index}] must be an object")
        _require_string(item.get("area"), f"strengths[{index}].area")
        _require_string(item.get("reason"), f"strengths[{index}].reason")

    for index, item in enumerate(
        _require_list(profile["gaps_or_limited_evidence"], "gaps_or_limited_evidence")
    ):
        if not isinstance(item, dict):
            raise GeminiResponseError(
                f"gaps_or_limited_evidence[{index}] must be an object"
            )
        for field in ("area", "assessment", "reason"):
            _require_string(item.get(field), f"gaps_or_limited_evidence[{index}].{field}")
        if item["assessment"] not in _GAP_ASSESSMENTS:
            raise GeminiResponseError(f"Invalid gap assessment at index {index}")

    for index, item in enumerate(
        _require_list(
            profile["transferable_capabilities"],
            "transferable_capabilities",
        )
    ):
        if not isinstance(item, dict):
            raise GeminiResponseError(
                f"transferable_capabilities[{index}] must be an object"
            )
        for field in ("capability", "evidence", "potential_applications"):
            if field == "potential_applications":
                applications = _require_list(
                    item.get(field),
                    f"transferable_capabilities[{index}].{field}",
                )
                if not all(isinstance(application, str) for application in applications):
                    raise GeminiResponseError(
                        f"Invalid transferable applications at index {index}"
                    )
            else:
                _require_string(
                    item.get(field),
                    f"transferable_capabilities[{index}].{field}",
                )
    return profile


def generate_user_profile(
    cv_text: str,
    existing_profile_text: str | None = None,
    *,
    api_key: str | None = None,
    model_name: str | None = None,
    timeout: float = 60.0,
    transport: Transport | None = None,
) -> dict[str, Any]:
    """Generate and validate an evidence-based profile from CV text.

    ``transport`` is injectable so tests never call Gemini. A failure is raised
    before any database update can occur.
    """
    if not cv_text or not cv_text.strip():
        raise GeminiConfigurationError("Cannot generate a profile without CV text")
    key = api_key or os.environ.get("GEMINI_API_KEY")
    if not key:
        raise GeminiConfigurationError(
            "GEMINI_API_KEY is not configured in the environment"
        )
    selected_model = model_name or os.environ.get("GEMINI_MODEL", DEFAULT_MODEL)
    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": _profile_prompt(cv_text, existing_profile_text)}],
            }
        ],
        "generationConfig": {
            "temperature": 0.1,
            "responseMimeType": "application/json",
        },
    }
    caller = transport or _default_transport
    response = caller(key, selected_model, payload, timeout)
    profile = _validate_profile(_parse_json(_response_text(response)))
    return {
        "profile_text": profile["professional_summary"].strip(),
        "profile_json": profile,
        "profile_model": selected_model,
        "profile_version": PROFILE_VERSION,
        "profile_source_hash": cv_source_hash(cv_text),
    }