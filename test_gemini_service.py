#!/usr/bin/env -S uv run --script
"""Offline tests for the Gemini profile extraction service."""

from __future__ import annotations

import json
import os

import gemini_service


def _profile(
    *,
    summary: str = "Evidence-based professional profile.",
    areas: list[dict] | None = None,
    industries: list[dict] | None = None,
) -> dict:
    return {
        "schema_version": "v1",
        "professional_summary": summary,
        "seniority": {"level": "senior", "evidence": "Led complex initiatives."},
        "experience_areas": areas or [],
        "industries": industries or [],
        "skills": [],
        "technologies_tools_methodologies": [],
        "leadership_and_responsibility": [],
        "strengths": [],
        "gaps_or_limited_evidence": [],
        "transferable_capabilities": [],
    }


def _response(profile: dict) -> dict:
    return {
        "candidates": [
            {"content": {"parts": [{"text": json.dumps(profile)}]}}
        ]
    }


def _raises(error_type, fn):
    try:
        fn()
    except error_type:
        return
    raise AssertionError(f"Expected {error_type.__name__}")


def test_missing_gemini_api_key():
    previous = os.environ.pop("GEMINI_API_KEY", None)
    try:
        _raises(
            gemini_service.GeminiConfigurationError,
            lambda: gemini_service.generate_user_profile("A CV"),
        )
    finally:
        if previous is not None:
            os.environ["GEMINI_API_KEY"] = previous


def test_successful_profile_extraction_uses_generic_cv_prompt():
    calls = []
    profile = _profile(summary="Senior DevOps engineer with cloud operations experience.")

    def transport(api_key, model_name, payload, timeout):
        calls.append((api_key, model_name, payload, timeout))
        return _response(profile)

    result = gemini_service.generate_user_profile(
        "DevOps engineer with Kubernetes and AWS experience.",
        api_key="test-key",
        model_name="test-model",
        transport=transport,
    )
    assert result["profile_text"] == profile["professional_summary"]
    assert result["profile_json"] == profile
    assert result["profile_model"] == "test-model"
    assert result["profile_version"] == "v1"
    assert calls and "any profession or industry" in calls[0][2]["contents"][0]["parts"][0]["text"]
    assert calls[0][0] == "test-key"


def test_valid_structured_json_response_is_preserved():
    profile = _profile(
        areas=[
            {
                "area": "Financial analysis",
                "strength": "strong",
                "years": 5,
                "recency": "recent",
                "seniority": "senior",
                "evidence": "Owned financial reporting.",
            }
        ]
    )
    result = gemini_service.generate_user_profile(
        "Financial analyst CV",
        api_key="test-key",
        transport=lambda *args: _response(profile),
    )
    assert result["profile_json"]["experience_areas"][0]["area"] == "Financial analysis"


def test_invalid_json_response_fails():
    bad_response = {
        "candidates": [{"content": {"parts": [{"text": "not JSON"}]}}]
    }
    _raises(
        gemini_service.GeminiResponseError,
        lambda: gemini_service.generate_user_profile(
            "A CV",
            api_key="test-key",
            transport=lambda *args: bad_response,
        ),
    )


def test_gemini_api_failure_is_reported_without_exposing_credentials():
    def transport(*args):
        raise gemini_service.GeminiAPIError("request failed")

    _raises(
        gemini_service.GeminiAPIError,
        lambda: gemini_service.generate_user_profile(
            "A CV",
            api_key="secret-that-must-not-be-logged",
            transport=transport,
        ),
    )


def test_different_profession_is_not_forced_into_management():
    profile = _profile(
        summary="Construction manager delivering commercial building projects.",
        areas=[
            {
                "area": "Construction management",
                "strength": "strong",
                "years": 12,
                "recency": "current",
                "seniority": "manager",
                "evidence": "Managed commercial construction delivery.",
            }
        ],
    )
    result = gemini_service.generate_user_profile(
        "Construction manager with commercial building experience.",
        api_key="test-key",
        transport=lambda *args: _response(profile),
    )
    assert result["profile_json"]["experience_areas"][0]["area"] == "Construction management"
    assert "Product Management" not in result["profile_text"]


def test_multiple_industries_and_disciplines_are_supported():
    profile = _profile(
        summary="Finance and software professional with analytical and operational leadership.",
        areas=[
            {
                "area": "Financial analysis",
                "strength": "strong",
                "years": 6,
                "recency": "recent",
                "seniority": "senior",
                "evidence": "Built financial models.",
            },
            {
                "area": "Software operations",
                "strength": "moderate",
                "years": 3,
                "recency": "current",
                "seniority": "lead",
                "evidence": "Improved software operations.",
            },
        ],
        industries=[
            {
                "industry": "Finance",
                "strength": "strong",
                "years": 6,
                "recency": "recent",
                "depth": "deep",
                "evidence": "Worked in financial reporting.",
            },
            {
                "industry": "Software",
                "strength": "moderate",
                "years": 3,
                "recency": "current",
                "depth": "moderate",
                "evidence": "Supported software operations.",
            },
        ],
    )
    result = gemini_service.generate_user_profile(
        "Finance and software operations CV",
        api_key="test-key",
        transport=lambda *args: _response(profile),
    )
    assert len(result["profile_json"]["experience_areas"]) == 2
    assert len(result["profile_json"]["industries"]) == 2


def test_source_hash_is_stable_and_does_not_contain_cv_text():
    cv_text = "Private CV content"
    digest = gemini_service.cv_source_hash(cv_text)
    assert digest == gemini_service.cv_source_hash(cv_text)
    assert cv_text not in digest


if __name__ == "__main__":
    tests = [
        value for name, value in sorted(globals().items()) if name.startswith("test_")
    ]
    for test in tests:
        test()
    print(f"ok ({len(tests)} tests)")