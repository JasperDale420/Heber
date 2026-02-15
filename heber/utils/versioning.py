"""Semantic version parsing and sorting utilities."""

from __future__ import annotations

import re


def version_sort_key(version: str) -> tuple[int, int, int, int, str]:
    """Sort key for version strings, preferring semantic versions.

    Parses version strings like 'v1.2.3' into sortable tuples.
    Non-semver strings sort before all semver strings.

    Args:
        version: Version string (e.g. 'v1.2.3', '2.0.0', 'latest')

    Returns:
        Tuple suitable for use as a sort key
    """
    clean = version.strip()
    semver_match = re.match(r"^v?(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$", clean)
    if semver_match:
        major, minor, patch = semver_match.groups()
        return (1, int(major), int(minor), int(patch), clean)
    return (0, 0, 0, 0, clean)
