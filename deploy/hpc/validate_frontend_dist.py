#!/usr/bin/env python3
"""Validate the pre-built frontend shipped with an HPC deployment."""

from __future__ import annotations

import argparse
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit


class _ReferenceParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.references: list[str] = []

    def handle_starttag(
        self,
        _tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        for name, value in attrs:
            if name.lower() in {"src", "href"} and value:
                self.references.append(value.strip())


def _local_reference(value: str) -> str | None:
    if not value or value.startswith(("#", "//")):
        return None

    parsed = urlsplit(value)
    if parsed.scheme or parsed.netloc:
        return None

    path = unquote(parsed.path).lstrip("/")
    return path or None


def validate_frontend_dist(dist_dir: Path) -> tuple[Path, ...]:
    """Return referenced local files or raise for an incomplete build."""

    dist_dir = dist_dir.resolve()
    index_path = dist_dir / "index.html"
    if not index_path.is_file() or index_path.stat().st_size == 0:
        raise ValueError(f"Missing or empty frontend entry point: {index_path}")

    try:
        index_text = index_path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"Frontend entry point is not valid UTF-8: {index_path}") from exc

    parser = _ReferenceParser()
    parser.feed(index_text)

    referenced_files: list[Path] = []
    has_javascript_entry = False
    for reference in parser.references:
        relative_value = _local_reference(reference)
        if relative_value is None:
            continue

        relative_path = Path(relative_value)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise ValueError(
                f"Unsafe local frontend reference {reference!r} in {index_path}"
            )

        asset_path = (dist_dir / relative_path).resolve()
        try:
            asset_path.relative_to(dist_dir)
        except ValueError as exc:
            raise ValueError(
                f"Frontend reference escapes the dist directory: {reference!r}"
            ) from exc

        if not asset_path.is_file() or asset_path.stat().st_size == 0:
            raise ValueError(
                f"Missing or empty frontend asset referenced by index.html: {asset_path}"
            )
        referenced_files.append(asset_path)
        if asset_path.suffix.lower() in {".js", ".mjs"}:
            has_javascript_entry = True

    if not has_javascript_entry:
        raise ValueError(
            f"Frontend entry point does not reference a local JavaScript bundle: {index_path}"
        )
    return tuple(referenced_files)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dist_dir", type=Path)
    args = parser.parse_args()

    referenced_files = validate_frontend_dist(args.dist_dir)
    print(
        "frontend build: OK "
        f"({len(referenced_files)} referenced local assets validated)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
