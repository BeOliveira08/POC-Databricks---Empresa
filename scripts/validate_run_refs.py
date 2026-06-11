#!/usr/bin/env python3
"""
validate_run_refs.py

Checks that every %run reference in Databricks notebooks points to a file
that actually exists in the repository.

Exit 0 = all good
Exit 1 = broken references found
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
PIPELINES_DIR = ROOT / "pipelines"

# Only match %run at line start or after # MAGIC — avoids false positives in comments
RUN_RE = re.compile(r'(?:^#\s*MAGIC\s+%run|^%run)\s+(\S+)', re.MULTILINE)


def resolve_ref(notebook: Path, ref: str) -> Path:
    """Resolve a %run path relative to the notebook's directory."""
    base = notebook.parent
    target = (base / ref).resolve()
    # Databricks strips .py extension — if path doesn't exist and has no suffix, try adding .py
    if not target.exists() and not target.suffix:
        return target.with_suffix(".py")
    return target


def main() -> int:
    errors: list[str] = []

    for notebook in PIPELINES_DIR.rglob("*.py"):
        if "silver_legado" in notebook.parts:
            continue
        if "utils" in notebook.parts:
            continue  # utils files are libraries, not notebooks

        content = notebook.read_text(encoding="utf-8", errors="ignore")

        for match in RUN_RE.finditer(content):
            ref = match.group(1)
            target = resolve_ref(notebook, ref)

            if not target.exists():
                rel_notebook = notebook.relative_to(ROOT)
                errors.append(f"  {rel_notebook}: %run {ref} -> not found ({target})")

    if errors:
        print("FAIL: Broken %run references:\n")
        for err in errors:
            print(err)
        return 1

    print(f"OK: %run references — all targets exist.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
