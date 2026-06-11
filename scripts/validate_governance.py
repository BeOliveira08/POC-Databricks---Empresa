#!/usr/bin/env python3
"""
validate_governance.py

Checks that every column declared in active pipeline notebooks has a
corresponding entry in pipelines/data_governance/catalog_metadata.py.

Exit 0 = all good
Exit 1 = missing columns found
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
NOTEBOOKS_DIR = ROOT / "pipelines" / "notebooks"
METADATA_FILE = ROOT / "pipelines" / "data_governance" / "catalog_metadata.py"
METADATA_TEMPLATES_FILE = ROOT / "pipelines" / "data_governance" / "metadata_templates.py"
SKIP_NOTEBOOK_DIRS = {"_frozen", "setup"}

# Matches: CREATE TABLE IF NOT EXISTS catalog.schema.table_name (
CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"[\w\$\{\}\.]+?\.(\w+)\s*\(",
    re.IGNORECASE,
)

CREATE_TABLE_VAR_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?\{(\w+)\}\s*\(",
    re.IGNORECASE,
)

COLUMN_DEF_RE = re.compile(r"^\s{4}(\w+)\s+\w+", re.MULTILINE)

TABLE_VAR_RE = re.compile(
    r"^(\w+)\s*=\s*f?[\"']\{CATALOG\}\.\{(?:BRONZE|SILVER|GOLD)\}\.(\w+)[\"']",
    re.MULTILINE,
)

METADATA_TABLE_CALL_RE = re.compile(
    r"_resolve\(((gold|silver|bronze)_table)\(\s*\"(\w+)\"",
    re.MULTILINE,
)
METADATA_COLUMN_RE = re.compile(r'col\("(\w+)"')
METADATA_HELPER_RE = re.compile(r"\b([A-Z_]+)\b")


def extract_notebook_schemas(notebooks_dir: Path) -> dict[str, set[str]]:
    """Returns {table_name: {col1, col2, ...}} from active CREATE TABLE statements."""
    schemas: dict[str, set[str]] = {}

    for notebook in notebooks_dir.rglob("*.py"):
        if SKIP_NOTEBOOK_DIRS.intersection(notebook.parts):
            continue

        content = notebook.read_text(encoding="utf-8", errors="ignore")
        table_vars = {
            match.group(1): match.group(2).lower()
            for match in TABLE_VAR_RE.finditer(content)
        }

        for match in CREATE_TABLE_RE.finditer(content):
            table_name = match.group(1).lower()
            block_start = match.end()

            depth = 1
            pos = block_start
            while pos < len(content) and depth > 0:
                if content[pos] == "(":
                    depth += 1
                elif content[pos] == ")":
                    depth -= 1
                pos += 1

            block = content[block_start : pos - 1]
            cols = {m.group(1).lower() for m in COLUMN_DEF_RE.finditer(block)}
            if cols:
                schemas.setdefault(table_name, set()).update(cols)

        for match in CREATE_TABLE_VAR_RE.finditer(content):
            table_var = match.group(1)
            table_name = table_vars.get(table_var)
            if not table_name:
                continue

            block_start = match.end()
            depth = 1
            pos = block_start
            while pos < len(content) and depth > 0:
                if content[pos] == "(":
                    depth += 1
                elif content[pos] == ")":
                    depth -= 1
                pos += 1

            block = content[block_start : pos - 1]
            cols = {m.group(1).lower() for m in COLUMN_DEF_RE.finditer(block)}
            if cols:
                schemas.setdefault(table_name, set()).update(cols)

    return schemas


def extract_metadata_coverage(metadata_file: Path) -> dict[str, set[str]]:
    """Returns {table_name: {col1, col2, ...}} from catalog_metadata.py."""
    content = metadata_file.read_text(encoding="utf-8")
    coverage: dict[str, set[str]] = {}
    helper_columns = load_metadata_template_columns(METADATA_TEMPLATES_FILE)

    for match in METADATA_TABLE_CALL_RE.finditer(content):
        table_type = match.group(1)
        table_name = match.group(3).lower()
        block_start = match.end()

        depth = 1
        pos = block_start
        while pos < len(content) and depth > 0:
            if content[pos] == "(":
                depth += 1
            elif content[pos] == ")":
                depth -= 1
            pos += 1

        block = content[block_start : pos - 1]
        cols = {m.group(1).lower() for m in METADATA_COLUMN_RE.finditer(block)}
        for helper_name in METADATA_HELPER_RE.findall(block):
            cols.update(helper_columns.get(helper_name, set()))
        if table_type in {"gold_table", "silver_table"}:
            cols.update(helper_columns.get("AUDIT_COLUMNS", set()))
        coverage[table_name] = cols

    return coverage


def load_metadata_template_columns(metadata_templates_file: Path) -> dict[str, set[str]]:
    """Load reusable helper column names from metadata_templates.py."""
    namespace: dict[str, object] = {}
    exec(metadata_templates_file.read_text(encoding="utf-8"), namespace)

    helpers: dict[str, set[str]] = {}
    for name, value in namespace.items():
        if not name.isupper() or not isinstance(value, list):
            continue

        cols = {
            item["name"].lower()
            for item in value
            if isinstance(item, dict) and "name" in item
        }
        if cols:
            helpers[name] = cols

    return helpers


def main() -> int:
    notebook_schemas = extract_notebook_schemas(NOTEBOOKS_DIR)
    metadata_coverage = extract_metadata_coverage(METADATA_FILE)

    errors: list[str] = []

    for table, cols in sorted(notebook_schemas.items()):
        governed_cols = metadata_coverage.get(table, set())

        if table in metadata_coverage and not governed_cols:
            continue

        missing = cols - governed_cols
        if missing:
            errors.append(f"  {table}: missing columns -> {', '.join(sorted(missing))}")

    if errors:
        print("FAIL: Governance validation failed. Missing column docs:\n")
        for err in errors:
            print(err)
        print("\nAdd the missing columns to pipelines/data_governance/catalog_metadata.py")
        return 1

    print(f"OK: Governance coverage - {len(notebook_schemas)} tables checked, all columns documented.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
