# CI/CD

## Pull Request Checks

Every pull request targeting `development` or `main` runs the following jobs in parallel:

| Job | Tool | What it checks |
|---|---|---|
| `formatting` | `black --check ./pipelines` | Code formatting |
| `lint` | `flake8` (max 120 chars, complexity 20) | Code quality |
| `secrets` | `detect-secrets` | Hardcoded credentials/PII |
| `run-refs` | `scripts/validate_run_refs.py` | Broken `%run` notebook references |
| `governance` | `scripts/validate_governance.py` | Column docs coverage for active notebooks in `catalog_metadata.py` |

All jobs run independently.

## Data Quality Check

Triggered on PRs that modify any file under `pipelines/notebooks/**` and target `development` or `main`.

Queries each affected table in the Databricks SQL Warehouse and posts a comment on the PR:

| Check | Description |
|---|---|
| Row count | Total rows in the table |
| Duplicates | Rows with repeated PK |
| Nulls (PK) | Null values in the key column |

Bronze tables only get row count. The CI step fails if duplicates or null PKs are found in Silver/Gold tables.

If the warehouse is offline or `DATABRICKS_WAREHOUSE_ID` is not set, the step posts a warning comment and passes.

## Deploy to Production

Production deploy remains tied to pushes on `main`:

1. `databricks bundle validate -t prod`
2. `databricks bundle deploy -t prod --auto-approve`

Governance is applied at runtime by the active Silver/Gold notebooks, including the `apply_governance` task in `gold_pipeline`.

## Workflows

| File | Trigger | Purpose |
|---|---|---|
| `data-pr-tests.yml` | PR -> `development` or `main` | Parallel code quality checks |
| `data-quality-check.yml` | PR -> `development` or `main` (notebooks only) | Data quality checks + PR comment |
| `validate.yml` | PR -> `development` or `main` | Bundle validation |
| `deploy-prod.yml` | Push -> `main` | Production deploy |
