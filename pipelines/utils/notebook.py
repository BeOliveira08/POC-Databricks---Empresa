# Databricks notebook source
# notebook
#
# Notebook-related utility functions.
# Usage: %run ../../utils/notebook


def current_notebook_dir():
    """Get the directory path of the currently running notebook.

    Dynamically retrieves the path from Databricks context.
    Falls back to NOTEBOOKS_BASE from project_params if not in Databricks.
    """
    try:
        path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        return path.rsplit("/", 1)[0]
    except Exception:
        # Fallback: use configured base path
        notebooks_base = globals().get("NOTEBOOKS_BASE", "/Workspace/Shared/portfolio-lakehouse-template")
        return notebooks_base
