# Databricks notebook source
# pdf
#
# PDF extraction helpers using PyMuPDF (fitz).
# Usage: %run ../../utils/pdf
#
# Requires PyMuPDF to be installed in the cluster:
#   %pip install PyMuPDF

import subprocess
import sys

try:
    import fitz
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "PyMuPDF"])
    import fitz


def extract_pdf_text(path: str) -> str:
    """Extract all text from a PDF file, joining pages with newlines.

    Returns an empty string if the file has no text content.
    """
    doc = fitz.open(path)
    try:
        return "\n".join(page.get_text("text") or "" for page in doc)
    finally:
        doc.close()


def extract_pdf_pages(path: str) -> list[str]:
    """Extract text from a PDF file, returning one string per page.

    Useful when page boundaries matter for parsing (e.g. FGTS extracts).
    """
    doc = fitz.open(path)
    try:
        return [page.get_text("text") or "" for page in doc]
    finally:
        doc.close()
