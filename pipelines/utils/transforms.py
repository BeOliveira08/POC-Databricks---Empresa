# Databricks notebook source
# transforms
#
# Reusable PySpark Column transformation helpers.
# Usage: %run ../../utils/transforms
#
# These functions operate on Spark Column expressions and are safe to use
# in any Bronze/Silver/Gold notebook. All handle None/null gracefully.

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    TimestampType,
)


def _repair_mojibake_value(value):
    """Try to repair common UTF-8/Latin-1 mojibake without changing clean text."""
    if value is None:
        return None

    text = str(value)
    if not text:
        return text

    suspicious_tokens = ("\u00c3", "\u00c2", "\ufffd")
    if not any(token in text for token in suspicious_tokens):
        return text

    def _score(candidate: str) -> tuple[int, int]:
        suspicious = sum(candidate.count(token) for token in suspicious_tokens)
        cleaned = sum(ch.isalnum() for ch in candidate)
        return suspicious, -cleaned

    candidates = [text]
    for source_encoding in ("latin-1", "cp1252"):
        try:
            repaired = text.encode(source_encoding).decode("utf-8")
            if repaired:
                candidates.append(repaired)
        except Exception:
            pass

    best = min(candidates, key=_score)
    return best


_repair_mojibake_udf = F.udf(_repair_mojibake_value, StringType())


def norm_text(col_name: str):
    """Normalize a text column: UPPER, trim, collapse internal whitespace.

    Example: "  foo   bar  " -> "FOO BAR"
    """
    return F.upper(
        F.trim(
            F.regexp_replace(
                F.coalesce(F.col(col_name).cast("string"), F.lit("")),
                r"\s+",
                " ",
            )
        )
    )


def repair_text(col_name: str):
    """Repair common mojibake patterns before downstream normalization."""
    return _repair_mojibake_udf(F.col(col_name).cast("string"))


def exclusividade_tipo_expr(col_name: str):
    """Classify exclusivity clause text into a compact business category.

    Categories:
    - EXCLUSIVO: explicit exclusivity during the contract
    - NAO_EXCLUSIVO: explicit non-exclusivity
    - NAO_CONCORRENCIA: non-compete / client non-solicitation restriction
    - null: no clear clause found
    """
    clause = F.lower(
        F.trim(
            F.regexp_replace(
                F.coalesce(F.col(col_name).cast("string"), F.lit("")),
                r"\s+",
                " ",
            )
        )
    )

    clause_ascii = clause
    for pattern, replacement in [
        (r"[áàâãäå]", "a"),
        (r"[éèêë]", "e"),
        (r"[íìîï]", "i"),
        (r"[óòôõö]", "o"),
        (r"[úùûü]", "u"),
        (r"[ç]", "c"),
        (r"[ñ]", "n"),
        (r"[^a-z0-9/ ]", " "),
        (r"\s+", " "),
    ]:
        clause_ascii = F.regexp_replace(clause_ascii, pattern, replacement)

    clause_ascii = (
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(
                            F.regexp_replace(clause_ascii, r"v.nculo", "vinculo"),
                            r"empregat.cio",
                            "empregaticio",
                        ),
                        r"n.o",
                        "nao",
                    ),
                    r"servi.os",
                    "servicos",
                ),
                r"vig.ncia",
                "vigencia",
            ),
            r"concorr.ncia",
            "concorrencia",
        )
    )

    return (
        F.when(clause_ascii == "", F.lit(None).cast("string"))
         .when(
             clause_ascii.rlike(
                 r"sem exclusividade|"
                 r"nao apresenta carater exclusivo|"
                 r"nao apresenta caracter exclusivo|"
                 r"nao exclusividade|"
                 r"sem que haja qualquer vinculo trabalhista ou de exclusividade|"
                 r"com ou sem exclusividade|"
                 r"facultado a contratada firmar quaisquer outros contratos|"
                 r"podendo a contratada prestar servicos a outras empresas|"
                 r"nao confere a qualquer das partes qualquer direito de preferencia ou de exclusividade|"
                 r"nao ha exclusividade|"
                 r"nao confere.*exclusividade"
             ),
             F.lit("NAO_EXCLUSIVO"),
         )
         .when(
             clause_ascii.rlike(
                 r"manter a exclusividade do vinculo empregaticio|"
                 r"assegurar a total exclusividade|"
                 r"total exclusividade com respeito a prestacao dos servicos|"
                 r"nao prestara quaisquer servicos a qualquer terceiro|"
                 r"manter exclusividade|"
                 r"carater exclusivo"
             ),
             F.lit("EXCLUSIVO"),
         )
         .when(
             clause_ascii.rlike(
                 r"nao concorrer|"
                 r"nao competira|"
                 r"nao competira com a contratante|"
                 r"nao oferecer os seus servicos|"
                 r"nao prospectar|"
                 r"nao firmar contrato.*clientes da contratante|"
                 r"clientes da contratante.*teve os servicos vinculados|"
                 r"todo o prazo contratual"
             ),
             F.lit("NAO_CONCORRENCIA"),
         )
         .otherwise(F.lit(None).cast("string"))
    )


def exclusividade_flag_expr(col_name: str):
    """Return exclusivity flag from the classified clause.

    1 = explicit exclusivity
    0 = explicit non-exclusivity
    null = non-compete only or no clear clause
    """
    tipo = exclusividade_tipo_expr(col_name)
    return (
        F.when(tipo == F.lit("EXCLUSIVO"), F.lit(1))
         .when(tipo == F.lit("NAO_EXCLUSIVO"), F.lit(0))
         .otherwise(F.lit(None).cast("int"))
    )


def nao_concorrencia_flag_expr(col_name: str):
    """Return 1 when the clause is a non-compete / non-solicitation restriction."""
    return F.when(exclusividade_tipo_expr(col_name) == F.lit("NAO_CONCORRENCIA"), F.lit(1)).otherwise(F.lit(0))


def hash_key(*cols):
    """Generate a SHA-256 surrogate key from one or more columns.

    Columns are concatenated with '||' as separator. Nulls become empty strings.

    Example: hash_key(F.col("cnpj"), F.col("competencia"))
    """
    return F.sha2(
        F.concat_ws("||", *[F.coalesce(c.cast("string"), F.lit("")) for c in cols]),
        256,
    )


def non_empty_string_expr(col_name: str):
    """Return column as string if non-empty after trimming, else null."""
    return F.when(
        F.trim(F.col(col_name).cast("string")) != "",
        F.col(col_name).cast("string")
    )


def only_digits_expr(col_name: str):
    """Strip all non-digit characters from a column expression."""
    return F.regexp_replace(
        F.coalesce(F.col(col_name).cast("string"), F.lit("")),
        r"\D",
        ""
    )


def normalize_cpf_expr(expr):
    """Return expression value only if exactly 11 digits, else null."""
    return F.when(F.length(expr) == 11, expr).otherwise(F.lit(None).cast("string"))


def normalize_cnpj_expr(expr):
    """Return expression value only if exactly 14 digits, else null."""
    return F.when(F.length(expr) == 14, expr).otherwise(F.lit(None).cast("string"))


def first_non_empty_string_expr(available_cols: set, *candidates):
    """Return first non-empty string from candidate columns that exist.

    Args:
        available_cols: Set of column names that exist in the DataFrame
        *candidates: Column names to check in order

    Returns:
        Coalesce expression of first non-empty string from existing columns
    """
    cols = existing_columns(available_cols, *candidates)
    if not cols:
        return F.lit(None).cast("string")
    return F.coalesce(*[non_empty_string_expr(col_name) for col_name in cols])


def first_cast_expr(available_cols: set, dtype: str, *candidates):
    """Return first non-null value from candidate columns, cast to dtype.

    Args:
        available_cols: Set of column names that exist in the DataFrame
        dtype: Target data type (e.g., "int", "double", "date")
        *candidates: Column names to check in order
    """
    cols = existing_columns(available_cols, *candidates)
    if not cols:
        return F.lit(None).cast(dtype)
    return F.coalesce(*[F.col(col_name).cast(dtype) for col_name in cols])


def first_digits_expr(available_cols: set, *candidates):
    """Return first non-empty string with only digits from candidate columns."""
    return F.regexp_replace(
        first_non_empty_string_expr(available_cols, *candidates),
        r"\D",
        ""
    )


def truncate_to_month(col_expr):
    """Truncate a date/timestamp column expression to the first day of its month."""
    return F.to_date(F.date_trunc("month", col_expr))


# Short aliases used in silver/gold notebooks
def only_digits(col_name: str):
    return only_digits_expr(col_name)


def normalize_cpf(expr):
    return normalize_cpf_expr(expr)


def normalize_cnpj(expr):
    return normalize_cnpj_expr(expr)


def fill_silver_nulls(
    df,
    string_default: str = "NAO_INFORMADO",
    numeric_default: float = 0,
    bool_default: bool = False,
    date_default: str = "1900-01-01",
    timestamp_default: str = "1900-01-01 00:00:00",
):
    """Replace nulls in a silver DataFrame using consistent type-based defaults."""
    for field in df.schema.fields:
        name = field.name
        dtype = field.dataType

        if isinstance(dtype, StringType):
            df = df.withColumn(name, F.coalesce(F.col(name), F.lit(string_default)))
        elif isinstance(dtype, (DoubleType, FloatType, DecimalType)):
            df = df.withColumn(name, F.coalesce(F.col(name), F.lit(float(numeric_default)).cast(dtype)))
        elif isinstance(dtype, (ByteType, ShortType, IntegerType, LongType)):
            df = df.withColumn(name, F.coalesce(F.col(name), F.lit(int(numeric_default)).cast(dtype)))
        elif isinstance(dtype, BooleanType):
            df = df.withColumn(name, F.coalesce(F.col(name), F.lit(bool_default)))
        elif isinstance(dtype, DateType):
            df = df.withColumn(name, F.coalesce(F.col(name), F.to_date(F.lit(date_default))))
        elif isinstance(dtype, TimestampType):
            df = df.withColumn(name, F.coalesce(F.col(name), F.to_timestamp(F.lit(timestamp_default))))

    return df
