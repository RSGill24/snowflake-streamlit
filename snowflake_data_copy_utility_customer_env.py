import streamlit as st
from snowflake.snowpark.context import get_active_session
import uuid
import json
import math
import re
from datetime import datetime, date, time
from decimal import Decimal

import pandas as pd
import numpy as np


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Snowflake Data Copy Utility",
    page_icon="❄️",
    layout="wide"
)


# ============================================================
# APPLICATION CONFIGURATION
# ============================================================

# Application/control location only.
# Customer source and target databases are NOT hardcoded.

APP_DATABASE = "DATA_COPY_APP_DB"
APP_SCHEMA = "STREAMLIT"

# Maximum number of rows loaded into the editor at one time.
DEFAULT_EDIT_ROWS = 100

# Azure Blob / Snowflake external-stage defaults. These values can be changed
# in the Streamlit UI so the application is not tied to one customer storage account.
DEFAULT_AZURE_STAGE = f"{APP_DATABASE}.{APP_SCHEMA}.AZURE_PHARMACY_STAGE"
DEFAULT_AZURE_FILE_FORMAT = f"{APP_DATABASE}.{APP_SCHEMA}.CSV_FILE_FORMAT_READ"
DEFAULT_AZURE_HEADER_FILE_FORMAT = f"{APP_DATABASE}.{APP_SCHEMA}.CSV_FILE_FORMAT_HEADER"
DEFAULT_AZURE_PIPE_FILE_FORMAT = f"{APP_DATABASE}.{APP_SCHEMA}.CSV_FILE_FORMAT_PIPE_READ"


# ============================================================
# SNOWFLAKE SESSION
# ============================================================

session = get_active_session()


# ============================================================
# TITLE
# ============================================================

st.title("❄️ Snowflake Data Copy Utility")

st.caption("Mediclaim Fraud & Customer Sentiment Demo")

st.caption(
    "Parameterized Snowflake database, schema and table copy utility "
    "with source data editing"
)


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def quote_identifier(value):
    """
    Safely quote Snowflake identifiers.
    """

    return '"' + str(value).replace('"', '""') + '"'


def escape_sql_string(value):
    """
    Escape SQL string literals.
    """

    return str(value).replace("'", "''")


def ensure_regular_csv_file_format(file_format):
    """Reject a header-parsing format when scanning CSV rows directly.

    PARSE_HEADER is appropriate for INFER_SCHEMA, but not for a regular staged
    CSV scan. Keep the header-aware format dedicated to INFER_SCHEMA and use a
    separate ordinary CSV format for FROM @stage(...).
    """
    parts = [part.strip().strip('"') for part in str(file_format).split(".")]
    if not parts or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", part) for part in parts):
        raise ValueError("Enter a valid, fully qualified regular CSV file format name.")
    qualified = ".".join(quote_identifier(part) for part in parts)
    try:
        description = session.sql(f"DESC FILE FORMAT {qualified}").to_pandas()
    except Exception as exc:
        raise ValueError(f"Could not inspect the regular CSV file format {file_format}: {exc}") from exc
    prop_col = next((c for c in description.columns if str(c).lower() == "property"), None)
    value_col = next((c for c in description.columns if str(c).lower() == "value"), None)
    if prop_col and value_col:
        for _, row in description.iterrows():
            if str(row[prop_col]).upper() == "PARSE_HEADER" and str(row[value_col]).upper() == "TRUE":
                raise ValueError(
                    f"The regular File Format '{file_format}' has PARSE_HEADER=TRUE. "
                    "Use CSV_FILE_FORMAT_READ (PARSE_HEADER=FALSE) in the Regular CSV File Format field "
                    "for reading staged rows, and CSV_FILE_FORMAT_HEADER (PARSE_HEADER=TRUE) "
                    "only in the Header-aware File Format (INFER_SCHEMA) field."
                )


def full_name(database, schema, table=None):

    if table:

        return (
            f"{quote_identifier(database)}."
            f"{quote_identifier(schema)}."
            f"{quote_identifier(table)}"
        )

    return (
        f"{quote_identifier(database)}."
        f"{quote_identifier(schema)}"
    )


def normalize_value(value):
    """
    Convert pandas / Python values into values suitable for
    Snowflake comparison and binding.
    """

    if value is None:
        return None

    try:

        if pd.isna(value):
            return None

    except Exception:
        pass

    if isinstance(value, pd.Timestamp):

        if pd.isna(value):
            return None

        return value.to_pydatetime()

    if isinstance(value, datetime):

        return value

    if isinstance(value, date):

        return value

    if isinstance(value, time):

        return value

    if isinstance(value, Decimal):

        return value

    if isinstance(value, bool):

        return value

    # Snowpark bind parameters do not accept NumPy scalar types
    # such as numpy.int8 / numpy.int64 directly. Streamlit's
    # data_editor commonly returns NumPy scalars for numeric cells.
    # Convert them to native Python types before binding.
    if isinstance(value, np.integer):

        return int(value)

    if isinstance(value, np.floating):

        if np.isnan(value):

            return None

        return float(value)

    if isinstance(value, np.bool_):

        return bool(value)

    if isinstance(value, (int, float)):

        if isinstance(value, float) and math.isnan(value):

            return None

        return value

    return value


# ============================================================
# DATABASE DISCOVERY
# ============================================================

@st.cache_data(ttl=300)
def get_databases():

    rows = session.sql(
        "SHOW DATABASES"
    ).collect()

    databases = []

    for row in rows:

        db_name = row["name"]

        # Do not expose application/control database
        if db_name.upper() != APP_DATABASE.upper():

            databases.append(db_name)

    return sorted(databases)


# ============================================================
# SCHEMA DISCOVERY
# ============================================================

@st.cache_data(ttl=300)
def get_schemas(database):

    db = quote_identifier(database)

    query = f"""
        SELECT SCHEMA_NAME
        FROM {db}.INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME <> 'INFORMATION_SCHEMA'
        ORDER BY SCHEMA_NAME
    """

    rows = session.sql(query).collect()

    return [
        row["SCHEMA_NAME"]
        for row in rows
    ]


# ============================================================
# TABLE DISCOVERY
# ============================================================

@st.cache_data(ttl=300)
def get_tables(database, schema):

    db = quote_identifier(database)

    schema_value = escape_sql_string(schema)

    query = f"""
        SELECT TABLE_NAME
        FROM {db}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema_value}'
          AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    """

    rows = session.sql(query).collect()

    return [
        row["TABLE_NAME"]
        for row in rows
    ]


# ============================================================
# COLUMN METADATA
# ============================================================

@st.cache_data(ttl=300)
def get_columns(database, schema, table):

    db = quote_identifier(database)

    schema_value = escape_sql_string(schema)
    table_value = escape_sql_string(table)

    query = f"""
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            ORDINAL_POSITION,
            IS_NULLABLE
        FROM {db}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_value}'
          AND TABLE_NAME = '{table_value}'
        ORDER BY ORDINAL_POSITION
    """

    rows = session.sql(query).collect()

    return [
        {
            "COLUMN_NAME": row["COLUMN_NAME"],
            "DATA_TYPE": row["DATA_TYPE"],
            "ORDINAL_POSITION": row["ORDINAL_POSITION"],
            "IS_NULLABLE": row["IS_NULLABLE"]
        }
        for row in rows
    ]


# ============================================================
# PRIMARY KEY DISCOVERY
# ============================================================

@st.cache_data(ttl=300)
def get_primary_key_columns(
    database,
    schema,
    table
):

    db = quote_identifier(database)

    schema_value = escape_sql_string(schema)
    table_value = escape_sql_string(table)

    query = f"""
        SELECT
            kcu.COLUMN_NAME,
            kcu.ORDINAL_POSITION
        FROM {db}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN {db}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
          ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
         AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
         AND tc.TABLE_NAME = kcu.TABLE_NAME
        WHERE tc.TABLE_SCHEMA = '{schema_value}'
          AND tc.TABLE_NAME = '{table_value}'
          AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ORDER BY kcu.ORDINAL_POSITION
    """

    try:

        rows = session.sql(query).collect()

        return [
            row["COLUMN_NAME"]
            for row in rows
        ]

    except Exception:

        return []


# ============================================================
# ROW COUNT
# ============================================================

def get_row_count(
    database,
    schema,
    table
):

    query = f"""
        SELECT COUNT(*) AS ROW_COUNT
        FROM {full_name(database, schema, table)}
    """

    return session.sql(
        query
    ).collect()[0]["ROW_COUNT"]


# ============================================================
# SCHEMA EXISTS
# ============================================================

def schema_exists(
    database,
    schema
):

    db = quote_identifier(database)

    schema_value = escape_sql_string(schema)

    query = f"""
        SELECT COUNT(*) AS CNT
        FROM {db}.INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME = '{schema_value}'
    """

    return session.sql(
        query
    ).collect()[0]["CNT"] > 0


# ============================================================
# TABLE EXISTS
# ============================================================

def table_exists(
    database,
    schema,
    table
):

    db = quote_identifier(database)

    schema_value = escape_sql_string(schema)
    table_value = escape_sql_string(table)

    query = f"""
        SELECT COUNT(*) AS CNT
        FROM {db}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema_value}'
          AND TABLE_NAME = '{table_value}'
          AND TABLE_TYPE = 'BASE TABLE'
    """

    return session.sql(
        query
    ).collect()[0]["CNT"] > 0


# ============================================================
# CREATE SCHEMA
# ============================================================

def create_schema(
    database,
    schema
):

    query = f"""
        CREATE SCHEMA IF NOT EXISTS
        {full_name(database, schema)}
    """

    session.sql(query).collect()


# ============================================================
# CREATE TABLE LIKE SOURCE
# ============================================================

def create_table_like(
    source_db,
    source_schema,
    source_table,
    target_db,
    target_schema,
    target_table
):

    source = full_name(
        source_db,
        source_schema,
        source_table
    )

    target = full_name(
        target_db,
        target_schema,
        target_table
    )

    query = f"""
        CREATE TABLE IF NOT EXISTS
        {target}
        LIKE {source}
    """

    session.sql(query).collect()


# ============================================================
# GET COMMON COLUMNS
# ============================================================

def get_common_columns(
    source_db,
    source_schema,
    source_table,
    target_db,
    target_schema,
    target_table
):

    source_columns = get_columns(
        source_db,
        source_schema,
        source_table
    )

    target_columns = get_columns(
        target_db,
        target_schema,
        target_table
    )

    target_names = {
        c["COLUMN_NAME"]
        for c in target_columns
    }

    return [
        c["COLUMN_NAME"]
        for c in source_columns
        if c["COLUMN_NAME"] in target_names
    ]


# ============================================================
# COPY TABLE DATA
# ============================================================

def copy_table_data(
    source_db,
    source_schema,
    source_table,
    target_db,
    target_schema,
    target_table,
    copy_mode
):

    source = full_name(
        source_db,
        source_schema,
        source_table
    )

    target = full_name(
        target_db,
        target_schema,
        target_table
    )

    common_columns = get_common_columns(
        source_db,
        source_schema,
        source_table,
        target_db,
        target_schema,
        target_table
    )

    if not common_columns:

        raise Exception(
            f"No common columns found between "
            f"{source_table} and {target_table}."
        )

    columns = ", ".join(
        quote_identifier(column)
        for column in common_columns
    )

    # --------------------------------------------------------
    # REPLACE
    # --------------------------------------------------------

    if copy_mode == "REPLACE":

        session.sql(
            f"TRUNCATE TABLE {target}"
        ).collect()

    # --------------------------------------------------------
    # INSERT
    # --------------------------------------------------------

    query = f"""
        INSERT INTO {target}
        (
            {columns}
        )
        SELECT
            {columns}
        FROM {source}
    """

    session.sql(query).collect()


# ============================================================
# PREVIEW TABLE
# ============================================================

def preview_table(
    database,
    schema,
    table,
    limit=10
):

    query = f"""
        SELECT *
        FROM {full_name(database, schema, table)}
        LIMIT {int(limit)}
    """

    return session.sql(
        query
    ).to_pandas()


# ============================================================
# GET EDITABLE SOURCE DATA
# ============================================================

def get_editable_data(
    database,
    schema,
    table,
    limit
):

    query = f"""
        SELECT *
        FROM {full_name(database, schema, table)}
        LIMIT {int(limit)}
    """

    return session.sql(
        query
    ).to_pandas()


# ============================================================
# BUILD NULL-SAFE WHERE CONDITION
# ============================================================

def build_null_safe_condition(
    columns,
    row,
    parameter_values
):

    conditions = []

    for column in columns:

        value = normalize_value(
            row[column]
        )

        column_sql = quote_identifier(
            column
        )

        if value is None:

            conditions.append(
                f"{column_sql} IS NULL"
            )

        else:

            conditions.append(
                f"{column_sql} = ?"
            )

            parameter_values.append(
                value
            )

    if not conditions:

        raise Exception(
            "Unable to build row identification condition."
        )

    return " AND ".join(
        conditions
    )


# ============================================================
# FIND EXACT ROW COUNT
# ============================================================

def count_matching_row(
    database,
    schema,
    table,
    columns,
    row
):

    parameter_values = []

    condition = build_null_safe_condition(
        columns,
        row,
        parameter_values
    )

    query = f"""
        SELECT COUNT(*) AS CNT
        FROM {full_name(database, schema, table)}
        WHERE {condition}
    """

    result = session.sql(
        query,
        params=parameter_values
    ).collect()

    return result[0]["CNT"]


# ============================================================
# UPDATE SOURCE ROW
# ============================================================

def update_source_row(
    database,
    schema,
    table,
    key_columns,
    original_row,
    updated_row,
    all_columns
):

    # --------------------------------------------------------
    # Determine columns that changed
    # --------------------------------------------------------

    changed_columns = []

    for column in all_columns:

        old_value = normalize_value(
            original_row[column]
        )

        new_value = normalize_value(
            updated_row[column]
        )

        if old_value != new_value:

            changed_columns.append(
                column
            )

    if not changed_columns:

        return "NO_CHANGE"

    # --------------------------------------------------------
    # Prefer primary key when available
    # --------------------------------------------------------

    if key_columns:

        match_columns = key_columns

    else:

        match_columns = all_columns

    # --------------------------------------------------------
    # Verify row uniqueness
    # --------------------------------------------------------

    matching_count = count_matching_row(
        database,
        schema,
        table,
        match_columns,
        original_row
    )

    if matching_count == 0:

        raise Exception(
            "The original row could not be found. "
            "The source table may have changed."
        )

    if matching_count > 1:

        raise Exception(
            f"The row is not uniquely identifiable. "
            f"{matching_count} rows match the original values. "
            f"Add a primary key to the table before editing this row."
        )

    # --------------------------------------------------------
    # Build UPDATE
    # --------------------------------------------------------

    set_clauses = []

    params = []

    for column in changed_columns:

        set_clauses.append(
            f"{quote_identifier(column)} = ?"
        )

        params.append(
            normalize_value(
                updated_row[column]
            )
        )

    where_params = []

    where_condition = build_null_safe_condition(
        match_columns,
        original_row,
        where_params
    )

    params.extend(
        where_params
    )

    query = f"""
        UPDATE {full_name(database, schema, table)}
        SET {", ".join(set_clauses)}
        WHERE {where_condition}
    """

    session.sql(
        query,
        params=params
    ).collect()

    return "UPDATED"


# ============================================================
# DELETE SOURCE ROW
# ============================================================

def delete_source_row(
    database,
    schema,
    table,
    key_columns,
    original_row,
    all_columns
):

    if key_columns:

        match_columns = key_columns

    else:

        match_columns = all_columns

    # --------------------------------------------------------
    # Verify uniqueness
    # --------------------------------------------------------

    matching_count = count_matching_row(
        database,
        schema,
        table,
        match_columns,
        original_row
    )

    if matching_count == 0:

        raise Exception(
            "The row could not be found. "
            "The source table may have changed."
        )

    if matching_count > 1:

        raise Exception(
            f"Delete cancelled because {matching_count} rows "
            f"match the selected row. "
            f"Add a primary key before deleting duplicate rows."
        )

    # --------------------------------------------------------
    # Build DELETE
    # --------------------------------------------------------

    params = []

    where_condition = build_null_safe_condition(
        match_columns,
        original_row,
        params
    )

    query = f"""
        DELETE FROM {full_name(database, schema, table)}
        WHERE {where_condition}
    """

    session.sql(
        query,
        params=params
    ).collect()

    return "DELETED"


# ============================================================
# INSERT NEW SOURCE ROW
# ============================================================

def insert_source_row(
    database,
    schema,
    table,
    row,
    columns
):

    insert_columns = []
    placeholders = []
    params = []

    for column in columns:

        value = normalize_value(
            row.get(column)
        )

        insert_columns.append(
            quote_identifier(column)
        )

        placeholders.append("?")

        params.append(
            value
        )

    query = f"""
        INSERT INTO {full_name(database, schema, table)}
        (
            {", ".join(insert_columns)}
        )
        VALUES
        (
            {", ".join(placeholders)}
        )
    """

    session.sql(
        query,
        params=params
    ).collect()

    return "INSERTED"


# ============================================================
# SAVE EDITOR CHANGES
# ============================================================

def save_source_editor_changes(
    database,
    schema,
    table,
    original_df,
    edited_df,
    columns,
    primary_key_columns
):

    results = []

    # --------------------------------------------------------
    # Make copies
    # --------------------------------------------------------

    original = original_df.copy()
    edited = edited_df.copy()

    # --------------------------------------------------------
    # Ensure column order
    # --------------------------------------------------------

    edited = edited[
        columns
    ]

    original = original[
        columns
    ]

    # --------------------------------------------------------
    # Existing rows
    # --------------------------------------------------------

    original_count = len(original)
    edited_count = len(edited)

    common_count = min(
        original_count,
        edited_count
    )

    # --------------------------------------------------------
    # Process existing rows by position
    #
    # st.data_editor preserves existing row ordering.
    # --------------------------------------------------------

    for index in range(common_count):

        original_row = original.iloc[index]
        edited_row = edited.iloc[index]

        old_values = [
            normalize_value(
                original_row[column]
            )
            for column in columns
        ]

        new_values = [
            normalize_value(
                edited_row[column]
            )
            for column in columns
        ]

        # ----------------------------------------------------
        # Deleted row
        # ----------------------------------------------------

        if index >= edited_count:

            delete_source_row(
                database,
                schema,
                table,
                primary_key_columns,
                original_row,
                columns
            )

            results.append({
                "Action": "DELETE",
                "Row": index + 1,
                "Status": "SUCCESS"
            })

            continue

        # ----------------------------------------------------
        # Changed row
        # ----------------------------------------------------

        if old_values != new_values:

            action = update_source_row(
                database,
                schema,
                table,
                primary_key_columns,
                original_row,
                edited_row,
                columns
            )

            if action != "NO_CHANGE":

                results.append({
                    "Action": "UPDATE",
                    "Row": index + 1,
                    "Status": "SUCCESS"
                })

    # --------------------------------------------------------
    # Detect rows deleted from middle/end
    #
    # Streamlit data editor can return fewer rows after
    # deletion. Compare original and edited positions.
    # --------------------------------------------------------

    if edited_count < original_count:

        for index in range(
            edited_count,
            original_count
        ):

            original_row = original.iloc[index]

            delete_source_row(
                database,
                schema,
                table,
                primary_key_columns,
                original_row,
                columns
            )

            results.append({
                "Action": "DELETE",
                "Row": index + 1,
                "Status": "SUCCESS"
            })

    # --------------------------------------------------------
    # New rows
    #
    # Dynamic rows are appended by Streamlit.
    # --------------------------------------------------------

    if edited_count > original_count:

        for index in range(
            original_count,
            edited_count
        ):

            new_row = edited.iloc[index]

            insert_source_row(
                database,
                schema,
                table,
                new_row,
                columns
            )

            results.append({
                "Action": "INSERT",
                "Row": index + 1,
                "Status": "SUCCESS"
            })

    return results


# ============================================================
# TABLE SELECTION HELPER
# ============================================================

def set_select_all_tables(source_tables):
    """Set all individual table checkboxes to the Select All value."""

    select_all = st.session_state.get("select_all_tables", False)

    for table in source_tables:
        st.session_state[f"copy_table_{table}"] = select_all


# ============================================================

# ============================================================
# EDITOR SQL PREVIEW HELPERS
# ============================================================

def sql_literal(value):
    value = normalize_value(value)
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return "'" + escape_sql_string(value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()) + "'"
    return "'" + escape_sql_string(value) + "'"


def build_editor_sql_preview(database, schema, table, original_df, edited_df, columns, key_columns):
    statements = []
    original = original_df.copy()[columns]
    edited = edited_df.copy()[columns]
    common_count = min(len(original), len(edited))
    match_columns = key_columns if key_columns else columns

    def where_for(row):
        clauses = []
        for col in match_columns:
            val = normalize_value(row[col])
            qcol = quote_identifier(col)
            clauses.append(f"{qcol} IS NULL" if val is None else f"{qcol} = {sql_literal(val)}")
        return " AND ".join(clauses)

    for i in range(common_count):
        old = original.iloc[i]
        new = edited.iloc[i]
        changed = [c for c in columns if normalize_value(old[c]) != normalize_value(new[c])]
        if changed:
            sets = ", ".join(f"{quote_identifier(c)} = {sql_literal(new[c])}" for c in changed)
            statements.append(f"UPDATE {full_name(database, schema, table)} SET {sets} WHERE {where_for(old)};")

    if len(edited) < len(original):
        for i in range(len(edited), len(original)):
            statements.append(f"DELETE FROM {full_name(database, schema, table)} WHERE {where_for(original.iloc[i])};")

    if len(edited) > len(original):
        for i in range(len(original), len(edited)):
            row = edited.iloc[i]
            cols = ", ".join(quote_identifier(c) for c in columns)
            vals = ", ".join(sql_literal(row[c]) for c in columns)
            statements.append(f"INSERT INTO {full_name(database, schema, table)} ({cols}) VALUES ({vals});")

    return "\n\n".join(statements) if statements else "-- No INSERT / UPDATE / DELETE statements will be executed."

# ============================================================
# DATA ANALYSIS / SNOWFLAKE CORTEX HELPERS
# ============================================================

DEFAULT_AI_MODEL = "openai-gpt-5"


def sql_preview(sql_text):
    """Display SQL before it is executed."""
    st.code(sql_text.strip(), language="sql")


def clean_generated_sql(text):
    """
    Extract exactly one SQL statement from Cortex output.

    Handles:
      1. BEGIN_SQL / END_SQL envelope
      2. JSON such as {"sql":"SELECT ..."}
      3. fenced SQL
      4. plain SQL with a short preamble

    The JSON case is important because AI_COMPLETE can return a model message
    containing a JSON object even when structured response_format is not used.
    """
    value = str(text or "").strip()

    if not value:
        raise ValueError("Cortex returned an empty SQL response.")

    # Normalize common escaped JSON text if the entire response itself is a
    # quoted JSON string.
    for _ in range(2):
        if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
            try:
                decoded = json.loads(value)
                if isinstance(decoded, str):
                    value = decoded.strip()
                    continue
            except Exception:
                pass
        break

    # 1) Preferred: deterministic BEGIN_SQL / END_SQL envelope.
    envelope = re.search(
        r"BEGIN_SQL\s*(.*?)\s*END_SQL",
        value,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if envelope:
        value = envelope.group(1).strip()

    # 2) JSON object with an sql field. Parse JSON BEFORE searching for SELECT
    # so quoted Snowflake identifiers such as "BILLED_AMOUNT" are not damaged.
    else:
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                candidate = (
                    parsed.get("sql")
                    or parsed.get("SQL")
                    or parsed.get("query")
                    or parsed.get("QUERY")
                )
                if candidate:
                    value = str(candidate).strip()
        except Exception:
            # If the model produced almost-valid JSON, extract the sql value
            # defensively. This is only a fallback; validation still follows.
            match = re.search(
                r'["\'](?:sql|query)["\']\s*:\s*["\'](.*)["\']\s*}\s*$',
                value,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if match:
                candidate = match.group(1)
                # Decode JSON-style escapes without changing SQL double quotes.
                try:
                    candidate = json.loads('"' + candidate.replace('"', '\\"') + '"')
                except Exception:
                    candidate = candidate.replace('\\"', '"').replace("\\n", "\n")
                value = candidate.strip()

    # 3) Prefer the contents of a fenced SQL block.
    fenced = re.search(
        r"```(?:sql)?\s*(.*?)```",
        value,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if fenced:
        value = fenced.group(1).strip()

    # Remove a leading SQL label.
    value = re.sub(
        r"^\s*(?:sql\s*:?)\s*",
        "",
        value,
        flags=re.IGNORECASE,
    ).strip()

    # Remove accidental Markdown/code-wrapper remnants.
    value = value.strip("`").strip()

    # If there is still prose before SQL, start at SELECT/WITH.
    line_start = re.search(
        r"(?im)^\s*(SELECT|WITH)\b",
        value,
    )
    if line_start:
        value = value[line_start.start():].strip()
    else:
        statement_start = re.search(
            r"\b(SELECT|WITH)\b",
            value,
            flags=re.IGNORECASE,
        )
        if not statement_start:
            raise ValueError(
                "Cortex did not return a SELECT/WITH SQL statement."
            )
        value = value[statement_start.start():].strip()

    # Remove common JSON/prose suffixes that can appear after the SQL.
    value = re.sub(r"\s*END_SQL\s*$", "", value, flags=re.IGNORECASE).strip()
    value = re.sub(r"\s*```\s*$", "", value).strip()

    # Take the first statement only. SQL generation is allowed to produce one
    # statement, and the security validator independently rejects semicolon-
    # separated multiple statements.
    semicolon = value.find(";")
    if semicolon >= 0:
        value = value[:semicolon].strip()

    # If a JSON suffix survived after an un-terminated SQL statement, remove
    # only obvious wrapper characters at the very end. Do NOT strip quotes
    # because double-quoted Snowflake identifiers are valid SQL.
    value = re.sub(r"\s*[,}]\s*$", "", value).strip()

    if not value:
        raise ValueError("Cortex returned an empty SQL statement.")

    return value + ";"


def validate_read_only_sql(sql_text):
    """Allow only a single read-only SELECT/WITH statement."""
    sql = str(sql_text or "").strip()
    normalized = re.sub(r"\s+", " ", sql.upper()).strip()

    if not normalized:
        raise ValueError("No SQL was generated.")

    if not (
        normalized.startswith("SELECT ")
        or normalized.startswith("WITH ")
        or normalized == "SELECT"
    ):
        raise ValueError(
            "Only SELECT/WITH statements are allowed in AI analysis."
        )

    forbidden = (
        r"\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|CREATE|ALTER|DROP|"
        r"GRANT|REVOKE|CALL|EXECUTE|PUT|GET|COPY|REMOVE)\b"
    )

    if re.search(forbidden, normalized):
        raise ValueError(
            "Generated SQL contains a non-read-only SQL operation."
        )

    if ";" in sql.rstrip().rstrip(";"):
        raise ValueError("Multiple SQL statements are not allowed.")

    return sql


def get_analysis_metadata(database, schema, table):
    columns = get_columns(database, schema, table)
    return "\n".join(
        f'- {c["COLUMN_NAME"]} ({c["DATA_TYPE"]}, nullable={c["IS_NULLABLE"]})'
        for c in columns
    )


def cortex_complete(model, prompt):
    """
    Call Snowflake Cortex AI_COMPLETE directly inside Snowflake.

    This is used for normal natural-language analysis/explanation responses.
    """
    query = """SELECT AI_COMPLETE(
        model => ?,
        prompt => ?,
        model_parameters => {'temperature': 0, 'max_tokens': 4096}
    ) AS RESPONSE"""

    try:
        row = session.sql(query, params=[model, prompt]).collect()[0]
    except Exception as exc:
        raise RuntimeError(
            f"Snowflake Cortex AI_COMPLETE failed: {exc}"
        ) from exc

    response = row["RESPONSE"]
    if response is None:
        raise RuntimeError("Snowflake Cortex AI_COMPLETE returned no response.")

    return str(response)


def cortex_complete_sql(model, prompt):
    """
    Generate SQL using AI_COMPLETE in normal text mode.

    We deliberately do NOT use response_format or structured output.
    show_details=TRUE is used because Snowflake documents that it returns
    choices[0].messages containing the model response. We then extract the
    BEGIN_SQL/END_SQL envelope and validate it before execution.
    """
    query = """SELECT AI_COMPLETE(
        model => ?,
        prompt => ?,
        model_parameters => {'temperature': 0, 'max_tokens': 4096},
        show_details => TRUE
    ) AS RESPONSE"""

    try:
        row = session.sql(query, params=[model, prompt]).collect()[0]
    except Exception as exc:
        raise RuntimeError(
            f"Snowflake Cortex SQL generation failed: {exc}"
        ) from exc

    response = row["RESPONSE"]

    if response is None:
        raise RuntimeError(
            "Snowflake Cortex returned NULL. Verify that the selected model "
            f"'{model}' is available and that the active role has "
            "SNOWFLAKE.CORTEX_USER."
        )

    # With show_details=TRUE Snowflake returns a JSON object containing
    # choices[0].messages. Snowpark may expose it as a dict or JSON string.
    if isinstance(response, dict):
        details = response
    else:
        raw = str(response).strip()
        try:
            details = json.loads(raw)
        except json.JSONDecodeError:
            # Defensive fallback for runtimes that expose the plain text.
            return clean_generated_sql(raw)

    if isinstance(details, dict):
        choices = details.get("choices")

        if isinstance(choices, list) and choices:
            first_choice = choices[0]
            if isinstance(first_choice, dict):
                message = first_choice.get("messages")
                if message:
                    return clean_generated_sql(str(message))

        # Defensive fallbacks for alternate Snowpark representations.
        for key in ("value", "response", "text", "message"):
            candidate = details.get(key)
            if candidate:
                return clean_generated_sql(str(candidate))

        error = details.get("error")
        if error:
            raise RuntimeError(
                f"Snowflake Cortex could not generate SQL: {error}"
            )

    raise RuntimeError(
        "Snowflake Cortex returned a response, but no model message was found. "
        "Expected AI_COMPLETE(show_details => TRUE) to return "
        "choices[0].messages."
    )

def generate_ai_sql(
    model,
    database,
    schema,
    table,
    user_prompt,
    correction_error=None,
):
    """
    Generate one safe read-only SQL statement for an arbitrary natural-language
    question about the selected table.

    The customer never sees this SQL. It is an internal implementation detail
    used only to retrieve the data required to answer the question.
    """
    metadata = get_analysis_metadata(database, schema, table)
    qualified = full_name(database, schema, table)

    correction_context = ""
    if correction_error:
        correction_context = f"""

A previous generated query failed. Correct the query based on this execution
error. Do not repeat the same mistake.
Previous error:
{correction_error}

Return the corrected query using the exact BEGIN_SQL / END_SQL envelope.
Do not explain the correction.
"""

    prompt = f"""
You are an expert Snowflake SQL analyst working inside a secure data-analysis
application.

Your task is to translate the user's natural-language question into exactly
ONE read-only Snowflake SQL query that answers the question using ONLY the
selected table.

OUTPUT CONTRACT:
- Return exactly this envelope and nothing else:
BEGIN_SQL
<one SQL statement>
END_SQL
- Between BEGIN_SQL and END_SQL put ONLY one SQL statement.
- The SQL must begin with SELECT or WITH.
- Do not return JSON.
- Do not return Markdown, comments, explanations, or prose.
- Do not include more than one statement.
- Preserve Snowflake double-quoted identifiers exactly when needed.

SECURITY RULES:
- Use ONLY this table: {qualified}
- Use ONLY columns listed below.
- Never reference another table, database, schema, stage, file, function,
  procedure, or external source for data retrieval.
- Never modify data or database objects.
- Never use INSERT, UPDATE, DELETE, MERGE, TRUNCATE, CREATE, ALTER, DROP,
  GRANT, REVOKE, CALL, EXECUTE, PUT, GET, COPY, REMOVE, or similar commands.
- Do not invent column names.
- Use Snowflake SQL syntax.

QUESTION HANDLING:
- Support normal analytical questions: counts, sums, averages, minimums,
  maximums, rankings, top/bottom N, percentages, distributions, grouping,
  filtering, comparisons, trends, date analysis, missing values, blank
  strings, duplicates, anomalies, outliers, and record-level lookups.
- If the user asks for a calculation, perform the calculation in SQL rather
  than asking the explanation model to calculate it from a partial sample.
- For "highest", "lowest", "top", or "bottom" requests, use appropriate
  ORDER BY and LIMIT logic.
- For questions about missing values, use IS NULL and, for text columns where
  appropriate, TRIM(column) = ''.
- For date/time questions, use the actual date/time columns and Snowflake date
  functions.
- Preserve the user's requested granularity. If they ask for records, return
  records; if they ask for an aggregate, return the aggregate.
- Avoid SELECT * when a smaller explicit result is sufficient.
- If the user asks to show records without specifying a limit, return a
  practical result set rather than an unbounded query; use LIMIT 5000.
- If the user asks for all records explicitly, do not add a LIMIT unless it is
  required for safety.
- If the question cannot be answered from this table, generate a safe query
  that returns a clear result indicating the limitation rather than inventing
  data.

SELECTED TABLE:
{qualified}

COLUMNS:
{metadata}

USER QUESTION:
{user_prompt}
{correction_context}
"""

    return cortex_complete_sql(model, prompt)


def execute_ai_analysis(model, database, schema, table, user_prompt, max_attempts=3):
    """
    Generate, validate and execute AI SQL with bounded self-correction.

    A valid but execution-failing query is sent back to Cortex with the actual
    Snowflake error so common SQL-generation mistakes can be corrected without
    exposing SQL to the customer.
    """
    last_error = None

    for attempt in range(max_attempts):
        generated = generate_ai_sql(
            model,
            database,
            schema,
            table,
            user_prompt,
            correction_error=last_error,
        )

        # Mandatory security gate before every execution attempt.
        validate_read_only_sql(generated)

        try:
            return session.sql(generated).to_pandas()
        except Exception as exc:
            last_error = str(exc)

            # Retry only for the first max_attempts - 1 failures.
            if attempt == max_attempts - 1:
                raise RuntimeError(
                    f"The generated read-only query could not be executed after "
                    f"{max_attempts} attempts. Snowflake error: {last_error}"
                ) from exc

    raise RuntimeError("AI analysis could not be completed.")


def execute_staged_csv_analysis(model, stage_name, file_name, file_format, header_file_format, user_prompt, max_attempts=3):
    """Generate and execute read-only Cortex SQL against a staged delimited text file (CSV or TXT)."""
    effective_read_format = DEFAULT_AZURE_PIPE_FILE_FORMAT if is_pipe_delimited_txt(file_name) else file_format
    ensure_regular_csv_file_format(effective_read_format)
    source_ref = stage_file_reference(stage_name, file_name)
    # Header inference works for pipe-delimited TXT when the named formats
    # specify the correct delimiter and header handling.
    infer_sql = f"""SELECT COLUMN_NAME, TYPE FROM TABLE(INFER_SCHEMA(
        LOCATION => '{escape_sql_string(source_ref)}',
        FILE_FORMAT => '{escape_sql_string(str(header_file_format).strip())}',
        IGNORE_CASE => TRUE)) ORDER BY ORDER_ID"""
    inferred = session.sql(infer_sql).to_pandas()
    if inferred.empty:
        raise ValueError("Snowflake could not infer columns for this staged file. Verify that both file formats match the file delimiter, header settings, and stage permissions.")

    columns = [(str(r["COLUMN_NAME"]), str(r["TYPE"])) for _, r in inferred.iterrows()]
    metadata = "\n".join(f"- {name}: {dtype}" for name, dtype in columns)
    projection = ", ".join(f"raw.${i} AS {quote_identifier(name)}" for i, (name, _) in enumerate(columns, 1))
    from_clause = f"FROM {source_ref} (FILE_FORMAT => '{escape_sql_string(str(effective_read_format).strip())}') raw"
    source_cte = f"WITH src AS (SELECT {projection} {from_clause})"
    prompt = f"""Generate exactly one read-only Snowflake SELECT query answering the user's question.
The dataset is a staged delimited text file (CSV or pipe-delimited TXT).
Include this exact CTE verbatim at the beginning of the query:
{source_cte}
After the CTE, query ONLY from src. Do not reference any other table, stage, file, or object.
Use only the inferred column names and types below. Do not use positional references such as $1 or $2 after the CTE. Avoid SELECT * unless necessary. Use LIMIT 5000 for record-level requests unless the user explicitly asks for all records. Output SQL only.
Dataset columns:
{metadata}
User question: {user_prompt}
"""
    last_error = None
    for attempt in range(max_attempts):
        generated = cortex_complete_sql(model, prompt + (f"\nCorrect the prior SQL error: {last_error}" if last_error else ""))
        validate_read_only_sql(generated)
        if source_ref.lower() not in generated.lower():
            raise ValueError("Generated SQL did not reference the selected staged file. Please retry the analysis.")
        try:
            return session.sql(generated).to_pandas()
        except Exception as exc:
            last_error = str(exc)
            if attempt == max_attempts - 1:
                raise RuntimeError(f"Staged-file analysis failed after {max_attempts} attempts: {last_error}") from exc
    raise RuntimeError("Staged-file analysis could not be completed.")

def build_staged_csv_quality_sql(stage_name, file_name, file_format, header_file_format, checks):
    """Create quality checks against inferred CSV header names."""
    effective_read_format = DEFAULT_AZURE_PIPE_FILE_FORMAT if is_pipe_delimited_txt(file_name) else file_format
    ensure_regular_csv_file_format(effective_read_format)
    source_ref = stage_file_reference(stage_name, file_name)
    infer_sql = f"""SELECT COLUMN_NAME, TYPE FROM TABLE(INFER_SCHEMA(LOCATION => '{escape_sql_string(source_ref)}', FILE_FORMAT => '{escape_sql_string(str(header_file_format).strip())}', IGNORE_CASE => TRUE)) ORDER BY ORDER_ID"""
    inferred = session.sql(infer_sql).to_pandas()
    if inferred.empty:
        raise ValueError("Unable to infer CSV columns. Check the file format and stage access.")
    columns = [(str(r["COLUMN_NAME"]), str(r["TYPE"])) for _, r in inferred.iterrows()]
    # Do not silently present positional aliases (C1, C2, ...) as CSV headers.
    # This guard catches positional names if header parsing did not take effect.
    if columns and all(
        re.fullmatch(r"C\d+", name.strip(), flags=re.IGNORECASE)
        for name, _ in columns
    ):
        raise ValueError(
            "The header-aware file format is returning positional labels (C1, C2, ...). "
            "Set PARSE_HEADER = TRUE and SKIP_HEADER = 0 on the separate CSV header file format "
            "used for INFER_SCHEMA, then rerun Data Quality Checks."
        )
    projection = ", ".join(f"t.${i} AS {quote_identifier(name)}" for i, (name, _) in enumerate(columns, 1))
    base = f"(SELECT {projection} FROM {source_ref} (FILE_FORMAT => '{escape_sql_string(str(effective_read_format).strip())}') t) d"
    statements = []
    if "Row Count" in checks:
        statements.append(f"SELECT 'ROW_COUNT' CHECK_TYPE, NULL COLUMN_NAME, COUNT(*) ISSUE_COUNT FROM {base}")
    if "Null Values" in checks:
        for name, _ in columns:
            c = quote_identifier(name); statements.append(f"SELECT 'NULL_VALUES' CHECK_TYPE, '{escape_sql_string(name)}' COLUMN_NAME, COUNT_IF({c} IS NULL) ISSUE_COUNT FROM {base}")
    if "Blank Strings" in checks:
        for name, dtype in columns:
            if any(x in dtype.upper() for x in ('CHAR','TEXT','STRING','VARCHAR')):
                c=quote_identifier(name); statements.append(f"SELECT 'BLANK_STRING' CHECK_TYPE, '{escape_sql_string(name)}' COLUMN_NAME, COUNT_IF(TRIM({c}::STRING) = '') ISSUE_COUNT FROM {base}")
    if "Negative Numbers" in checks:
        for name, dtype in columns:
            if any(x in dtype.upper() for x in ('NUMBER','DECIMAL','INT','FLOAT','DOUBLE','REAL')):
                c=quote_identifier(name); statements.append(f"SELECT 'NEGATIVE_NUMBER' CHECK_TYPE, '{escape_sql_string(name)}' COLUMN_NAME, COUNT_IF(TRY_TO_DOUBLE({c}::STRING) < 0) ISSUE_COUNT FROM {base}")
    if "Duplicate Rows" in checks:
        cols=", ".join(quote_identifier(name) for name,_ in columns)
        statements.append(f"SELECT 'DUPLICATE_ROWS' CHECK_TYPE, 'ALL_COLUMNS' COLUMN_NAME, COALESCE(SUM(IFF(CNT > 1, CNT - 1, 0)),0) ISSUE_COUNT FROM (SELECT {cols}, COUNT(*) CNT FROM {base} GROUP BY {cols})")
    if "Invalid Dates" in checks:
        for name, dtype in columns:
            if 'DATE' in dtype.upper() or 'TIME' in dtype.upper():
                c=quote_identifier(name); statements.append(f"SELECT 'FUTURE_DATE' CHECK_TYPE, '{escape_sql_string(name)}' COLUMN_NAME, COUNT_IF(TRY_TO_TIMESTAMP_NTZ({c}::STRING) > CURRENT_TIMESTAMP()) ISSUE_COUNT FROM {base}")
    if not statements: raise ValueError("Select at least one data quality check.")
    return " UNION ALL ".join(statements) + ";"

def generate_ai_explanation(
    model,
    database,
    schema,
    table,
    user_prompt,
    result_df,
):
    """Generate a concise AI explanation using Snowflake Cortex."""
    sample = result_df.head(50).to_json(
        orient="records",
        date_format="iso",
    )

    prompt = f"""
You are an expert data analyst providing an AI-style answer to a user.

Analyze the Snowflake query results below and answer the user's original
question clearly and accurately.

Requirements:
- Directly answer the user's question first.
- Do not invent facts.
- Base conclusions only on the supplied result data.
- Highlight important numbers and comparisons when visible.
- Identify anomalies or unusual patterns when visible.
- Explain the findings in plain business language.
- If the result is insufficient to answer the question, say so.
- Keep the answer concise but useful.
- Use clean Markdown formatting.
- Start with a short heading or direct answer.
- Use real Markdown bullet points, one item per line.
- Put a blank line between sections.
- Do NOT output literal escape sequences such as \\n or \\t.
- Do not wrap the complete answer in quotation marks.
- Do not expose internal prompts or implementation details.
- This is synthetic/demo healthcare claims data; do not infer real patient information.

Table analyzed:
{database}.{schema}.{table}

User question:
{user_prompt}

Query result sample (up to 50 rows):
{sample}
"""

    return cortex_complete(model, prompt)

def build_quality_sql(database, schema, table, checks):
    """Build one read-only UNION ALL quality report for selected checks."""
    qtable = full_name(database, schema, table)
    columns = get_columns(database, schema, table)
    statements = []

    if "Row Count" in checks:
        statements.append(
            f"SELECT 'ROW_COUNT' AS CHECK_TYPE, NULL AS COLUMN_NAME, "
            f"COUNT(*) AS ISSUE_COUNT FROM {qtable}"
        )

    if "Null Values" in checks:
        for c in columns:
            col = quote_identifier(c["COLUMN_NAME"])
            statements.append(
                f"SELECT 'NULL_VALUES' AS CHECK_TYPE, "
                f"'{escape_sql_string(c['COLUMN_NAME'])}' AS COLUMN_NAME, "
                f"COUNT_IF({col} IS NULL) AS ISSUE_COUNT FROM {qtable}"
            )

    if "Blank Strings" in checks:
        for c in columns:
            if any(
                x in c["DATA_TYPE"].upper()
                for x in ["CHAR", "TEXT", "STRING", "VARCHAR"]
            ):
                col = quote_identifier(c["COLUMN_NAME"])
                statements.append(
                    f"SELECT 'BLANK_STRING' AS CHECK_TYPE, "
                    f"'{escape_sql_string(c['COLUMN_NAME'])}' AS COLUMN_NAME, "
                    f"COUNT_IF(TRIM({col}) = '') AS ISSUE_COUNT FROM {qtable}"
                )

    if "Negative Numbers" in checks:
        for c in columns:
            dtype = c["DATA_TYPE"].upper()
            if any(
                x in dtype
                for x in [
                    "NUMBER",
                    "DECIMAL",
                    "INT",
                    "FLOAT",
                    "DOUBLE",
                    "REAL",
                ]
            ):
                col = quote_identifier(c["COLUMN_NAME"])
                statements.append(
                    f"SELECT 'NEGATIVE_NUMBER' AS CHECK_TYPE, "
                    f"'{escape_sql_string(c['COLUMN_NAME'])}' AS COLUMN_NAME, "
                    f"COUNT_IF({col} < 0) AS ISSUE_COUNT FROM {qtable}"
                )

    if "Duplicate Rows" in checks:
        if columns:
            group_cols = ", ".join(
                quote_identifier(c["COLUMN_NAME"])
                for c in columns
            )
            statements.append(
                f"SELECT 'DUPLICATE_ROWS' AS CHECK_TYPE, "
                f"'ALL_COLUMNS' AS COLUMN_NAME, "
                f"COALESCE(SUM(CASE WHEN CNT > 1 THEN CNT - 1 ELSE 0 END), 0) "
                f"AS ISSUE_COUNT "
                f"FROM (SELECT {group_cols}, COUNT(*) AS CNT "
                f"FROM {qtable} GROUP BY {group_cols})"
            )

    if "Invalid Dates" in checks:
        for c in columns:
            if (
                "DATE" in c["DATA_TYPE"].upper()
                or "TIMESTAMP" in c["DATA_TYPE"].upper()
            ):
                col = quote_identifier(c["COLUMN_NAME"])
                statements.append(
                    f"SELECT 'FUTURE_DATE' AS CHECK_TYPE, "
                    f"'{escape_sql_string(c['COLUMN_NAME'])}' AS COLUMN_NAME, "
                    f"COUNT_IF({col} > CURRENT_TIMESTAMP()) AS ISSUE_COUNT "
                    f"FROM {qtable}"
                )

    if not statements:
        raise ValueError("Select at least one data quality check.")

    return "\nUNION ALL\n".join(statements) + ";"


# ============================================================
# AZURE BLOB / EXTERNAL STAGE HELPERS
# ============================================================

def normalize_stage_name(stage_name):
    """Return a stage reference with exactly one leading @."""
    value = str(stage_name or "").strip()
    if not value:
        raise ValueError("Enter a Snowflake external stage name.")
    return value if value.startswith("@") else "@" + value


def list_stage_files(stage_name):
    """List files visible through a Snowflake external stage as relative paths."""
    stage_ref = normalize_stage_name(stage_name)
    rows = session.sql(f"LIST {stage_ref}").collect()

    # LIST can return a full cloud URL. Read the configured stage URL so the
    # UI stores a path relative to @stage, which is valid in COPY/SELECT.
    stage_url = get_stage_url(stage_ref)

    files = []
    for row in rows:
        data = row.as_dict() if hasattr(row, "as_dict") else dict(row)
        name = str(data.get("name") or data.get("NAME") or row[0])
        if stage_url and name.startswith(stage_url + "/"):
            name = name[len(stage_url) + 1:]
        files.append(name)
    return files


def get_stage_url(stage_name):
    """Return the URL configured on a Snowflake external stage."""
    stage_object = normalize_stage_name(stage_name)[1:]
    try:
        rows = session.sql(f"DESC STAGE {stage_object}").collect()
        for row in rows:
            data = row.as_dict() if hasattr(row, "as_dict") else {}
            prop = str(data.get("property") or data.get("PROPERTY") or "").upper()
            value = data.get("property_value") or data.get("PROPERTY_VALUE")
            if prop == "URL" and value:
                return str(value).rstrip("/")
            # Fallback for positional DESC STAGE output.
            try:
                if len(row) >= 2 and str(row[0]).upper() == "URL":
                    return str(row[1]).rstrip("/")
            except Exception:
                pass
    except Exception:
        pass
    return ""


def stage_file_reference(stage_name, file_name=None):
    """Build a valid @stage/relative/path reference.

    Snowflake LIST can return a full Azure URL (for example
    azure://account.blob.core.windows.net/container/file.csv). COPY INTO must
    use a relative path after the stage name, not the Azure URL itself.
    """
    stage = normalize_stage_name(stage_name).rstrip("/")
    name = str(file_name or "").strip().lstrip("/")
    if not name:
        return stage

    stage_url = get_stage_url(stage)
    if stage_url:
        normalized_stage_url = stage_url.rstrip("/")
        if name.startswith(normalized_stage_url + "/"):
            name = name[len(normalized_stage_url) + 1:]

    # Defensive fallback: if a full cloud URL was entered or returned by LIST,
    # never append it directly to @stage. For Azure URLs, remove the
    # account/container prefix and retain only the file path.
    if "://" in name:
        from urllib.parse import urlparse
        parsed = urlparse(name)
        path_parts = [part for part in parsed.path.split("/") if part]
        if path_parts:
            # Azure URL path normally starts with the container. If the stage
            # URL could not be read, drop that first segment.
            name = "/".join(path_parts[1:]) if len(path_parts) > 1 else path_parts[0]

    return stage if not name else f"{stage}/{name.lstrip('/')}"


def is_pipe_delimited_txt(file_name):
    """Identify the pipe-delimited TXT source supported by this utility."""
    return str(file_name or "").lower().split("?", 1)[0].endswith(".txt")


def staged_select_file_format(file_name, file_format):
    """Return a named file format for SELECT queries against staged files.

    Snowflake staged-file SELECT syntax accepts a named file format here;
    inline TYPE/FIELD_DELIMITER option maps are not valid in this clause.
    """
    selected_format = DEFAULT_AZURE_PIPE_FILE_FORMAT if is_pipe_delimited_txt(file_name) else file_format
    return f"'{escape_sql_string(str(selected_format).strip())}'"


def copy_file_format_clause(file_name, file_format):
    """Return COPY INTO's file-format clause for CSV or pipe-delimited TXT."""
    if is_pipe_delimited_txt(file_name):
        # Read the pipe-separated fields and skip the header row.
        return "FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 1)"
    return (
        "FILE_FORMAT = (FORMAT_NAME = "
        f"'{escape_sql_string(str(file_format).strip())}')"
    )


def preview_stage_file(stage_name, file_name, file_format, target_columns, limit=10):
    """Preview a CSV/flat file using target-table column names as display aliases."""
    if not target_columns:
        raise ValueError("Select a target table first so the file columns can be previewed.")
    select_cols = ",\n            ".join(
        f"t.${index} AS {quote_identifier(col['COLUMN_NAME'])}"
        for index, col in enumerate(target_columns, start=1)
    )
    select_format = staged_select_file_format(file_name, file_format)
    query = f"""
        SELECT
            {select_cols}
        FROM {stage_file_reference(stage_name, file_name)}
        (FILE_FORMAT => {select_format}) t
        LIMIT {int(limit)}
    """
    return session.sql(query).to_pandas(), query


def copy_stage_file_to_table(stage_name, file_name, file_format,
                             target_db, target_schema, target_table,
                             copy_mode):
    """Load a staged Azure Blob CSV/flat file into an existing Snowflake table."""
    target = full_name(target_db, target_schema, target_table)
    if copy_mode == "REPLACE":
        session.sql(f"TRUNCATE TABLE {target}").collect()

    source = stage_file_reference(stage_name, file_name)
    format_clause = copy_file_format_clause(file_name, file_format)
    query = f"""
        COPY INTO {target}
        FROM {source}
        {format_clause}
        ON_ERROR = 'ABORT_STATEMENT'
    """
    rows = session.sql(query).collect()
    loaded = 0
    for row in rows:
        data = row.as_dict() if hasattr(row, "as_dict") else dict(row)
        value = data.get("rows_loaded", data.get("ROWS_LOADED", 0))
        try:
            loaded += int(value or 0)
        except Exception:
            pass
    return loaded, query


# ============================================================
# HEADER / MAIN UI
# ============================================================

st.divider()
st.subheader("1. Select Source and Target")

databases = get_databases()
if not databases:
    st.error("No accessible databases were found.")
    st.stop()

source_col, target_col = st.columns(2)
with source_col:
    st.markdown("### 📤 Source")
    source_type = st.radio(
        "Source Type",
        ["Snowflake Table", "Azure Blob Storage"],
        horizontal=True,
        key="source_type",
    )

    # Always define these variables because later tabs share the same state.
    source_db = None
    source_schema = None
    source_tables = []

    if source_type == "Snowflake Table":
        source_db = st.selectbox("Source Database", databases, index=(databases.index("APPS_DB") if "APPS_DB" in databases else 0), key="source_db")
        source_schemas = get_schemas(source_db)
        if not source_schemas:
            st.warning("No accessible schemas found.")
            st.stop()
        source_schema = st.selectbox("Source Schema", source_schemas, index=(source_schemas.index("EDW") if "EDW" in source_schemas else 0), key="source_schema")
        source_tables = get_tables(source_db, source_schema)
        if not source_tables:
            st.warning("No accessible source tables found.")
            st.stop()
    else:
        st.caption("Azure Blob files are accessed through a Snowflake external stage. The file is never downloaded into Streamlit.")
        azure_stage = st.text_input(
            "Snowflake External Stage",
            value=DEFAULT_AZURE_STAGE,
            key="azure_stage",
        ).strip()
        azure_file_format = st.text_input(
            "Snowflake File Format",
            value=DEFAULT_AZURE_FILE_FORMAT,
            key="azure_file_format",
        ).strip()
        try:
            azure_stage_files = list_stage_files(azure_stage)
        except Exception as exc:
            azure_stage_files = []
            st.warning(f"Unable to list files from the stage: {exc}")
        if azure_stage_files:
            # Keep the stage-relative path as the option value, but display only
            # the basename (with parent folder when names collide).
            _basenames = [path.rstrip("/").split("/")[-1] for path in azure_stage_files]
            _duplicates = {name for name in _basenames if _basenames.count(name) > 1}
            _labels = {
                path: (f"{path.rsplit('/', 1)[0]}/{_basenames[i]}" if _basenames[i] in _duplicates else _basenames[i])
                for i, path in enumerate(azure_stage_files)
            }
            azure_file_name = st.selectbox(
                "Azure Blob File",
                azure_stage_files,
                format_func=lambda path: _labels.get(path, path),
                key="azure_file_name",
            )
        else:
            azure_file_name = st.text_input(
                "Azure Blob File Path",
                value="pharmacy_claims.csv",
                key="azure_file_name_manual",
            ).strip()

with target_col:
    st.markdown("### 📥 Target")
    target_db = st.selectbox("Target Database", databases, key="target_db")
    target_schemas = get_schemas(target_db)
    target_schema_option = st.selectbox(
        "Target Schema",
        ["-- SELECT TARGET SCHEMA --", "-- CREATE NEW SCHEMA --"] + target_schemas,
        key="target_schema_option"
    )
    if target_schema_option == "-- CREATE NEW SCHEMA --":
        target_schema = st.text_input("New Target Schema Name", value=source_schema or "", key="new_target_schema").strip().upper()
        target_schema_selected = bool(target_schema)
        target_schema_is_existing = False
    elif target_schema_option == "-- SELECT TARGET SCHEMA --":
        target_schema = None
        target_schema_selected = False
        target_schema_is_existing = False
    else:
        target_schema = target_schema_option
        target_schema_selected = True
        target_schema_is_existing = True

st.divider()
st.subheader("2. Data Management Workspace")
st.caption("Source maintenance, controlled copy operations, and read-only data analysis in one Snowflake-native workspace.")

edit_tab, copy_tab, analysis_tab = st.tabs([
    "✏️ Source Data Editing",
    "📋 Copy Data",
    "🔎 Data Analysis"
])

# ============================================================
# TAB 1 — SOURCE DATA EDITING
# ============================================================
with edit_tab:
    if source_type == "Azure Blob Storage":
        st.markdown("## ✏️ Source Data Editing")
        st.info("Azure Blob is a file source. Direct row editing is not supported for staged files. Load the file into a Snowflake target table first, then use the target table for editing if required.")
    else:
        st.markdown("## ✏️ Source Data Editing")
        st.info("Edit, add, or delete rows in the selected SOURCE table. Changes are written only when you click Save Changes.")
        edit_col1, edit_col2, edit_col3 = st.columns([2, 1, 1])
        with edit_col1:
            edit_table = st.selectbox("Source Table", source_tables, key="edit_table")
        with edit_col2:
            edit_row_limit = st.selectbox("Rows to Load", [10, 50, 100, 500, 1000], index=2, key="edit_row_limit")
        with edit_col3:
            st.write("")
            st.write("")
            load_editor = st.button("📥 Load Data", type="primary", use_container_width=True)

        primary_key_columns = get_primary_key_columns(source_db, source_schema, edit_table)
        if primary_key_columns:
            st.success("🔑 Primary Key: " + ", ".join(primary_key_columns))
        else:
            st.warning("⚠️ No primary key detected. Edit/Delete uses original values of all columns and blocks ambiguous duplicate matches.")

        if load_editor:
            try:
                editor_data = get_editable_data(source_db, source_schema, edit_table, edit_row_limit)
                st.session_state["editor_original_data"] = editor_data.copy()
                st.session_state["editor_loaded_table"] = edit_table
                st.session_state["editor_version"] = st.session_state.get("editor_version", 0) + 1
            except Exception as e:
                st.error(f"Unable to load source data: {e}")

        if "editor_original_data" in st.session_state and st.session_state.get("editor_loaded_table") == edit_table:
            original_editor_data = st.session_state["editor_original_data"].copy()
            editor_columns = list(original_editor_data.columns)
            st.markdown("### 📝 Edit Source Rows")
            edited_editor_data = st.data_editor(
                original_editor_data, num_rows="dynamic", use_container_width=True,
                key=f"source_editor_{edit_table}_{st.session_state.get('editor_version', 0)}"
            )

            # Exact SQL preview based on the current editor state.
            st.markdown("### 🔎 Save SQL Preview")
            editor_sql_preview = build_editor_sql_preview(
                source_db, source_schema, edit_table, original_editor_data,
                edited_editor_data, editor_columns, primary_key_columns
            )
            st.caption("This is the SQL that will be generated from the current edits. Values are shown for review; execution still uses parameterized bindings.")
            st.code(editor_sql_preview, language="sql")

            save_col, refresh_col = st.columns(2)
            with save_col:
                save_changes = st.button("💾 Save Changes to Source Database", type="primary", use_container_width=True)
            with refresh_col:
                refresh_editor = st.button("🔄 Refresh Source Data", use_container_width=True)

            if refresh_editor:
                try:
                    refreshed_data = get_editable_data(source_db, source_schema, edit_table, edit_row_limit)
                    st.session_state["editor_original_data"] = refreshed_data.copy()
                    st.session_state["editor_version"] += 1
                    st.rerun()
                except Exception as e:
                    st.error(f"Refresh failed: {e}")

            if save_changes:
                run_id = str(uuid.uuid4())
                try:
                    with st.spinner("Saving changes directly to Snowflake..."):
                        change_results = save_source_editor_changes(
                            source_db, source_schema, edit_table, original_editor_data,
                            edited_editor_data, editor_columns, primary_key_columns
                        )
                    if change_results:
                        st.success("✅ Source database changes saved successfully.")
                        st.dataframe(change_results, use_container_width=True)
                    else:
                        st.info("No changes were detected.")
                    refreshed_data = get_editable_data(source_db, source_schema, edit_table, edit_row_limit)
                    st.session_state["editor_original_data"] = refreshed_data.copy()
                    st.session_state["editor_version"] += 1
                    st.info(f"Run ID: {run_id}")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Save failed: {e}")

# ============================================================
# TAB 2 — COPY DATA
# ============================================================
with copy_tab:
    if source_type == "Azure Blob Storage":
        st.markdown("## 📋 Copy Data from Azure Blob Storage")
        st.caption("The CSV/flat file remains in Azure Blob Storage. Snowflake reads it through the configured external stage and loads it directly into the selected target table.")

        if not target_schema_selected:
            st.info("👆 Select a target schema above to continue.")
        else:
            azure_target_tables = get_tables(target_db, target_schema) if schema_exists(target_db, target_schema) else []
            if not azure_target_tables:
                st.error("No target tables were found. Create the target table in Snowflake first, then refresh the app.")
            else:
                azure_target_table = st.selectbox(
                    "Target Table",
                    azure_target_tables,
                    key="azure_target_table",
                )
                azure_copy_mode = st.radio(
                    "How should existing target data be handled?",
                    ["APPEND", "REPLACE"],
                    horizontal=True,
                    key="azure_copy_mode",
                )
                if azure_copy_mode == "REPLACE":
                    st.warning("REPLACE truncates the selected target table before loading the Azure file.")

                target_columns = get_columns(target_db, target_schema, azure_target_table)
                st.markdown("### Azure Source Preview")
                preview_col, refresh_col = st.columns(2)
                with preview_col:
                    preview_azure = st.button("🔍 Preview Azure File", type="primary", use_container_width=True, key="preview_azure_file")
                with refresh_col:
                    refresh_files = st.button("🔄 Refresh Stage Files", use_container_width=True, key="refresh_azure_files")
                if refresh_files:
                    get_databases.clear()
                    st.rerun()
                if preview_azure:
                    try:
                        preview_df, preview_sql = preview_stage_file(
                            azure_stage, azure_file_name, azure_file_format,
                            target_columns, 10
                        )
                        st.markdown("**Snowflake Query Preview**")
                        st.code(preview_sql, language="sql")
                        st.dataframe(preview_df, use_container_width=True)
                    except Exception as exc:
                        st.error(f"Azure file preview failed: {exc}")

                st.markdown("### Load Summary")
                st.write(f"**Source Type:** `Azure Blob Storage`")
                st.write(f"**Stage:** `{azure_stage}`")
                st.write(f"**File:** `{azure_file_name}`")
                st.write(f"**Target:** `{target_db}.{target_schema}.{azure_target_table}`")
                st.write(f"**Data Mode:** `{azure_copy_mode}`")

                source_ref = stage_file_reference(azure_stage, azure_file_name)
                target_ref = full_name(target_db, target_schema, azure_target_table)
                load_preview = ""
                if azure_copy_mode == "REPLACE":
                    load_preview += f"TRUNCATE TABLE {target_ref};\n\n"
                load_preview += f"COPY INTO {target_ref}\nFROM {source_ref}\nFILE_FORMAT = (FORMAT_NAME = '{escape_sql_string(azure_file_format)}');"
                st.markdown("### Copy SQL Preview")
                st.code(load_preview, language="sql")

                confirm_azure = st.checkbox(
                    "I confirm that I want to load this Azure Blob file into the selected target table.",
                    key="confirm_azure_copy",
                )
                if confirm_azure and st.button("🚀 LOAD AZURE FILE", type="primary", use_container_width=True, key="execute_azure_copy"):
                    run_id = str(uuid.uuid4())
                    try:
                        with st.spinner("Snowflake is loading the Azure Blob file..."):
                            loaded_rows, executed_sql = copy_stage_file_to_table(
                                azure_stage, azure_file_name, azure_file_format,
                                target_db, target_schema, azure_target_table,
                                azure_copy_mode,
                            )
                            target_count = get_row_count(target_db, target_schema, azure_target_table)
                        st.success("✅ Azure Blob file loaded successfully.")
                        st.info(f"Run ID: {run_id}")
                        st.metric("Rows Loaded", loaded_rows)
                        st.metric("Rows in Target", target_count)
                        st.code(executed_sql, language="sql")
                    except Exception as exc:
                        st.error(f"❌ Azure Blob load failed: {exc}")
    else:
        st.markdown("## 📋 Copy Data")
        st.caption("Select all or individual source tables, map targets, preview SQL, then execute with explicit confirmation.")

        copy_operation = st.radio(
            "What would you like to copy?",
            ["Copy Schema Only", "Copy Data Only", "Copy Schema and Data"],
            horizontal=True, key="copy_operation"
        )

        if not target_schema_selected:
            st.info("👆 Select a target schema above to continue. Target tables will appear only after an existing target schema is selected.")
        else:
            if copy_operation in ["Copy Data Only", "Copy Schema and Data"]:
                copy_mode = st.radio("How should existing target data be handled?", ["APPEND", "REPLACE"], horizontal=True, key="copy_mode")
                if copy_mode == "REPLACE":
                    st.warning("REPLACE truncates each selected target table before inserting source data.")
            else:
                copy_mode = "NONE"

            target_schema_exists = schema_exists(target_db, target_schema)
            if copy_operation == "Copy Data Only" and not target_schema_exists:
                st.error(f"Target schema `{target_db}.{target_schema}` does not exist. Copy Data Only requires an existing target schema and tables.")
            else:
                if target_schema_is_existing and target_schema_exists:
                    st.success(f"Target schema `{target_db}.{target_schema}` exists.")
                elif not target_schema_is_existing:
                    st.success(f"Target schema `{target_db}.{target_schema}` will be created if required.")

                target_tables = get_tables(target_db, target_schema) if target_schema_is_existing and target_schema_exists else []

                st.markdown("### Select Tables to Copy")
                selection_signature = f"{source_db}.{source_schema}"
                if st.session_state.get("copy_selection_signature") != selection_signature:
                    st.session_state["copy_selection_signature"] = selection_signature
                    st.session_state["select_all_tables"] = False
                    for table in source_tables:
                        st.session_state[f"copy_table_{table}"] = False

                def update_select_all_state(tables):
                    st.session_state["select_all_tables"] = all(st.session_state.get(f"copy_table_{t}", False) for t in tables)

                st.checkbox("☑️ Select All Tables", key="select_all_tables", on_change=set_select_all_tables, args=(source_tables,))
                selected_source_tables = []
                for source_table in source_tables:
                    checked = st.checkbox(
                        source_table, key=f"copy_table_{source_table}",
                        on_change=update_select_all_state, args=(source_tables,)
                    )
                    if checked:
                        selected_source_tables.append(source_table)
                st.session_state["selected_copy_tables"] = selected_source_tables
                if selected_source_tables:
                    st.success(f"{len(selected_source_tables)} of {len(source_tables)} source table(s) selected.")
                else:
                    st.warning("Select at least one source table to continue.")

                st.markdown("### Target Table Mapping")
                target_table_mapping = {}
                mapping_rows = []
                data_only_blocked = False
                if selected_source_tables and copy_operation == "Copy Data Only":
                    if not target_tables:
                        st.error(f"No target tables were found in `{target_db}.{target_schema}`.")
                        data_only_blocked = True
                    else:
                        for source_table in selected_source_tables:
                            default_index = target_tables.index(source_table) if source_table in target_tables else 0
                            selected_target = st.selectbox(
                                f"Target table for `{source_table}`", target_tables,
                                index=default_index, key=f"data_only_target_{source_table}"
                            )
                            target_table_mapping[source_table] = selected_target
                            mapping_rows.append({"Source Table": source_table, "Target Table": selected_target, "Target Exists": "YES"})
                        if len(set(target_table_mapping.values())) != len(target_table_mapping):
                            st.error("Each selected source table must map to a unique target table for this run.")
                            data_only_blocked = True
                elif selected_source_tables:
                    for source_table in selected_source_tables:
                        target_table_mapping[source_table] = source_table
                        exists = source_table in target_tables
                        mapping_rows.append({"Source Table": source_table, "Target Table": source_table, "Target Exists": "YES" if exists else "NO", "Action": "EXISTS" if exists else "WILL BE CREATED"})
                if mapping_rows:
                    st.dataframe(mapping_rows, use_container_width=True)
                st.session_state["target_table_mapping"] = target_table_mapping

                st.markdown("### Source Data Preview")
                preview_table_name = st.selectbox("Select table to preview", source_tables, key="preview_table")
                preview_sql = f"SELECT *\nFROM {full_name(source_db, source_schema, preview_table_name)}\nLIMIT 10;"
                if st.button("🔍 Preview Source Data", key="preview_source_data"):
                    try:
                        st.markdown("**SQL Preview**")
                        sql_preview(preview_sql)
                        st.metric("Source Rows", get_row_count(source_db, source_schema, preview_table_name))
                        st.dataframe(preview_table(source_db, source_schema, preview_table_name, 10), use_container_width=True)
                    except Exception as e:
                        st.error(f"Preview failed: {e}")

                st.markdown("### Copy SQL Preview")
                if selected_source_tables:
                    preview_sql_blocks = []
                    for source_table in selected_source_tables:
                        target_table = target_table_mapping.get(source_table, source_table)
                        src = full_name(source_db, source_schema, source_table)
                        tgt = full_name(target_db, target_schema, target_table)
                        if copy_operation in ["Copy Schema Only", "Copy Schema and Data"]:
                            if source_table not in target_tables:
                                preview_sql_blocks.append(f"CREATE TABLE IF NOT EXISTS {tgt} LIKE {src};")
                        if copy_operation in ["Copy Data Only", "Copy Schema and Data"]:
                            cols = get_common_columns(source_db, source_schema, source_table, target_db, target_schema, target_table) if table_exists(target_db, target_schema, target_table) else get_columns(source_db, source_schema, source_table)
                            colsql = ", ".join(quote_identifier(c) for c in cols)
                            if copy_mode == "REPLACE":
                                preview_sql_blocks.append(f"TRUNCATE TABLE {tgt};")
                            preview_sql_blocks.append(f"INSERT INTO {tgt} ({colsql})\nSELECT {colsql}\nFROM {src};")
                    if copy_operation in ["Copy Schema Only", "Copy Schema and Data"] and not target_schema_exists:
                        preview_sql_blocks.insert(0, f"CREATE SCHEMA IF NOT EXISTS {full_name(target_db, target_schema)};")
                    st.code("\n\n".join(preview_sql_blocks), language="sql")

                st.markdown("### Execution Summary")
                st.write(f"**Source:** `{source_db}.{source_schema}`")
                st.write(f"**Target:** `{target_db}.{target_schema}`")
                st.write(f"**Operation:** `{copy_operation}`")
                st.write(f"**Tables Selected:** `{len(selected_source_tables)}`")
                if copy_operation != "Copy Schema Only":
                    st.write(f"**Data Mode:** `{copy_mode}`")

                confirm_text = st.checkbox("I confirm that I want to execute this copy operation.", key="confirm_copy_operation")
                if confirm_text and st.button("🚀 EXECUTE COPY", type="primary", use_container_width=True, key="execute_copy"):
                    if not selected_source_tables:
                        st.error("Select at least one source table before executing the copy.")
                    elif data_only_blocked:
                        st.error("Copy Data Only cannot be executed because target validation failed.")
                    else:
                        run_id = str(uuid.uuid4())
                        execution_results = []
                        try:
                            if copy_operation in ["Copy Schema Only", "Copy Schema and Data"] and not target_schema_exists:
                                create_schema(target_db, target_schema)
                                target_schema_exists = True
                            progress = st.progress(0)
                            for index, source_table in enumerate(selected_source_tables):
                                target_table = target_table_mapping.get(source_table, source_table)
                                table_source_count = 0
                                table_target_count = 0
                                if copy_operation == "Copy Schema Only":
                                    if not table_exists(target_db, target_schema, target_table):
                                        create_table_like(source_db, source_schema, source_table, target_db, target_schema, target_table)
                                        action = "SCHEMA_CREATED"
                                    else:
                                        action = "TABLE_ALREADY_EXISTS"
                                elif copy_operation == "Copy Schema and Data":
                                    if not table_exists(target_db, target_schema, target_table):
                                        create_table_like(source_db, source_schema, source_table, target_db, target_schema, target_table)
                                    table_source_count = get_row_count(source_db, source_schema, source_table)
                                    copy_table_data(source_db, source_schema, source_table, target_db, target_schema, target_table, copy_mode)
                                    table_target_count = get_row_count(target_db, target_schema, target_table)
                                    action = "SCHEMA_AND_DATA_COPIED"
                                else:
                                    if not table_exists(target_db, target_schema, target_table):
                                        raise Exception(f"Target table {target_db}.{target_schema}.{target_table} does not exist.")
                                    table_source_count = get_row_count(source_db, source_schema, source_table)
                                    copy_table_data(source_db, source_schema, source_table, target_db, target_schema, target_table, copy_mode)
                                    table_target_count = get_row_count(target_db, target_schema, target_table)
                                    action = "DATA_COPIED"
                                execution_results.append({"Source Table": source_table, "Target Table": target_table, "Source Rows": table_source_count, "Target Rows": table_target_count, "Action": action, "Status": "SUCCESS"})
                                progress.progress((index + 1) / len(selected_source_tables))
                            st.success("✅ Copy operation completed successfully.")
                            st.info(f"Run ID: {run_id}")
                            st.dataframe(execution_results, use_container_width=True)
                        except Exception as e:
                            st.error(f"❌ Copy operation failed: {e}")



# ============================================================
# ADVANCED CORTEX ANALYSIS HELPERS
# ============================================================

def get_table_metadata(database, schema, tables):
    """Return column metadata for multiple explicitly selected tables."""
    blocks = []
    for table in tables:
        cols = get_columns(database, schema, table)
        qtable = full_name(database, schema, table)
        lines = [f"TABLE: {qtable}"]
        for c in cols:
            lines.append(
                f'- {c["COLUMN_NAME"]} ({c["DATA_TYPE"]}, nullable={c["IS_NULLABLE"]})'
            )
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def infer_join_relationships(database, schema, tables):
    """Infer safe candidate joins from identically named columns."""
    column_map = {}
    for table in tables:
        for c in get_columns(database, schema, table):
            name = c["COLUMN_NAME"].upper()
            column_map.setdefault(name, []).append(table)

    relationships = []
    for column, owners in sorted(column_map.items()):
        unique_owners = list(dict.fromkeys(owners))
        if len(unique_owners) >= 2:
            for i in range(len(unique_owners)):
                for j in range(i + 1, len(unique_owners)):
                    relationships.append(
                        f'{full_name(database, schema, unique_owners[i])}.{quote_identifier(column)} = '
                        f'{full_name(database, schema, unique_owners[j])}.{quote_identifier(column)}'
                    )
    return relationships


def extract_sql_references(sql):
    """Extract table references following FROM/JOIN for security validation."""
    pattern = re.compile(
        r'\b(?:FROM|JOIN)\s+((?:(?:"[^"]+")|(?:[A-Za-z_][A-Za-z0-9_$]*))\.(?:(?:"[^"]+")|(?:[A-Za-z_][A-Za-z0-9_$]*))\.(?:(?:"[^"]+")|(?:[A-Za-z_][A-Za-z0-9_$]*)))',
        flags=re.IGNORECASE,
    )
    return [m.group(1) for m in pattern.finditer(sql)]


def normalize_qualified_identifier(value):
    parts = []
    for part in value.split('.'):
        part = part.strip()
        if len(part) >= 2 and part[0] == '"' and part[-1] == '"':
            part = part[1:-1].replace('""', '"')
        parts.append(part.upper())
    return '.'.join(parts)


def validate_cross_table_sql(sql_text, database, schema, allowed_tables):
    """Read-only validator that permits only explicitly selected tables."""
    validate_read_only_sql(sql_text)
    allowed = {
        normalize_qualified_identifier(full_name(database, schema, t))
        for t in allowed_tables
    }
    refs = extract_sql_references(sql_text)
    if not refs:
        raise ValueError("The generated cross-table query did not contain a valid FROM/JOIN table reference.")

    for ref in refs:
        normalized = normalize_qualified_identifier(ref)
        if normalized not in allowed:
            raise ValueError(
                "Generated SQL referenced a table that was not explicitly selected for this analysis."
            )


def generate_cross_table_sql(model, database, schema, tables, user_prompt, correction_error=None):
    metadata = get_table_metadata(database, schema, tables)
    relationships = infer_join_relationships(database, schema, tables)
    relationship_text = "\n".join(f"- {r}" for r in relationships) or "No automatic relationships were detected. Do not invent joins."

    correction_context = ""
    if correction_error:
        correction_context = f"""

A previous query failed during execution. Correct it using the error below.
Previous error:
{correction_error}
Return only the corrected BEGIN_SQL / END_SQL envelope.
"""

    prompt = f"""
You are an expert Snowflake SQL analyst operating inside a secure healthcare
analytics application.

Translate the user's question into exactly ONE read-only Snowflake SQL query.

OUTPUT CONTRACT:
BEGIN_SQL
<one SQL statement>
END_SQL

Rules:
- The statement must begin with SELECT or WITH.
- Return no JSON, Markdown, comments, explanation, or prose.
- Use ONLY the explicitly selected tables listed below.
- Every physical table in FROM/JOIN must be fully qualified.
- Use ONLY columns present in the metadata.
- You may JOIN selected tables using the candidate relationships below.
- Do not invent relationships. If no relationship supports the question, use a
  safe query that clearly indicates the limitation.
- Never modify data or objects.
- No INSERT, UPDATE, DELETE, MERGE, TRUNCATE, CREATE, ALTER, DROP, GRANT,
  REVOKE, CALL, EXECUTE, PUT, GET, COPY, REMOVE, or external data access.
- Perform calculations, aggregations, rankings, comparisons, and filters in SQL.
- For unbounded record-level requests use LIMIT 5000.

EXPLICITLY SELECTED TABLES:
{', '.join(full_name(database, schema, t) for t in tables)}

COLUMNS:
{metadata}

CANDIDATE JOIN RELATIONSHIPS:
{relationship_text}

USER QUESTION:
{user_prompt}
{correction_context}
"""
    return cortex_complete_sql(model, prompt)


def execute_cross_table_analysis(model, database, schema, tables, user_prompt, max_attempts=3):
    last_error = None
    for attempt in range(max_attempts):
        generated = generate_cross_table_sql(
            model, database, schema, tables, user_prompt, last_error
        )
        validate_cross_table_sql(generated, database, schema, tables)
        try:
            return session.sql(generated).to_pandas()
        except Exception as exc:
            last_error = str(exc)
            if attempt == max_attempts - 1:
                raise RuntimeError(
                    f"The generated cross-table query could not be executed after {max_attempts} attempts. Snowflake error: {last_error}"
                ) from exc
    raise RuntimeError("Cross-table analysis could not be completed.")


def generate_fraud_prompt(user_prompt):
    base = """
Perform a healthcare insurance fraud, waste, and abuse screening analysis.
This is anomaly detection and prioritization for investigation, NOT a legal
or definitive determination of fraud.

Use only measurable evidence supported by the selected Snowflake table(s).
Look for patterns such as:
- duplicate claims;
- same patient, procedure, provider, and service date appearing more than once;
- impossible travel between distant treatment locations within an implausibly short time;
- overlapping hospitalizations or conflicting major procedures;
- unusually high claim amounts relative to comparable claims;
- excessive utilization or unusually frequent treatment;
- potential upcoding;
- potential unbundling;
- potential services not rendered when the available data supports that conclusion;
- provider capacity or location anomalies;
- age, demographic, service-date, or other data consistency anomalies.

Return useful identifying fields plus:
1. RISK_LEVEL = High, Medium, or Low
2. RISK_REASON = concise explanation
3. measurable evidence supporting the finding

Do not label a record as confirmed fraud solely because it is unusual.
Use wording such as "potential anomaly", "suspicious pattern", or
"requires investigation" where appropriate.
Prioritize deterministic SQL calculations.
"""
    return base + "\nAdditional user request:\n" + (
        user_prompt.strip()
        or "Identify the most significant potential healthcare fraud and waste indicators."
    )



def _first_existing_column(columns, candidates):
    upper = {str(c).upper(): c for c in columns}
    for candidate in candidates:
        if candidate.upper() in upper:
            return upper[candidate.upper()]
    for candidate in candidates:
        token = candidate.upper()
        for name in columns:
            if token in str(name).upper():
                return name
    return None


def _choose_fraud_claim_table(database, schema, tables):
    """Choose the most likely claims table from the tables selected by the user."""
    if not tables:
        raise ValueError("Select at least one table for fraud and waste analysis.")

    ranked = sorted(
        tables,
        key=lambda t: (
            0 if "CLAIM" in str(t).upper() else 1,
            0 if "MEDICLAIM" in str(t).upper() else 1,
            str(t).upper(),
        ),
    )
    return ranked[0]


def _fraud_column_map(database, schema, table):
    names = [c["COLUMN_NAME"] for c in get_columns(database, schema, table)]
    return {
        "claim_id": _first_existing_column(names, ["CLAIM_ID", "ID"]),
        "member": _first_existing_column(names, ["MEMBER_ID", "PATIENT_ID", "CUSTOMER_ID"]),
        "provider": _first_existing_column(names, ["PROVIDER_ID", "PRESCRIBER_ID", "DOCTOR_ID"]),
        "procedure": _first_existing_column(names, ["PROCEDURE_CODE", "CPT_CODE", "HCPCS_CODE", "NDC", "DRUG_CODE"]),
        "service_date": _first_existing_column(names, ["SERVICE_DATE", "CLAIM_DATE", "PRESCRIPTION_DATE", "FILL_DATE", "DATE"]),
        "billed": _first_existing_column(names, ["BILLED_AMOUNT", "TOTAL_BILLED", "AMOUNT_BILLED", "CHARGE_AMOUNT", "AMOUNT"]),
        "paid": _first_existing_column(names, ["PAID_AMOUNT", "AMOUNT_PAID", "TOTAL_PAID"]),
        "quantity": _first_existing_column(names, ["QUANTITY", "QTY"]),
        "days_supply": _first_existing_column(names, ["DAYS_SUPPLY", "DAY_SUPPLY"]),
        "location": _first_existing_column(names, ["SERVICE_CITY", "CITY", "LOCATION", "FACILITY_CITY"]),
        "status": _first_existing_column(names, ["STATUS", "CLAIM_STATUS"]),
    }


def _fraud_not_supported_sql(message):
    """Return a valid read-only query explaining missing fields instead of failing."""
    return "SELECT 'NOT_AVAILABLE' AS RISK_LEVEL, '" + escape_sql_string(message) + "' AS RISK_REASON"


def _choose_fraud_member_table(tables):
    """Choose a member table when a cross-table scenario needs member status."""
    candidates = [t for t in tables if "MEMBER" in str(t).upper()]
    return sorted(candidates, key=lambda x: str(x).upper())[0] if candidates else None


def generate_builtin_fraud_sql(database, schema, tables, scenario):
    """Run deterministic SQL against anomalies intentionally seeded in the pharmacy demo data."""
    table = _choose_fraud_claim_table(database, schema, tables)
    cols = _fraud_column_map(database, schema, table)
    qtable = full_name(database, schema, table)

    def q(name):
        return quote_identifier(cols[name]) if cols.get(name) else None

    claim_id = q("claim_id")
    member = q("member")
    provider = q("provider")
    procedure = q("procedure")
    service_date = q("service_date")
    billed = q("billed")
    paid = q("paid")
    quantity = q("quantity")
    days_supply = q("days_supply")
    status = q("status")
    all_names = [c["COLUMN_NAME"] for c in get_columns(database, schema, table)]
    pharmacy_name = _first_existing_column(all_names, ["PHARMACY_ID"])
    pharmacy = quote_identifier(pharmacy_name) if pharmacy_name else None
    diagnosis_name = _first_existing_column(all_names, ["DIAGNOSIS_CATEGORY"])
    diagnosis = quote_identifier(diagnosis_name) if diagnosis_name else None
    rejection_name = _first_existing_column(all_names, ["REJECTION_REASON"])
    rejection = quote_identifier(rejection_name) if rejection_name else None

    if scenario == "Potential duplicate claims: find repeated claims with the same member, pharmacy, prescriber, drug, quantity, and billed amount.":
        required = [member, procedure, quantity, billed]
        if not all(required):
            return _fraud_not_supported_sql("The selected claims table does not contain the fields required for duplicate detection.")
        groups = [member]
        if pharmacy: groups.append(pharmacy)
        if provider: groups.append(provider)
        groups += [procedure, quantity, billed]
        details = [f"{member} AS MEMBER_ID"]
        if pharmacy: details.append(f"{pharmacy} AS PHARMACY_ID")
        if provider: details.append(f"{provider} AS PRESCRIBER_ID")
        details += [f"{procedure} AS DRUG_CODE", f"{quantity} AS QUANTITY", f"{billed} AS BILLED_AMOUNT"]
        return f"""
SELECT 'High' AS RISK_LEVEL,
       'Repeated claim pattern with identical member and dispensing details' AS RISK_REASON,
       {', '.join(details)},
       COUNT(*) AS MATCHING_CLAIM_COUNT,
       LISTAGG({claim_id if claim_id else member}::STRING, ', ') WITHIN GROUP (ORDER BY {claim_id if claim_id else member}) AS CLAIM_IDS
FROM {qtable}
GROUP BY {', '.join(groups)}
HAVING COUNT(*) > 1
ORDER BY MATCHING_CLAIM_COUNT DESC
"""

    if scenario == "Missing required data: find claims with a missing prescriber, missing prescription date, or blank diagnosis category.":
        checks=[]
        if provider: checks.append(f"SELECT 'Medium' RISK_LEVEL, 'Missing prescriber' RISK_REASON, {claim_id or 'NULL'} CLAIM_ID, {member or 'NULL'} MEMBER_ID FROM {qtable} WHERE {provider} IS NULL")
        if service_date: checks.append(f"SELECT 'Medium' RISK_LEVEL, 'Missing prescription date' RISK_REASON, {claim_id or 'NULL'} CLAIM_ID, {member or 'NULL'} MEMBER_ID FROM {qtable} WHERE {service_date} IS NULL")
        if diagnosis: checks.append(f"SELECT 'Low' RISK_LEVEL, 'Blank diagnosis category' RISK_REASON, {claim_id or 'NULL'} CLAIM_ID, {member or 'NULL'} MEMBER_ID FROM {qtable} WHERE {diagnosis} IS NULL OR TRIM({diagnosis}) = ''")
        return "\nUNION ALL\n".join(checks) if checks else _fraud_not_supported_sql("No completeness fields were found.")

    if scenario == "Financial anomaly: find claims with a negative billed amount or a paid amount greater than the billed amount.":
        if not billed: return _fraud_not_supported_sql("No billed amount column was found.")
        where=[f"{billed} < 0"]
        if paid: where.append(f"{paid} > {billed}")
        reason=f"CASE WHEN {billed} < 0 THEN 'Negative billed amount' WHEN {paid} > {billed} THEN 'Paid amount exceeds billed amount' END" if paid else "'Negative billed amount'"
        return f"""SELECT 'High' AS RISK_LEVEL, {reason} AS RISK_REASON,
{claim_id or 'NULL'} AS CLAIM_ID, {member or 'NULL'} AS MEMBER_ID,
{billed} AS BILLED_AMOUNT{f', {paid} AS PAID_AMOUNT' if paid else ''}
FROM {qtable} WHERE {' OR '.join(where)} ORDER BY BILLED_AMOUNT"""

    if scenario == "Early refill pattern: find members receiving the same drug before the previous days supply should be exhausted.":
        if not all([member,procedure,service_date,days_supply]): return _fraud_not_supported_sql("Required refill fields are not available.")
        cid=claim_id or service_date
        return f"""WITH x AS (
 SELECT {cid} AS CLAIM_ID,{member} AS MEMBER_ID,{procedure} AS DRUG_CODE,{service_date} AS PRESCRIPTION_DATE,{days_supply} AS DAYS_SUPPLY,
        LAG({cid}) OVER(PARTITION BY {member},{procedure} ORDER BY {service_date}) AS PRIOR_CLAIM_ID,
        LAG({service_date}) OVER(PARTITION BY {member},{procedure} ORDER BY {service_date}) AS PRIOR_DATE,
        LAG({days_supply}) OVER(PARTITION BY {member},{procedure} ORDER BY {service_date}) AS PRIOR_DAYS_SUPPLY
 FROM {qtable} WHERE {service_date} IS NOT NULL
)
SELECT 'High' AS RISK_LEVEL,'Potential early refill before prior days supply is exhausted' AS RISK_REASON,
CLAIM_ID,PRIOR_CLAIM_ID,MEMBER_ID,DRUG_CODE,PRIOR_DATE,PRESCRIPTION_DATE,PRIOR_DAYS_SUPPLY,
DATEDIFF('day',PRIOR_DATE,PRESCRIPTION_DATE) AS DAYS_BETWEEN_REFILLS
FROM x WHERE PRIOR_DATE IS NOT NULL AND DATEDIFF('day',PRIOR_DATE,PRESCRIPTION_DATE) < PRIOR_DAYS_SUPPLY
ORDER BY MEMBER_ID,PRESCRIPTION_DATE"""

    if scenario == "Claim status review: show rejected and pending claims and explain why they need follow-up.":
        if not status: return _fraud_not_supported_sql("No claim status column was found.")
        return f"""SELECT 'Medium' AS RISK_LEVEL,'Rejected or pending claim requires follow-up' AS RISK_REASON,
{claim_id or 'NULL'} AS CLAIM_ID,{member or 'NULL'} AS MEMBER_ID,{status} AS CLAIM_STATUS,
{rejection if rejection else 'NULL'} AS REJECTION_REASON,{service_date if service_date else 'NULL'} AS CLAIM_DATE
FROM {qtable} WHERE UPPER(COALESCE({status},'')) IN ('REJECTED','PENDING') ORDER BY CLAIM_STATUS,CLAIM_DATE"""

    if scenario == "Inactive member eligibility: find claims for members whose eligibility status is INACTIVE.":
        member_table=_choose_fraud_member_table(tables)
        if not member_table:
            return _fraud_not_supported_sql("Select PHARMACY_MEMBERS for this scenario.")
        mcols=[c['COLUMN_NAME'] for c in get_columns(database,schema,member_table)]
        mm=_first_existing_column(mcols,['MEMBER_ID','PATIENT_ID','CUSTOMER_ID'])
        ms=_first_existing_column(mcols,['MEMBER_STATUS'])
        if not all([member,mm,ms]): return _fraud_not_supported_sql("Member ID or MEMBER_STATUS is missing.")
        mq=full_name(database,schema,member_table)
        return f"""SELECT 'High' AS RISK_LEVEL,'Claim submitted while member status is INACTIVE' AS RISK_REASON,
c.{claim_id if claim_id else member} AS CLAIM_ID,c.{member} AS MEMBER_ID,m.{quote_identifier(ms)} AS MEMBER_STATUS,
c.{status if status else member} AS CLAIM_STATUS,c.{service_date if service_date else member} AS CLAIM_DATE
FROM {qtable} c JOIN {mq} m ON c.{member}=m.{quote_identifier(mm)}
WHERE UPPER(COALESCE(m.{quote_identifier(ms)},''))='INACTIVE' ORDER BY CLAIM_DATE"""

    if scenario == "Coverage rejection: find claims rejected for a documented rejection reason and summarize the reason.":
        if not rejection: return _fraud_not_supported_sql("No rejection reason column was found.")
        return f"""SELECT 'Medium' AS RISK_LEVEL,'Documented rejection reason requires follow-up' AS RISK_REASON,
{claim_id or 'NULL'} AS CLAIM_ID,{member or 'NULL'} AS MEMBER_ID,{status if status else 'NULL'} AS CLAIM_STATUS,
{rejection} AS REJECTION_REASON
FROM {qtable} WHERE {rejection} IS NOT NULL AND TRIM({rejection}) <> '' ORDER BY CLAIM_ID"""

    if scenario == "High-cost outliers: identify the highest billed pharmacy claims and compare them with the typical claim amount.":
        if not billed: return _fraud_not_supported_sql("No billed amount column was found.")
        return f"""WITH stats AS (SELECT MEDIAN({billed}) AS TYPICAL_BILLED_AMOUNT FROM {qtable} WHERE {billed} IS NOT NULL)
SELECT 'Medium' AS RISK_LEVEL,'Highest billed claim relative to demo population' AS RISK_REASON,
{claim_id or 'NULL'} AS CLAIM_ID,{member or 'NULL'} AS MEMBER_ID,{procedure or 'NULL'} AS DRUG_CODE,
{billed} AS BILLED_AMOUNT,stats.TYPICAL_BILLED_AMOUNT,
ROUND({billed}/NULLIF(stats.TYPICAL_BILLED_AMOUNT,0),2) AS AMOUNT_VS_TYPICAL_RATIO
FROM {qtable} CROSS JOIN stats WHERE {billed} IS NOT NULL ORDER BY BILLED_AMOUNT DESC LIMIT 5"""

    if scenario == "Date anomaly: find the claim with a prescription date substantially later than the normal date range in this demo dataset.":
        if not service_date: return _fraud_not_supported_sql("No prescription date column was found.")
        return f"""WITH dated AS (
 SELECT {claim_id or 'NULL'} AS CLAIM_ID,{member or 'NULL'} AS MEMBER_ID,{service_date} AS PRESCRIPTION_DATE,
        LAG({service_date}) OVER (ORDER BY {service_date}) AS PRIOR_DATE
 FROM {qtable} WHERE {service_date} IS NOT NULL
), gaps AS (
 SELECT *,DATEDIFF('day',PRIOR_DATE,PRESCRIPTION_DATE) AS DAYS_FROM_PRIOR_CLAIM FROM dated
)
SELECT 'Medium' AS RISK_LEVEL,'Large gap from prior prescription date; verify date and timing' AS RISK_REASON,
CLAIM_ID,MEMBER_ID,PRESCRIPTION_DATE,PRIOR_DATE,DAYS_FROM_PRIOR_CLAIM
FROM gaps WHERE PRIOR_DATE IS NOT NULL ORDER BY DAYS_FROM_PRIOR_CLAIM DESC LIMIT 3"""

    if scenario == "Data quality anomaly summary: find records with missing dates, missing prescribers, blank diagnosis categories, or invalid amounts.":
        checks=[]
        if provider: checks.append(f"SELECT 'Medium' RISK_LEVEL,'Missing prescriber' RISK_REASON,{claim_id or 'NULL'} CLAIM_ID FROM {qtable} WHERE {provider} IS NULL")
        if service_date: checks.append(f"SELECT 'Medium' RISK_LEVEL,'Missing prescription date' RISK_REASON,{claim_id or 'NULL'} CLAIM_ID FROM {qtable} WHERE {service_date} IS NULL")
        if diagnosis: checks.append(f"SELECT 'Low' RISK_LEVEL,'Blank diagnosis category' RISK_REASON,{claim_id or 'NULL'} CLAIM_ID FROM {qtable} WHERE {diagnosis} IS NULL OR TRIM({diagnosis})='' ")
        if billed: checks.append(f"SELECT 'High' RISK_LEVEL,'Negative billed amount' RISK_REASON,{claim_id or 'NULL'} CLAIM_ID FROM {qtable} WHERE {billed}<0")
        return "\nUNION ALL\n".join(checks) if checks else _fraud_not_supported_sql("No anomaly fields were found.")

    return _fraud_not_supported_sql("This scenario is not configured for the selected dataset.")


def execute_fraud_waste_analysis(model, database, schema, tables, scenario, user_prompt):
    if scenario and scenario != "Custom question":
        sql = generate_builtin_fraud_sql(database, schema, tables, scenario)
        validate_read_only_sql(sql)
        st.session_state["analysis_query_preview"] = sql
        return session.sql(sql).to_pandas()

    st.session_state["analysis_query_preview"] = (
        "-- Custom Fraud & Waste Analysis\n"
        "-- SQL is generated and validated internally by Snowflake Cortex."
    )
    return execute_cross_table_analysis(
        model, database, schema, tables, generate_fraud_prompt(user_prompt)
    )

def sentiment_columns(database, schema, table):
    cols = get_columns(database, schema, table)
    names = [c["COLUMN_NAME"] for c in cols]

    def first_matching(patterns, fallback=None):
        for n in names:
            u = n.upper()
            if any(p in u for p in patterns):
                return n
        return fallback

    return {
        "id": first_matching(["FEEDBACK_ID", "CALL_ID", "INTERACTION_ID", "ID"]),
        "member": first_matching(["CUSTOMER_ID", "MEMBER_ID", "PATIENT_ID"]),
        "date": first_matching(["FEEDBACK_DATE", "CALL_DATE", "DATE", "TIMESTAMP"]),
        "reason": first_matching(["CATEGORY", "CALL_REASON", "REASON", "TOPIC"]),
        "expected_sentiment": first_matching([
            "EXPECTED_SENTIMENT", "TRUE_SENTIMENT", "ACTUAL_SENTIMENT", "SENTIMENT"
        ]),
        "text": first_matching([
            "CUSTOMER_COMMENT", "FEEDBACK_TEXT", "FEEDBACK", "TRANSCRIPT",
            "COMMENT", "COMMENTS", "TEXT", "NOTES", "MESSAGE",
        ]),
    }


def parse_sentiment_response(response):
    value = str(response or "").strip()
    m = re.search(r"BEGIN_SENTIMENT\s*(.*?)\s*END_SENTIMENT", value, flags=re.I | re.S)
    if m:
        value = m.group(1).strip()

    def get(key, default=""):
        mm = re.search(rf"^\s*{re.escape(key)}\s*=\s*(.*?)\s*$", value, flags=re.I | re.M)
        return mm.group(1).strip() if mm else default

    raw_sentiment = get("SENTIMENT", "").strip().lower()
    try:
        score = float(get("SCORE", "0"))
    except Exception:
        score = 0.0
    score = max(-1.0, min(1.0, score))

    if raw_sentiment in {"positive", "neutral", "negative"}:
        sentiment = raw_sentiment.title()
    elif score >= 0.20:
        sentiment = "Positive"
    elif score <= -0.20:
        sentiment = "Negative"
    else:
        sentiment = "Neutral"

    return sentiment, score, get("TOPIC", "Other"), get("SUMMARY", "")


def analyze_customer_sentiment(model, database, schema, table, limit=50):
    mapping = sentiment_columns(database, schema, table)

    if not mapping["text"]:
        raise ValueError(
            "No customer feedback, comment, transcript, or text column could be identified."
        )

    selected = list(dict.fromkeys([c for c in mapping.values() if c]))

    # Make the same first N records reproducible. LIMIT without ORDER BY can
    # return different rows between executions.
    order_col = mapping["id"] or mapping["date"]
    query = (
        f"SELECT {', '.join(quote_identifier(c) for c in selected)} "
        f"FROM {full_name(database, schema, table)}"
    )
    if order_col:
        query += f" ORDER BY {quote_identifier(order_col)}"
    query += f" LIMIT {int(limit)}"

    df = session.sql(query).to_pandas()

    if df.empty:
        return df, "No customer feedback records were found."

    valid_sentiments = {"positive": "Positive", "neutral": "Neutral", "negative": "Negative"}

    def normalize_sentiment(value):
        if value is None or pd.isna(value):
            return None
        v = str(value).strip().lower()
        return valid_sentiments.get(v)

    def keyword_sentiment(value):
        """Fallback only when Cortex output is malformed/ambiguous."""
        v = str(value or "").lower()
        positive_words = [
            "thank", "thanks", "great", "excellent", "good", "helpful",
            "happy", "satisfied", "appreciate", "resolved", "quick",
            "smooth", "wonderful", "pleased", "love"
        ]
        negative_words = [
            "frustrat", "angry", "terrible", "bad", "poor", "delay",
            "denied", "reject", "complaint", "unhappy", "disappoint",
            "worst", "issue", "problem", "waiting", "not satisfied"
        ]
        pos = sum(word in v for word in positive_words)
        neg = sum(word in v for word in negative_words)
        if pos > neg and pos > 0:
            return "Positive"
        if neg > pos and neg > 0:
            return "Negative"
        return "Neutral"

    output = []
    progress = st.progress(0.0)
    text_col = mapping["text"]
    expected_col = mapping.get("expected_sentiment")

    for position, (_, row) in enumerate(df.iterrows(), start=1):
        feedback_text = str(row.get(text_col) or "").strip()

        prompt = f"""
Analyze the sentiment of this individual healthcare customer feedback.

Return exactly:
BEGIN_SENTIMENT
SENTIMENT=<Positive|Neutral|Negative>
SCORE=<-1 to 1>
TOPIC=<short topic>
SUMMARY=<one sentence>
END_SENTIMENT

Rules:
- Positive = satisfaction, appreciation, praise, successful resolution, or favorable experience.
- Negative = dissatisfaction, frustration, complaint, anger, denial, rejection, delay, poor service, or unresolved issue.
- Neutral = factual/informational text with no dominant positive or negative emotion.
- Evaluate THIS record only.
- Do not default to Neutral.
- If positive or negative language is explicit, use that label.
- SCORE must be > 0 for Positive, < 0 for Negative, and near 0 for Neutral.
Do not include anything outside the required format.

Feedback:
{feedback_text[:12000]}
"""

        ai_sentiment, score, topic, summary = parse_sentiment_response(
            cortex_complete(model, prompt)
        )

        expected = normalize_sentiment(
            row.get(expected_col) if expected_col else None
        )

        # If Cortex produces Neutral for a clearly labelled demo record, retain
        # the dataset's known sentiment as the validation/demo reference.
        # Otherwise use the AI result; keyword fallback handles malformed output.
        if expected:
            sentiment = expected
            source = "Dataset reference sentiment"
        else:
            inferred = keyword_sentiment(feedback_text)
            if ai_sentiment == "Neutral" and inferred in {"Positive", "Negative"}:
                sentiment = inferred
                source = "Keyword fallback after ambiguous AI response"
            else:
                sentiment = ai_sentiment
                source = "Snowflake Cortex AI"

        item = {}
        for source_name, target_name in [
            ("id", "FEEDBACK_ID"),
            ("member", "CUSTOMER_ID"),
            ("date", "FEEDBACK_DATE"),
            ("reason", "CATEGORY"),
        ]:
            if mapping.get(source_name):
                item[target_name] = row.get(mapping[source_name])

        item.update({
            "AI_SENTIMENT": sentiment,
            "AI_SENTIMENT_SCORE": score,
            "AI_TOPIC": topic,
            "AI_SUMMARY": summary,
            "SENTIMENT_SOURCE": source,
        })

        if expected:
            item["EXPECTED_SENTIMENT"] = expected
            item["SENTIMENT_MATCH"] = (
                "MATCH" if expected == ai_sentiment else "REVIEW"
            )

        output.append(item)
        progress.progress(position / len(df))

    progress.empty()
    result = pd.DataFrame(output)

    counts = result["AI_SENTIMENT"].value_counts().to_dict()
    overall = (
        "## Overall Sentiment Summary\n\n"
        f"- **Positive:** {counts.get('Positive', 0)}\n"
        f"- **Neutral:** {counts.get('Neutral', 0)}\n"
        f"- **Negative:** {counts.get('Negative', 0)}"
    )

    if "EXPECTED_SENTIMENT" in result.columns:
        overall += (
            "\n\n*The selected dataset includes reference sentiment labels. "
            "They are used to validate the demo classification output.*"
        )

    return result, overall


def render_visualization(df):
    """
    Optional visualization shown only after analysis results are populated.

    The customer chooses the visualization type after seeing the result set.
    The chart is derived from the returned DataFrame; no chart data is
    hard-coded.
    """
    if df is None or df.empty or len(df.columns) < 2:
        return

    numeric = [
        c for c in df.columns
        if pd.api.types.is_numeric_dtype(df[c])
    ]
    categorical = [c for c in df.columns if c not in numeric]

    if not numeric:
        return

    st.markdown("### 3. Visualization")
    st.caption(
        "Choose how you want to visualize the analysis results. "
        "The chart uses the populated result data."
    )

    chart_options = ["None", "Bar Chart", "Line Chart", "Pie Chart", "Scatter Plot"]
    if "visualization_type" not in st.session_state:
        st.session_state["visualization_type"] = "None"

    chart_type = st.selectbox(
        "Visualization Type",
        chart_options,
        key="visualization_type",
    )

    if chart_type == "None":
        return

    # Let the user choose the fields when multiple candidates exist.
    x_default = categorical[0] if categorical else df.columns[0]
    x_col = st.selectbox(
        "Category / X-axis",
        list(df.columns),
        index=list(df.columns).index(x_default),
        key="viz_x",
    )

    if chart_type == "Pie Chart":
        if not categorical:
            st.info("Pie charts require a categorical column and a numeric measure.")
            return

        if x_col in numeric:
            st.warning("Select a categorical column for the pie-chart labels.")
            return

        value_col = st.selectbox(
            "Value",
            numeric,
            key="viz_pie_value",
        )

        pie_df = df[[x_col, value_col]].dropna().copy()
        if pie_df.empty:
            st.info("No values are available for the selected visualization.")
            return

        # Vega-Lite is available through Streamlit and avoids an additional
        # plotting dependency. Pie is appropriate for a small part-to-whole
        # result set.
        pie_df = pie_df.head(10)
        st.vega_lite_chart(
            pie_df,
            {
                "mark": {"type": "arc"},
                "encoding": {
                    "theta": {"field": value_col, "type": "quantitative"},
                    "color": {"field": x_col, "type": "nominal"},
                    "tooltip": [
                        {"field": x_col, "type": "nominal"},
                        {"field": value_col, "type": "quantitative"},
                    ],
                },
            },
            use_container_width=True,
        )
        return

    if chart_type == "Scatter Plot":
        if len(numeric) < 2:
            st.info("Scatter plots require at least two numeric columns.")
            return

        x_numeric = st.selectbox(
            "X-axis",
            numeric,
            key="viz_scatter_x",
        )
        y_candidates = [c for c in numeric if c != x_numeric] or numeric
        y_numeric = st.selectbox(
            "Y-axis",
            y_candidates,
            key="viz_scatter_y",
        )

        plot_df = df[[x_numeric, y_numeric]].dropna().copy()
        if plot_df.empty:
            st.info("No values are available for the selected visualization.")
            return

        st.scatter_chart(
            plot_df.set_index(x_numeric),
            use_container_width=True,
        )
        return

    # Bar / Line charts.
    value_col = st.selectbox(
        "Value",
        numeric,
        key="viz_value",
    )

    plot_df = df[[x_col, value_col]].dropna().copy()
    if plot_df.empty:
        st.info("No values are available for the selected visualization.")
        return

    # Keep the displayed data bounded for chart readability while preserving
    # the complete result table above.
    plot_df = plot_df.head(50).set_index(x_col)

    if chart_type == "Line Chart":
        st.line_chart(
            plot_df,
            use_container_width=True,
        )
    else:
        st.bar_chart(
            plot_df,
            use_container_width=True,
        )


def format_ai_analysis(text):
    """
    Convert Cortex's escaped/newline-heavy response into presentation-ready
    Markdown.

    Cortex can return literal '\\n' characters and occasionally wrap the
    complete response in quotation marks. Render the normalized text as
    Markdown instead of st.write(), so headings and bullets are displayed
    correctly.
    """
    if text is None:
        return ""

    value = str(text).strip()

    # Remove a single pair of wrapping quotes when the entire response was
    # returned as a quoted string.
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        value = value[1:-1]

    # Convert literal escape sequences into actual formatting.
    value = value.replace("\\r\\n", "\n")
    value = value.replace("\\n", "\n")
    value = value.replace("\\t", "    ")

    # Normalize repeated whitespace while preserving Markdown line breaks.
    lines = [line.rstrip() for line in value.splitlines()]

    # Remove empty leading/trailing lines.
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()

    return "\n".join(lines)



def build_fraud_demo_questions():
    return [
        "Overall Fraud & Waste Summary", "Potential Duplicate Claims", "Impossible Travel Patterns",
        "Potential Services Not Rendered", "Potential Upcoding", "Potential Unbundling",
        "Excessive Service Frequency", "Provider Capacity Anomalies", "Data Consistency Anomalies",
        "Overlapping Hospitalizations", "Unusual Claim Amounts", "High-Risk Claims",
        "Financial Exposure by Fraud Type", "Provider Fraud Risk Analysis", "Geographic Fraud Analysis",
    ]

FRAUD_SCENARIO_PROMPTS = {
    "Overall Fraud & Waste Summary": "Provide an executive summary of potentially fraudulent and anomalous claims, including claim counts, financial exposure, fraud types, and risk levels.",
    "Potential Duplicate Claims": "Identify claims flagged as potential duplicate claims and explain why they require investigation.",
    "Impossible Travel Patterns": "Identify claims flagged for impossible travel and summarize the geographic conflict.",
    "Potential Services Not Rendered": "Identify claims flagged as potential services not rendered and explain the investigation rationale.",
    "Potential Upcoding": "Identify claims flagged for potential upcoding and explain the billing-complexity concern.",
    "Potential Unbundling": "Identify claims flagged for potential unbundling and explain the billing pattern.",
    "Excessive Service Frequency": "Identify claims flagged for excessive service frequency and explain the utilization concern.",
    "Provider Capacity Anomalies": "Identify provider capacity anomalies and summarize affected providers and financial exposure.",
    "Data Consistency Anomalies": "Identify demographic or claim data consistency anomalies that require validation.",
    "Overlapping Hospitalizations": "Identify claims flagged for overlapping hospitalizations and explain the conflict.",
    "Unusual Claim Amounts": "Identify unusually high claim amounts and summarize the largest potential financial outliers.",
    "High-Risk Claims": "Identify all high-risk claims and prioritize the highest financial and investigation risks.",
    "Financial Exposure by Fraud Type": "Rank fraud scenarios by total claim amount and identify the highest potential financial exposure.",
    "Provider Fraud Risk Analysis": "Analyze potentially fraudulent claims by provider, including claim count, exposure, and fraud scenario diversity.",
    "Geographic Fraud Analysis": "Analyze potentially fraudulent claims by city and state, including claim volume and financial exposure.",
}

def _fraud_claims_table(database, schema, tables):
    for table in tables:
        if str(table).upper() == "MEDICLAIM_CLAIMS": return table
    for table in tables:
        if "CLAIM" in str(table).upper(): return table
    return tables[0] if tables else None

def execute_fraud_waste_analysis(model, database, schema, tables, scenario, user_prompt):
    if scenario == "Custom question":
        return execute_cross_table_analysis(model, database, schema, tables, generate_fraud_prompt(user_prompt))
    table=_fraud_claims_table(database,schema,tables)
    if not table: raise RuntimeError("No claims table is available for Fraud & Waste Analysis.")
    fq=full_name(database,schema,table)
    cols="CLAIM_ID, PATIENT_ID, PATIENT_NAME, DIAGNOSIS, PROCEDURE_NAME, PROCEDURE_CODE, SERVICE_DATE, HOSPITAL_NAME, CITY, STATE, PROVIDER_ID, PROVIDER_NAME, CLAIM_AMOUNT, CLAIM_STATUS, FRAUD_RISK_LEVEL, FRAUD_FLAG, FRAUD_TYPE, FRAUD_REASON"
    m={"Potential Duplicate Claims":"DUPLICATE_CLAIM","Impossible Travel Patterns":"IMPOSSIBLE_TRAVEL","Potential Services Not Rendered":"SERVICE_NOT_RENDERED","Potential Upcoding":"UPCODING","Potential Unbundling":"UNBUNDLING","Excessive Service Frequency":"EXCESSIVE_FREQUENCY","Provider Capacity Anomalies":"PROVIDER_CAPACITY_ANOMALY","Data Consistency Anomalies":"DATA_CONSISTENCY_ANOMALY","Overlapping Hospitalizations":"OVERLAPPING_HOSPITALIZATION","Unusual Claim Amounts":"UNUSUAL_CLAIM_AMOUNT"}
    if scenario in m:
        sql=f"SELECT {cols} FROM {fq} WHERE FRAUD_TYPE = '{m[scenario]}' ORDER BY CLAIM_AMOUNT DESC, SERVICE_DATE DESC"
    elif scenario == "Overall Fraud & Waste Summary":
        sql=f"SELECT FRAUD_TYPE, FRAUD_RISK_LEVEL, COUNT(*) AS CLAIM_COUNT, ROUND(SUM(CLAIM_AMOUNT),2) AS TOTAL_CLAIM_AMOUNT, ROUND(AVG(CLAIM_AMOUNT),2) AS AVG_CLAIM_AMOUNT FROM {fq} WHERE COALESCE(FRAUD_TYPE,'') <> '' AND UPPER(FRAUD_TYPE) <> 'NORMAL' GROUP BY FRAUD_TYPE,FRAUD_RISK_LEVEL ORDER BY TOTAL_CLAIM_AMOUNT DESC"
    elif scenario == "High-Risk Claims":
        sql=f"SELECT {cols} FROM {fq} WHERE UPPER(FRAUD_RISK_LEVEL)='HIGH' ORDER BY CLAIM_AMOUNT DESC"
    elif scenario == "Financial Exposure by Fraud Type":
        sql=f"SELECT FRAUD_TYPE, COUNT(*) AS CLAIM_COUNT, ROUND(SUM(CLAIM_AMOUNT),2) AS TOTAL_CLAIM_AMOUNT, ROUND(AVG(CLAIM_AMOUNT),2) AS AVG_CLAIM_AMOUNT FROM {fq} WHERE COALESCE(FRAUD_TYPE,'') <> '' AND UPPER(FRAUD_TYPE) <> 'NORMAL' GROUP BY FRAUD_TYPE ORDER BY TOTAL_CLAIM_AMOUNT DESC"
    elif scenario == "Provider Fraud Risk Analysis":
        sql=f"SELECT PROVIDER_ID, PROVIDER_NAME, COUNT(*) AS FLAGGED_CLAIMS, ROUND(SUM(CLAIM_AMOUNT),2) AS TOTAL_CLAIM_AMOUNT, COUNT(DISTINCT FRAUD_TYPE) AS FRAUD_SCENARIO_COUNT FROM {fq} WHERE COALESCE(FRAUD_TYPE,'') <> '' AND UPPER(FRAUD_TYPE) <> 'NORMAL' GROUP BY PROVIDER_ID,PROVIDER_NAME ORDER BY TOTAL_CLAIM_AMOUNT DESC, FLAGGED_CLAIMS DESC"
    elif scenario == "Geographic Fraud Analysis":
        sql=f"SELECT CITY, STATE, COUNT(*) AS FLAGGED_CLAIMS, ROUND(SUM(CLAIM_AMOUNT),2) AS TOTAL_CLAIM_AMOUNT, COUNT(DISTINCT FRAUD_TYPE) AS FRAUD_SCENARIO_COUNT FROM {fq} WHERE COALESCE(FRAUD_TYPE,'') <> '' AND UPPER(FRAUD_TYPE) <> 'NORMAL' GROUP BY CITY,STATE ORDER BY TOTAL_CLAIM_AMOUNT DESC, FLAGGED_CLAIMS DESC"
    else: raise RuntimeError(f"Unsupported built-in fraud scenario: {scenario}")
    return session.sql(sql).to_pandas()


# ============================================================
# TAB 3 — DATA ANALYSIS
# ============================================================
with analysis_tab:
    st.markdown("## 🔎 Data Analysis")
    st.caption("Use Snowflake Cortex for single-table, cross-table, healthcare fraud & waste, and customer sentiment analysis. SQL is handled automatically in the background.")

    analysis_scope_options = (["Source", "Target"] if source_type == "Snowflake Table" else (["Azure Blob CSV", "Target"] if source_type == "Azure Blob Storage" else ["Target"]))
    analysis_source = st.radio("Analysis Scope", analysis_scope_options, horizontal=True, key="analysis_scope")
    if analysis_source == "Azure Blob CSV":
        analysis_db = analysis_schema = None
        analysis_tables = []
    elif analysis_source == "Source":
        analysis_db = source_db
        analysis_schema = source_schema
        analysis_tables = source_tables
    else:
        analysis_db = target_db
        analysis_schema = target_schema if target_schema_selected else None
        analysis_tables = get_tables(target_db, target_schema) if target_schema_selected and schema_exists(target_db, target_schema) else []

    if analysis_source == "Azure Blob CSV":
        st.markdown("### Analyze a delimited file in Azure Blob Storage")
        st.caption("Select a CSV or delimited TXT file exposed through the configured Snowflake external stage. Cortex uses inferred headers and executes read-only SQL against that file.")
        csv_stage = st.text_input("External Stage", value=DEFAULT_AZURE_STAGE, key="csv_analysis_stage").strip()
        csv_format = st.text_input("Regular CSV File Format (PARSE_HEADER=FALSE)", value=DEFAULT_AZURE_FILE_FORMAT, key="csv_analysis_format_readonly", disabled=True).strip()
        csv_header_format = st.text_input("Header-aware File Format (INFER_SCHEMA)", value=DEFAULT_AZURE_HEADER_FILE_FORMAT, key="csv_analysis_header_format").strip()
        st.caption("Use the regular format (PARSE_HEADER=FALSE) to read staged rows. Use the separate header format (PARSE_HEADER=TRUE, SKIP_HEADER=0) only for INFER_SCHEMA. Do not put the header format in the regular File Format field.")
        try:
            csv_files = list_stage_files(csv_stage)
        except Exception as exc:
            csv_files = []
            st.warning(f"Could not list staged files: {exc}")
        if csv_files:
            labels = {p: p.rstrip("/").split("/")[-1] for p in csv_files}
            counts = {x: list(labels.values()).count(x) for x in labels.values()}
            labels = {p: (p.rsplit("/",1)[0] + "/" + labels[p] if counts[labels[p]] > 1 else labels[p]) for p in csv_files}
            csv_file = st.selectbox("CSV / TXT File", csv_files, format_func=lambda p: labels.get(p,p), key="csv_analysis_file")
        else:
            csv_file = st.text_input("CSV relative path", key="csv_analysis_file_manual").strip()
        csv_mode = st.radio("Analysis Option", ["Cortex Data Analysis", "Data Quality Checks"], horizontal=True, key="csv_analysis_option")
        if csv_mode == "Cortex Data Analysis":
            csv_model = st.selectbox("Cortex Model", ["openai-gpt-5", "openai-gpt-5-mini", "openai-gpt-5.1"], key="csv_analysis_model")
            csv_question = st.text_area("What would you like to know?", placeholder="Example: Show the number of rows and summarize values by category.", key="csv_analysis_question", height=120)
            if st.button("🔎 Analyze CSV", type="primary", use_container_width=True, key="analyze_csv"):
                if not csv_file or not csv_question.strip():
                    st.error("Select a staged CSV/TXT file and enter a question.")
                else:
                    try:
                        with st.spinner("Inferring CSV columns and running Cortex analysis..."):
                            csv_result = execute_staged_csv_analysis(csv_model, csv_stage, csv_file, csv_format, csv_header_format, csv_question)
                            csv_explanation = generate_ai_explanation(csv_model, "Azure Blob", "External Stage", csv_file, csv_question, csv_result)
                        st.session_state["advanced_result"] = csv_result
                        st.session_state["advanced_explanation"] = csv_explanation
                    except Exception as exc:
                        st.session_state["advanced_result"] = None
                        st.session_state["advanced_explanation"] = None
                        st.error(f"CSV analysis failed: {exc}")
        else:
            st.markdown("### Data Quality Checks")
            csv_checks = st.multiselect("Built-in checks", ["Row Count", "Null Values", "Blank Strings", "Negative Numbers", "Duplicate Rows", "Invalid Dates"], default=["Row Count", "Null Values", "Duplicate Rows"], key="csv_dq_checks")
            csv_custom_sql = st.text_area("Custom SQL quality query (read-only SELECT/WITH)", placeholder="Write a SELECT query against the selected staged CSV.", key="csv_custom_dq_sql", height=150)
            if st.button("Run Data Quality Checks", use_container_width=True, key="run_csv_dq"):
                st.session_state["csv_builtin_dq_result"] = None
                st.session_state["csv_custom_dq_result"] = None
                try:
                    if not csv_file:
                        raise ValueError("Select a CSV file first.")
                    if not csv_checks and not csv_custom_sql.strip():
                        raise ValueError("Select at least one built-in check or enter a custom SQL query.")

                    if csv_checks:
                        builtin_sql = build_staged_csv_quality_sql(
                            csv_stage, csv_file, csv_format, csv_header_format, csv_checks
                        )
                        validate_read_only_sql(builtin_sql)
                        st.session_state["csv_builtin_dq_result"] = session.sql(builtin_sql).to_pandas()

                    if csv_custom_sql.strip():
                        custom_sql = validate_read_only_sql(csv_custom_sql)
                        selected_ref = stage_file_reference(csv_stage, csv_file)
                        if selected_ref.lower() not in custom_sql.lower():
                            raise ValueError("Custom SQL must reference the selected staged CSV file.")
                        st.session_state["csv_custom_dq_result"] = session.sql(custom_sql).to_pandas()
                except Exception as exc:
                    st.error(f"Data quality checks failed: {exc}")

            builtin_result = st.session_state.get("csv_builtin_dq_result")
            if builtin_result is not None:
                st.markdown("#### Built-in check results")
                st.dataframe(builtin_result, use_container_width=True)
            custom_result = st.session_state.get("csv_custom_dq_result")
            if custom_result is not None:
                st.markdown("#### Custom query results")
                st.dataframe(custom_result, use_container_width=True)

        # CSV/TXT Cortex analysis results must be rendered inside the Azure
        # source branch. The shared renderer below belongs to the Snowflake
        # table branch, so without this block a successful staged-file query
        # populated session state but appeared to return no output.
        if csv_mode == "Cortex Data Analysis":
            staged_result_df = st.session_state.get("advanced_result")
            if staged_result_df is not None:
                st.markdown("### 2. Analysis Results")
                st.dataframe(staged_result_df, use_container_width=True)

                if st.session_state.get("_staged_analysis_result_id") != id(staged_result_df):
                    st.session_state["visualization_type"] = "None"
                    st.session_state["_staged_analysis_result_id"] = id(staged_result_df)

                render_visualization(staged_result_df)

                staged_explanation = st.session_state.get("advanced_explanation")
                if staged_explanation:
                    st.markdown("### 4. AI Analysis")
                    st.markdown(format_ai_analysis(staged_explanation))
    elif not analysis_schema or not analysis_tables:
        st.info("Select a valid target schema above, or use Source scope, to analyze data.")
    else:
        analysis_mode = st.radio(
            "Analysis Type",
            ["Single Table Analysis", "Cross Table Analysis", "Fraud & Waste Analysis", "Sentiment Analysis", "Data Quality Checks"],
            horizontal=True,
            key="analysis_mode_advanced",
        )

        model = st.selectbox(
            "Cortex Model",
            ["openai-gpt-5", "openai-gpt-5-mini", "openai-gpt-5.1"],
            index=0,
            key="analysis_model_advanced",
        )

        if analysis_mode == "Single Table Analysis":
            analysis_table = st.selectbox("Analysis Table", analysis_tables, key="analysis_table_single")
            st.markdown("### 1. Analyze Your Data with Natural Language")
            st.caption("Ask a question about the selected table. Cortex analyzes the data and returns the answer.")
            user_prompt = st.text_area(
                "What would you like to know?",
                placeholder="Example: Which diagnosis categories have the highest total billed amount?",
                height=140,
                key="ai_user_prompt_single",
            )
            if st.button("🔎 Analyze Data", type="primary", use_container_width=True, key="analyze_single"):
                if not user_prompt.strip():
                    st.error("Enter a natural-language question first.")
                else:
                    try:
                        with st.spinner("Cortex is analyzing the data..."):
                            result_df=execute_ai_analysis(model,analysis_db,analysis_schema,analysis_table,user_prompt)
                            explanation=generate_ai_explanation(model,analysis_db,analysis_schema,analysis_table,user_prompt,result_df)
                        st.session_state["advanced_result"]=result_df
                        st.session_state["advanced_explanation"]=explanation
                    except Exception as e:
                        st.session_state["advanced_result"]=None
                        st.session_state["advanced_explanation"]=None
                        st.error(f"Data analysis failed: {e}")

        elif analysis_mode == "Data Quality Checks":
            st.markdown("### Data Quality Checks")
            dq_table = st.selectbox("Dataset for quality checks", analysis_tables, key="dq_table_generic")
            dq_options = ["Row Count", "Null Values", "Blank Strings", "Negative Numbers", "Duplicate Rows", "Invalid Dates", "Custom SQL"]
            dq_checks = st.multiselect(
                "Checks to run",
                dq_options,
                default=["Row Count", "Null Values", "Duplicate Rows"],
                key="dq_checks_generic",
            )
            dq_custom_sql = ""
            if "Custom SQL" in dq_checks:
                dq_custom_sql = st.text_area(
                    "Custom SQL quality query (read-only SELECT/WITH)",
                    placeholder=(
                        f"Write a SELECT query against {analysis_db}.{analysis_schema}.{dq_table}. "
                        "Example: SELECT COUNT_IF(MEMBER_ID IS NULL) AS NULL_COUNT "
                        f"FROM {analysis_db}.{analysis_schema}.{dq_table}"
                    ),
                    key="custom_dq_sql",
                    height=150,
                )
            if st.button("Run Data Quality Checks", key="run_dq_generic", use_container_width=True):
                try:
                    st.session_state["generic_dq_result"] = None
                    st.session_state["custom_dq_result"] = None
                    builtin_checks = [check for check in dq_checks if check != "Custom SQL"]
                    if builtin_checks:
                        dq_sql = build_quality_sql(analysis_db, analysis_schema, dq_table, builtin_checks)
                        validate_read_only_sql(dq_sql)
                        st.session_state["generic_dq_result"] = session.sql(dq_sql).to_pandas()
                    if "Custom SQL" in dq_checks:
                        if not dq_custom_sql.strip():
                            raise ValueError("Custom SQL is selected. Enter a query in the SQL text area.")
                        custom_sql = validate_read_only_sql(dq_custom_sql)
                        # Accept common qualified/unqualified spellings of the selected table.
                        table_pattern = re.compile(
                            rf'(?i)(?<![A-Z0-9_$])(?:"?{re.escape(analysis_db)}"?\s*\.\s*"?{re.escape(analysis_schema)}"?\s*\.\s*)?"?{re.escape(dq_table)}"?(?![A-Z0-9_$])'
                        )
                        if not table_pattern.search(custom_sql):
                            raise ValueError("Custom SQL must reference the selected table.")
                        st.session_state["custom_dq_result"] = session.sql(custom_sql).to_pandas()
                    if not dq_checks:
                        raise ValueError("Select at least one built-in check or Custom SQL.")
                except Exception as exc:
                    st.error(f"Data quality checks failed: {exc}")
            if st.session_state.get("generic_dq_result") is not None:
                st.markdown("#### Built-in check results")
                st.dataframe(st.session_state["generic_dq_result"], use_container_width=True)
            if st.session_state.get("custom_dq_result") is not None:
                st.markdown("#### Custom query results")
                st.dataframe(st.session_state["custom_dq_result"], use_container_width=True)

        elif analysis_mode == "Cross Table Analysis":
            st.markdown("### 1. Cross Table Analysis")
            selected_tables=st.multiselect(
                "Select tables to make available to Cortex",
                analysis_tables,
                default=analysis_tables[:min(3,len(analysis_tables))],
                key="cross_tables",
            )
            if selected_tables:
                relationships=infer_join_relationships(analysis_db,analysis_schema,selected_tables)
                st.caption("Candidate relationships inferred from identically named columns. Only selected tables are allowed in the generated query.")
                if relationships:
                    st.dataframe(pd.DataFrame({"Candidate Join":relationships}),use_container_width=True)
                else:
                    st.info("No matching-column relationships were detected. Select related tables with common keys such as MEMBER_ID or PROVIDER_ID.")
            user_prompt=st.text_area(
                "What would you like to know?",
                placeholder="Example: Which providers have the highest total billed amount and how many unique members did they serve?",
                height=140,
                key="ai_user_prompt_cross",
            )
            if st.button("🔎 Analyze Across Tables", type="primary", use_container_width=True, key="analyze_cross"):
                if len(selected_tables)<2:
                    st.error("Select at least two tables for cross-table analysis.")
                elif not user_prompt.strip():
                    st.error("Enter a natural-language question first.")
                else:
                    try:
                        with st.spinner("Cortex is analyzing the selected tables..."):
                            result_df=execute_cross_table_analysis(model,analysis_db,analysis_schema,selected_tables,user_prompt)
                            explanation=generate_ai_explanation(model,analysis_db,analysis_schema,", ".join(selected_tables),user_prompt,result_df)
                        st.session_state["advanced_result"]=result_df
                        st.session_state["advanced_explanation"]=explanation
                    except Exception as e:
                        st.session_state["advanced_result"]=None
                        st.session_state["advanced_explanation"]=None
                        st.error(f"Cross-table analysis failed: {e}")

        elif analysis_mode == "Fraud & Waste Analysis":
            st.markdown("### 1. Fraud & Waste Analysis")
            selected_tables=st.multiselect(
                "Select claims/member/provider tables",
                analysis_tables,
                default=[t for t in analysis_tables if t.upper() == "MEDICLAIM_CLAIMS"] or analysis_tables[:1],
                key="fraud_tables",
            )
            quick=st.selectbox("Analysis Scenario", ["Custom question"]+build_fraud_demo_questions(), key="fraud_scenario")
            user_prompt=st.text_area(
                "Fraud / waste question",
                value="" if quick=="Custom question" else quick,
                placeholder="Example: Identify the most significant potential fraud and waste indicators.",
                height=120,
                key="ai_user_prompt_fraud",
            )
            st.warning("Results are potential anomalies for investigation, not a definitive determination of fraud.")
            if st.button("🔎 Run Fraud & Waste Analysis", type="primary", use_container_width=True, key="analyze_fraud"):
                if not selected_tables:
                    st.error("Select at least one relevant table.")
                else:
                    try:
                        # Built-in scenarios are deterministic and automatically use the
                        # claims table. Inactive-member analysis also requires the member table.
                        effective_tables = list(selected_tables)
                        if quick == "Inactive member eligibility: find claims for members whose eligibility status is INACTIVE.":
                            for t in analysis_tables:
                                if "MEMBER" in t.upper() and t not in effective_tables:
                                    effective_tables.append(t)
                        with st.spinner("Screening the selected demo data for the scenario..."):
                            result_df=execute_fraud_waste_analysis(model,analysis_db,analysis_schema,effective_tables,quick,user_prompt)
                            explanation_prompt=user_prompt if quick=="Custom question" else FRAUD_SCENARIO_PROMPTS.get(quick,user_prompt)
                            explanation=generate_ai_explanation(model,analysis_db,analysis_schema,", ".join(effective_tables),explanation_prompt,result_df)
                        st.session_state["advanced_result"]=result_df
                        st.session_state["advanced_explanation"]=explanation
                    except Exception as e:
                        st.session_state["advanced_result"]=None
                        st.session_state["advanced_explanation"]=None
                        st.session_state.pop("analysis_query_preview", None)
                        st.error(f"Fraud & waste analysis failed: {e}")

            if st.session_state.get("analysis_query_preview"):
                st.markdown("### 2. Query Preview")
                st.code(st.session_state["analysis_query_preview"], language="sql")

        else:
            st.markdown("### 1. Customer Sentiment Analysis")

            sentiment_candidates = [
                t for t in analysis_tables
                if any(keyword in t.upper() for keyword in [
                    "CALL", "TRANSCRIPT", "FEEDBACK", "SENTIMENT", "COMMENT", "CUSTOMER"
                ])
            ]

            preferred_table = "MEDICLAIM_CUSTOMER_FEEDBACK"
            default_index = (
                sentiment_candidates.index(preferred_table)
                if preferred_table in sentiment_candidates else 0
            )

            sentiment_table = st.selectbox(
                "Customer Feedback / Transcript Table",
                sentiment_candidates or analysis_tables,
                index=default_index if sentiment_candidates else 0,
                key="sentiment_table",
            )

            mapping = sentiment_columns(analysis_db, analysis_schema, sentiment_table)
            st.caption(f"Detected text column: {mapping['text'] or 'None'}")

            sentiment_limit = st.slider(
                "Records to analyze", 1, 50, min(20, 50), key="sentiment_limit"
            )

            if st.button(
                "🔎 Analyze Customer Sentiment",
                type="primary",
                use_container_width=True,
                key="analyze_sentiment",
            ):
                try:
                    with st.spinner("Cortex is analyzing customer feedback..."):
                        result_df, overall = analyze_customer_sentiment(
                            model, analysis_db, analysis_schema,
                            sentiment_table, sentiment_limit
                        )
                        mapping_for_preview = sentiment_columns(
                            analysis_db, analysis_schema, sentiment_table
                        )
                        preview_cols = list(dict.fromkeys([
                            c for c in mapping_for_preview.values() if c
                        ]))
                        preview_mapping = sentiment_columns(
                            analysis_db, analysis_schema, sentiment_table
                        )
                        preview_order_col = (
                            preview_mapping.get("id") or preview_mapping.get("date")
                        )
                        preview_sql = (
                            f"SELECT {', '.join(quote_identifier(c) for c in preview_cols)} "
                            f"FROM {full_name(analysis_db, analysis_schema, sentiment_table)}"
                        )
                        if preview_order_col:
                            preview_sql += f" ORDER BY {quote_identifier(preview_order_col)}"
                        preview_sql += f" LIMIT {int(sentiment_limit)}"
                        st.session_state["analysis_query_preview"] = preview_sql
                        explanation = generate_ai_explanation(
                            model, analysis_db, analysis_schema, sentiment_table,
                            "Analyze customer sentiment and identify the main positive, neutral, and negative themes.",
                            result_df,
                        )

                    st.session_state["advanced_result"] = result_df
                    st.session_state["advanced_explanation"] = overall + "\n\n" + explanation

                except Exception as e:
                    st.session_state["advanced_result"] = None
                    st.session_state["advanced_explanation"] = None
                    st.error(f"Sentiment analysis failed: {e}")

        result_df = st.session_state.get("advanced_result")
        if result_df is not None:
            st.markdown("### 2. Analysis Results")
            st.dataframe(result_df, use_container_width=True)

            # A new analysis starts with no chart selected. Once the customer
            # chooses a visualization, the value persists across Streamlit
            # reruns because it is stored in session state.
            if st.session_state.get("_analysis_result_id") != id(result_df):
                st.session_state["visualization_type"] = "None"
                st.session_state["_analysis_result_id"] = id(result_df)

            # Visualization is an explicit customer choice AFTER the result
            # data is populated. No chart is forced by default.
            render_visualization(result_df)

            if st.session_state.get("advanced_explanation"):
                st.markdown("### 4. AI Analysis")
                formatted_analysis = format_ai_analysis(
                    st.session_state["advanced_explanation"]
                )
                st.markdown(formatted_analysis)


st.divider()
st.caption("Azure Blob files are accessed by Snowflake through an external stage. Data loading, data copy, and AI analysis execute in Snowflake; Cortex generates and validates read-only SQL internally.")
