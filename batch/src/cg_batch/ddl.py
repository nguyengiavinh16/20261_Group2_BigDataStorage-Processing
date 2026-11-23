"""Read batch/ddl/iceberg_tables.sql as a list of Spark SQL statements (pure, used by jobs/apply_ddl, BAT-02)."""

import re
from pathlib import Path

from cryptoguard_common.paths import repo_root

DDL_FILE = Path("batch") / "ddl" / "iceberg_tables.sql"
_TABLE = re.compile(r"^CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([\w.]+)", re.IGNORECASE)
_NAMESPACE = re.compile(r"^CREATE\s+NAMESPACE\s+IF\s+NOT\s+EXISTS\s+([\w.]+)", re.IGNORECASE)


def default_ddl_path() -> Path:
    return repo_root() / DDL_FILE


def split_statements(sql: str) -> list[str]:
    """Drop `--` comments and split on `;`, ignoring both inside single-quoted strings."""
    statements, buf, in_str, i = [], [], False, 0
    while i < len(sql):
        ch = sql[i]
        if in_str:
            buf.append(ch)
            if ch == "'":
                in_str = False
        elif ch == "'":
            in_str = True
            buf.append(ch)
        elif sql.startswith("--", i):
            while i < len(sql) and sql[i] != "\n":
                i += 1
            continue
        elif ch == ";":
            statements.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    statements.append("".join(buf))
    return [" ".join(s.split()) for s in statements if s.strip()]


def with_catalog(statement: str, catalog: str) -> str:
    """The DDL file is written for catalog `cg`; rename it if config.iceberg.catalog_name differs."""
    if catalog == "cg":
        return statement
    return re.sub(r"\bcg\.(?=\w+)", f"{catalog}.", statement)


def created_object(statement: str) -> tuple[str, str] | None:
    """('table'|'namespace', identifier) for CREATE … IF NOT EXISTS statements, else None."""
    for kind, pattern in (("table", _TABLE), ("namespace", _NAMESPACE)):
        m = pattern.match(statement)
        if m:
            return kind, m.group(1)
    return None
