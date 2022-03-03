# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# Refer
#   https://docs.aws.amazon.com/athena/latest/ug/reserved-words.html
from typing import Set

ATHENA_RESERVED_DDL_KEYWORDS = [
    "all",
    "alter",
    "and",
    "array",
    "as",
    "authorization",
    "between",
    "bigint",
    "binary",
    "boolean",
    "both",
    "by",
    "case",
    "cashe",
    "cast",
    "char",
    "column",
    "conf",
    "constraint",
    "commit",
    "create",
    "cross",
    "cube",
    "current",
    "current_date",
    "current_timestamp",
    "cursor",
    "database",
    "date",
    "dayofweek",
    "decimal",
    "delete",
    "describe",
    "distinct",
    "double",
    "drop",
    "else",
    "end",
    "exchange",
    "exists",
    "extended",
    "external",
    "extract",
    "false",
    "fetch",
    "float",
    "floor",
    "following",
    "for",
    "foreign",
    "from",
    "full",
    "function",
    "grant",
    "group",
    "grouping",
    "having",
    "if",
    "import",
    "in",
    "inner",
    "insert",
    "int",
    "integer",
    "intersect",
    "interval",
    "into",
    "is",
    "join",
    "lateral",
    "left",
    "less",
    "like",
    "local",
    "macro",
    "map",
    "more",
    "none",
    "not",
    "null",
    "numeric",
    "of",
    "on",
    "only",
    "or",
    "order",
    "out",
    "outer",
    "over",
    "partialscan",
    "partition",
    "percent",
    "preceding",
    "precision",
    "preserve",
    "primary",
    "procedure",
    "range",
    "reads",
    "reduce",
    "regexp",
    "references",
    "revoke",
    "right",
    "rlike",
    "rollback",
    "rollup",
    "row",
    "rows",
    "select",
    "set",
    "smallint",
    "start",
    "table",
    "tablesample",
    "then",
    "time",
    "timestamp",
    "to",
    "transform",
    "trigger",
    "true",
    "truncate",
    "unbounded",
    "union",
    "uniquejoin",
    "update",
    "user",
    "using",
    "utc_timestamp",
    "values",
    "varchar",
    "views",
    "when",
    "where",
    "window",
    "with",
]


ATHENA_RESERVED_SELECT_STATEMENT_KEYWORDS = [
    "alter",
    "and",
    "as",
    "between",
    "by",
    "case",
    "cast",
    "constraint",
    "create",
    "cross",
    "cube",
    "current_date",
    "current_path",
    "current_time",
    "current_timestamp",
    "current_user",
    "deallocate",
    "delete",
    "describe",
    "distinct",
    "drop",
    "else",
    "end",
    "escape",
    "except",
    "execute",
    "exists",
    "extract",
    "false",
    "first",
    "for",
    "from",
    "full",
    "group",
    "grouping",
    "having",
    "in",
    "inner",
    "insert",
    "intersect",
    "into",
    "is",
    "join",
    "last",
    "left",
    "like",
    "localtime",
    "localtimestamp",
    "natural",
    "normalize",
    "not",
    "null",
    "of",
    "on",
    "or",
    "order",
    "outer",
    "prepare",
    "recursive",
    "right",
    "rollup",
    "select",
    "table",
    "then",
    "true",
    "unescape",
    "union",
    "unnest",
    "using",
    "values",
    "when",
    "where",
    "with",
]


# TODO move to execution/common module when bootstrapping is supported
def check_name_for_DDL(entity_name):
    # Refer
    #  https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html
    first_char = entity_name[0]
    if first_char == "_" or entity_name in ATHENA_RESERVED_DDL_KEYWORDS:
        return "`" + entity_name + "`"
    return entity_name


def check_name_for_DML(entity_name):
    # Refer
    #  https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html
    first_char = entity_name[0]
    # number check is required only in SELECT, CTAS and VIEW
    if first_char in [str(i) for i in range(10)] or entity_name in ATHENA_RESERVED_SELECT_STATEMENT_KEYWORDS:
        return '"' + entity_name + '"'
    return entity_name


def get_valid_chars_in_athena_entity_names() -> Set[str]:
    import string

    allowed_chars = set(string.ascii_lowercase)
    allowed_chars.update([str(c) for c in range(10)])
    allowed_chars.add("_")
    return allowed_chars


def to_athena_format(entity_name) -> str:
    # Refer
    #  https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html
    entity_name_lower = entity_name.lower().replace("-", "_")
    allowed_chars = get_valid_chars_in_athena_entity_names()
    alien_chars = set(entity_name_lower) - allowed_chars
    if alien_chars:
        raise ValueError(f"Please do not use the following chars {alien_chars!r} in Athena entity name {entity_name!r}")
    return entity_name_lower
