"""input_features_dai module for input SQL, used for factor and feature extraction, data labeling, etc."""

import re
import uuid
from collections import OrderedDict

import structlog

from bigmodule import I  # noqa: N812

logger = structlog.get_logger()

# metadata
# Module author
author = "BigQuant"
# Module category
category = "Data"
# Module display name
friendly_name = "Input Features (DAI SQL)"
# Documentation URL, optional
doc_url = "wiki/doc/aistudio-aiide-NzAjgKapzW#h-Input-Features-Dai-SQL"
# Whether to automatically cache results
cacheable = True

MODES = OrderedDict(
    [
        ("Expression", "expr"),
        ("SQL", "sql"),
    ]
)
MODE0 = list(MODES.keys())[0]

DEFAULT_SQL = """-- Use DAI SQL to get data, build factors, etc., the following is an example for reference
-- Use fields from data inputs 1/2/3: e.g. input_1.close, input_1.* EXCLUDE(date, instrument)

SELECT
    -- Enter factor expressions here

    m_lag(close, 90) / close AS return_90,
    m_lag(close, 30) / close AS return_30,
    -- Columns starting with underscores are intermediate variables, not included in final output (e.g. _rank_return_90)
    c_pct_rank(-return_90) AS _rank_return_90,
    c_pct_rank(return_30) AS _rank_return_30,

    c_rank(volume) AS rank_volume,
    close / m_lag(close, 1) as return_0,

    -- Date and stock code
    date, instrument
FROM
    cn_stock_prefactors
    -- SQL mode does not automatically join input data sources, can be used freely as needed
    -- JOIN input_1 USING(date, instrument)
WHERE
    -- WHERE filtering, executed before window functions
    -- Exclude ST stocks
    st_status = 0
QUALIFY
    -- QUALIFY filtering, executed after window functions, e.g. m_lag(close, 3) AS close_3, filters on close_3 should be placed here
    -- Remove rows with null values
    COLUMNS(*) IS NOT NULL
    -- _rank_return_90 is a result of a window function, needs to be placed in QUALIFY
    AND _rank_return_90 > 0.1
    AND _rank_return_30 < 0.1
-- Order by date and stock code, ascending
ORDER BY date, instrument
"""

DEFAULT_EXPR = """
-- Data usage: table_name.column_name, for columns without specified table names, will infer from expr_tables, if the same column appears in multiple tables, need to specify the table name explicitly

m_lag(close, 90) / close AS return_90
m_lag(close, 30) / close AS return_30
-- cn_stock_bar1d.close / cn_stock_bar1d.open
-- cn_stock_prefactors.pe_ttm

-- In expression mode, automatically join input data 1/2/3, can directly use their fields. Including all columns of input_1 except date, instrument. Note that field names cannot be duplicated, otherwise it will cause errors
-- input_1.* EXCLUDE(date, instrument)
-- input_1.close
-- input_2.close / input_1.close
"""

DEFAULT_EXPR_FILTERS = """
-- Filtering in expression mode is placed in QUALIFY, i.e., data query, calculation, and finally filtering conditions

-- c_pct_rank(-return_90) <= 0.3
-- c_pct_rank(return_30) <= 0.3
-- cn_stock_bar1d.turn > 0.02
"""

# Remove string content within single quotes: instrument in ('jm2201.DCE') to avoid extracting jm2201
REMOVE_STRING_RE = re.compile(r"'[^']*'")
TABLE_NAME_RE = re.compile(r"(?<!\.)\b[a-zA-Z_]\w*\b(?=\.[a-zA-Z_*])")

EXPR_SQL_TEMPLATE = """
SELECT
    {expr}
FROM {tables}
{qualify}
{order_by}
"""


def _ds_to_table(ds) -> dict:
    if isinstance(ds, str):
        sql = ds
    else:
        type_ = ds.type
        if type_ == "json":
            sql = ds.read()["sql"]
        elif type == "text":
            sql = ds.read()
        else:
            # bdb
            return {"sql": "", "table_id": ds.id}

    import bigdb

    table_id = f"_t_{uuid.uuid4().hex}"
    parts = [x.strip().strip(";") for x in bigdb.connect().parse_query(sql)]
    parts[-1] = f"CREATE TABLE {table_id} AS {parts[-1]}"
    sql = ";\n".join(parts)
    if sql:
        sql += ";\n"

    return {
        "sql": sql,
        "table_id": table_id,
    }


def _ds_to_tables(inputs) -> dict:
    sql = ""
    tables = []
    input_tables = []
    for i, x in enumerate(inputs):
        if x is None:
            continue
        table = _ds_to_table(x)
        table["name"] = f"input_{i+1}"
        tables.append(table)

    return {
        "items": tables,
        "map": {x["name"]: x for x in tables},
        "sql": "".join([x["sql"] for x in tables]),
    }


def _split_expr(expr):
    lines = []
    for line in expr.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("--") or line.startswith("#"):
            continue
        lines.append(line)

    return lines


def _build_sql_from_expr(expr: str, expr_filters: str, default_tables="", order_by="", expr_drop_na=True, input_tables={}):
    expr_lines = _split_expr(expr)
    filter_lines = _split_expr(expr_filters)

    # collect all tables, join them
    tables = [x.strip() for x in default_tables.split(";") if x.strip()] + [x["table_id"] for x in input_tables["items"]]
    for line in expr_lines + filter_lines:
        tables += TABLE_NAME_RE.findall(REMOVE_STRING_RE.sub('', line))
    # de-dup and add using primary key
    join_usings = {}
    table_set = set()
    table_list = []
    for x in tables:
        if " USING(" in x:
            s = x.split(" ", 1)
            # input_* table
            if s[0] in input_tables["map"]:
                s[0] = input_tables["map"][s[0]]["table_id"]
            join_usings[s[0]] = s[1]
            x = s[0]
        if x in input_tables["map"]:
            x = input_tables["map"][x]["table_id"]
        # TODO: process x is input_*
        if x not in table_set:
            table_list.append(x)
        table_set.add(x)
    for i in range(1, len(table_list)):
        table_list[i] += " " + join_usings.get(table_list[i], "USING(date, instrument)").strip()
    tables = "\n    JOIN ".join(table_list)

    # Build filtering and place it in QUALIFY
    if expr_drop_na:
        filter_lines.append("COLUMNS(*) IS NOT NULL")
    qualify = ""
    if filter_lines:
        qualify = "QUALIFY\n    " + "\n    AND ".join(filter_lines)

    # ORDER BY date, instrument
    if order_by:
        order_by = f"ORDER BY {order_by}"

    sql = EXPR_SQL_TEMPLATE.format(expr=",\n    ".join(expr_lines), tables=tables, qualify=qualify, order_by=order_by)

    return sql


def _create_ds_from_sql(sql: str, extract_data: bool, base_ds=None):
    import dai

    if extract_data:
        logger.info("extract data ..")
        try:
            df = dai.query(sql).df()
        except:
            logger.error(f"dai query failed: {sql}")
            raise
        logger.info(f"extracted {df.shape}.")
        ds = dai.DataSource.write_bdb(df, base_ds=base_ds)
    else:
        ds = dai.DataSource.write_json({"sql": sql}, base_ds=base_ds)

    return ds


def run(
    input_1: I.port("Data Input 1, if there is metadata extra, it will be passed to the output data", specific_type_name="DataSource", optional=True) = None,
    input_2: I.port("Data Input 2", specific_type_name="DataSource", optional=True) = None,
    input_3: I.port("Data Input 3", specific_type_name="DataSource", optional=True) = None,
    mode: I.choice("Input Mode", list(MODES.keys())) = MODE0,
    expr: I.code(
        "Expression features, build features through expressions, simple and easy to use",
        default=DEFAULT_EXPR,
        auto_complete_type="sql",
    ) = None,
    expr_filters: I.code(
        "Expression filters, each line is a condition, multiple conditions are AND related, conditions can use OR combination",
        default=DEFAULT_EXPR_FILTERS,
        auto_complete_type="sql",
    ) = None,
    expr_tables: I.str(
        "Expression-default tables, for fields without specified table names, default from these tables, only fill in necessary tables, can improve performance, multiple table names separated by semicolons"
    ) = "cn_stock_prefactors",
    extra_fields: I.str("Expression-other fields, other fields to include, merge with expr, non-feature fields generally placed here, multiple fields separated by commas") = "date, instrument",
    order_by: I.str("Expression-order by fields, order by fields e.g. date ASC, instrument DESC") = "date, instrument",
    expr_drop_na: I.bool("Expression-remove null values, remove rows containing null values, parameter for expression mode") = True,
    sql: I.code(
        "SQL features, in SQL mode, build features through SQL, more flexible, most comprehensive functionality.",
        default=DEFAULT_SQL,
        auto_complete_type="sql",
    ) = None,
    extract_data: I.bool("Extract data, whether to extract data, if extracted, returns a BDB DataSource containing DataFrame") = False,
) -> [I.port("Output (SQL file)", "data")]:

    input_tables = _ds_to_tables([input_1, input_2, input_3])

    if ";" in expr_tables:
        logger.warning("Detected Chinese semicolon in Expression-default tables parameter, please use English semicolon. Automatically replaced, please note next time, otherwise it may lead to unexpected errors.")
        expr_tables = expr_tables.replace("；", ";")

    mode = MODES[mode]
    if mode == "expr":
        logger.info("expr mode")
        # if "date" not in expr or "instrument" not in expr:
        #     logger.warning("not found date/instrument in expr, the new version will not add date, instrument by default")
        if expr is None:
            expr = ""

        if expr_filters is None:
            expr_filters = ""

        final_sql = _build_sql_from_expr(
            expr + "\n" + extra_fields.replace(",", "\n"), expr_filters, expr_tables, order_by=order_by, expr_drop_na=expr_drop_na, input_tables=input_tables
        )
        # if sql is not None:
        #     final_sql = sql.strip() + ";\n" + final_sql
    else:
        logger.info("sql mode")
        if sql is None:
            logger.error("In SQL mode, SQL feature input is empty!")
            raise

        final_sql = sql

    # Replace input_*
    for x in input_tables["items"]:
        final_sql = re.sub(rf'\b{x["name"]}\b', x["table_id"], final_sql)

    final_sql = input_tables["sql"] + final_sql

    # Use extra from the first input ds
    return I.Outputs(data=_create_ds_from_sql(final_sql, extract_data, input_1))


def post_run(outputs):
    """Post-run function"""
    return outputs