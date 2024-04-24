"""input_features_dai 模块输入 SQL，可用于因子和特征抽取、数据标注等"""

import re
import uuid
from collections import OrderedDict

import structlog

from bigmodule import I  # noqa: N812

logger = structlog.get_logger()

# metadata
# 模块作者
author = "BigQuant"
# 模块分类
category = "数据输入输出"
# 模块显示名
friendly_name = "输入特征(DAI SQL)"
# 文档地址, optional
doc_url = "https://bigquant.com/wiki/doc/aistudio-aiide-NzAjgKapzW#h-输入特征dai-sql"
# 是否自动缓存结果
cacheable = True

MODES = OrderedDict(
    [
        ("表达式", "expr"),
        ("SQL", "sql"),
    ]
)
MODE0 = list(MODES.keys())[0]

DEFAULT_SQL = """-- 使用DAI SQL获取数据，构建因子等，如下是一个例子作为参考
-- DAI SQL 语法: https://bigquant.com/wiki/doc/dai-PLSbc1SbZX#h-sql%E5%85%A5%E9%97%A8%E6%95%99%E7%A8%8B
-- 使用数据输入1/2/3里的字段: e.g. input_1.close, input_1.* EXCLUDE(date, instrument)

SELECT

    -- 在这里输入因子表达式
    -- DAI SQL 算子/函数: https://bigquant.com/wiki/doc/dai-PLSbc1SbZX#h-%E5%87%BD%E6%95%B0
    -- 数据&字段: 数据文档 https://bigquant.com/data/home

    c_rank(volume) AS rank_volume,
    close / m_lag(close, 1) as return_0,

    -- 日期和股票代码
    date, instrument
FROM
    -- 预计算因子 cn_stock_bar1d https://bigquant.com/data/datasources/cn_stock_bar1d
    cn_stock_bar1d
WHERE
    -- WHERE 过滤，在窗口等计算算子之前执行
    -- 剔除ST股票
    st_status = 0
QUALIFY
    -- QUALIFY 过滤，在窗口等计算算子之后执行，比如 m_lag(close, 3) AS close_3，对于 close_3 的过滤需要放到这里
    -- 去掉有空值的行
    COLUMNS(*) IS NOT NULL
-- 按日期和股票代码排序，从小到大
ORDER BY date, instrument
"""

DEFAULT_EXPR = """-- DAI SQL 算子/函数: https://bigquant.com/wiki/doc/dai-PLSbc1SbZX#h-%E5%87%BD%E6%95%B0
-- 数据&字段: 数据文档 https://bigquant.com/data/home
-- 数据使用: 表名.字段名, 对于没有指定表名的列，会从 expr_tables 推断
-- 给输出数据列命名: AS field_name
-- 使用数据输入1/2/3里的字段: e.g. input_1.close, input_1.* EXCLUDE(date, instrument)
-- 在这里输入表达式, 每行一个表达式, 会根据这个输入解析表名并构建查询和计算SQL,

cn_stock_bar1d.close / cn_stock_bar1d.open AS daily_change
c_rank(cn_stock_valuation.total_market_cap) AS rank_total_market_cap
m_lag(rank_total_market_cap, 2) AS rank_total_market_cap_2
"""

TABLE_NAME_RE = re.compile(r"(?<!\.)\b[a-zA-Z_]\w*\b(?=\.[a-zA-Z_*])")

EXPR_SQL_TEMPLATE = """
SELECT
    {expr},
    date,
    instrument
FROM {tables}
{qualify}
ORDER BY date, instrument
"""


def _build_table(ds) -> dict:
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


def _build_sql_from_expr(expr: str, default_tables="", expr_drop_na=True, multi_expr=True):
    lines = []
    for line in expr.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("--") or line.startswith("#"):
            continue
        lines.append(line)
    if not multi_expr:
        lines = " ".join(lines)

    exprs = []
    tables = []
    if default_tables:
        if isinstance(default_tables, str):
            default_tables = [x.strip() for x in default_tables.split(",") if x.strip()]
        tables += default_tables
    for line in lines:
        exprs.append(line)
        tables += TABLE_NAME_RE.findall(line)

    tables = list(sorted(set(tables)))
    for i in range(1, len(tables)):
        if " USING(" not in tables[i]:
            tables[i] = f"{tables[i]} USING(date, instrument)"

    qualify = ""
    if expr_drop_na:
        qualify = "QUALIFY\n    COLUMNS(*) IS NOT NULL"

    return EXPR_SQL_TEMPLATE.format(expr=",\n    ".join(exprs), tables="\n    JOIN ".join(tables), qualify=qualify)


def _process_inputs(sql, *inputs):
    create_table_sql = ""
    for i, x in enumerate(inputs):
        if x is None:
            continue
        table = _build_table(x)
        create_table_sql += table["sql"]
        sql = re.sub(rf"\binput_{i+1}\b", table["table_id"], sql)

    return create_table_sql + sql


def run(
    input_1: I.port("数据输入1", specific_type_name="DataSource", optional=True) = None,
    input_2: I.port("数据输入2", specific_type_name="DataSource", optional=True) = None,
    input_3: I.port("数据输入3", specific_type_name="DataSource", optional=True) = None,
    mode: I.choice("输入模式", list(MODES.keys())) = MODE0,
    expr: I.code(
        "表达式特征，通过表达式构建特征，简单易用",
        default=DEFAULT_EXPR,
        auto_complete_type="sql",
    ) = None,
    expr_tables: I.str("表达式-默认数据表，对于没有给出表名的字段，默认来自这些表，只填写需要的表，可以提高性能，多个表名用英文逗号分隔") = "cn_stock_factors",
    expr_drop_na: I.bool("表达式-移除空值，去掉包含空值的行，用于表达式模式的参数") = True,
    sql: I.code(
        "SQL特征，通过SQL来构建特征，更加灵活，功能最全面",
        default=DEFAULT_SQL,
        auto_complete_type="sql",
    ) = None,
    extract_data: I.bool("抽取数据，是否抽取数据，如果抽取数据，将返回一个BDB DataSource，包含数据DataFrame") = False,
) -> [I.port("输出(SQL文件)", "data")]:
    """输入特征（因子）数据"""
    import dai

    mode = MODES[mode]
    if mode == "sql":
        logger.info("sql mode")
    else:
        logger.info("expr mode")
        sql = _build_sql_from_expr(expr, expr_tables, expr_drop_na=expr_drop_na, multi_expr=True)

    sql = _process_inputs(sql, input_1, input_2, input_3)

    if extract_data:
        logger.info("extract data ..")
        df = dai.query(sql).df()
        logger.info(f"extracted {df.shape}.")
        data_ds = dai.DataSource.write_bdb(df)
    else:
        data_ds = dai.DataSource.write_json({"sql": sql})

    return I.Outputs(data=data_ds)


def post_run(outputs):
    """后置运行函数"""
    return outputs
