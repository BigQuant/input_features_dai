"""input_features_dai 模块输入 SQL, 可用于因子和特征抽取、数据标注等"""

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
category = "数据"
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

DEFAULT_SQL = """-- 使用DAI SQL获取数据, 构建因子等, 如下是一个例子作为参考
-- DAI SQL 语法: https://bigquant.com/wiki/doc/dai-PLSbc1SbZX#h-sql%E5%85%A5%E9%97%A8%E6%95%99%E7%A8%8B
-- 使用数据输入1/2/3里的字段: e.g. input_1.close, input_1.* EXCLUDE(date, instrument)

SELECT
    -- 在这里输入因子表达式
    -- DAI SQL 算子/函数: https://bigquant.com/wiki/doc/dai-PLSbc1SbZX#h-%E5%87%BD%E6%95%B0
    -- 数据&字段: 数据文档 https://bigquant.com/data/home

    m_lag(close, 90) / close AS return_90,
    m_lag(close, 30) / close AS return_30,
    -- 下划线开始命名的列是中间变量, 不会在最终结果输出 (e.g. _rank_return_90)
    c_pct_rank(-return_90) AS _rank_return_90,
    c_pct_rank(return_30) AS _rank_return_30,

    c_rank(volume) AS rank_volume,
    close / m_lag(close, 1) as return_0,

    -- 日期和股票代码
    date, instrument
FROM
    -- 预计算因子 cn_stock_bar1d https://bigquant.com/data/datasources/cn_stock_bar1d
    cn_stock_prefactors
    -- SQL 模式不会自动join输入数据源, 可以根据需要自由灵活的使用
    -- JOIN input_1 USING(date, instrument)
WHERE
    -- WHERE 过滤, 在窗口等计算算子之前执行
    -- 剔除ST股票
    st_status = 0
QUALIFY
    -- QUALIFY 过滤, 在窗口等计算算子之后执行, 比如 m_lag(close, 3) AS close_3, 对于 close_3 的过滤需要放到这里
    -- 去掉有空值的行
    COLUMNS(*) IS NOT NULL
    -- _rank_return_90 是窗口函数结果，需要放在 QUALIFY 里
    AND _rank_return_90 > 0.1
    AND _rank_return_30 < 0.1
-- 按日期和股票代码排序, 从小到大
ORDER BY date, instrument
"""

DEFAULT_EXPR = """-- DAI SQL 算子/函数: https://bigquant.com/wiki/doc/dai-PLSbc1SbZX#h-%E5%87%BD%E6%95%B0
-- 数据&字段: 数据文档 https://bigquant.com/data/home
-- 数据使用: 表名.字段名, 对于没有指定表名的列, 会从 expr_tables 推断, 如果同名字段在多个表中出现, 需要显式的给出表名

m_lag(close, 90) / close AS return_90
m_lag(close, 30) / close AS return_30
-- cn_stock_bar1d.close / cn_stock_bar1d.open
-- cn_stock_prefactors https://bigquant.com/data/datasources/cn_stock_prefactors 是常用因子表(VIEW), JOIN了很多数据表, 性能会比直接用相关表慢一点, 但使用简单
-- cn_stock_prefactors.pe_ttm

-- 表达式模式下, 会自动join输入数据1/2/3, 可以在表达式里直接使用其字段。包括 input_1 的所有列但去掉 date, instrument。注意字段不能有重复的, 否则会报错
-- input_1.* EXCLUDE(date, instrument)
-- input_1.close
-- input_2.close / input_1.close
"""

DEFAULT_EXPR_FILTERS = """-- DAI SQL 算子/函数: https://bigquant.com/wiki/doc/dai-PLSbc1SbZX#h-%E5%87%BD%E6%95%B0
-- 数据&字段: 数据文档 https://bigquant.com/data/home
-- 表达式模式的过滤都是放在 QUALIFY 里, 即数据查询、计算, 最后才到过滤条件

-- c_pct_rank(-return_90) <= 0.3
-- c_pct_rank(return_30) <= 0.3
-- cn_stock_bar1d.turn > 0.02
"""

# 去除单引号内的内容字符串: instrument in ('jm2201.DCE') 避免抽取出 jm2201
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

    # 构建过滤添加，放到 QUALIFY 里
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
    input_1: I.port("数据输入1, 如果有metadata extra, 会传递给输出 data", specific_type_name="DataSource", optional=True) = None,
    input_2: I.port("数据输入2", specific_type_name="DataSource", optional=True) = None,
    input_3: I.port("数据输入3", specific_type_name="DataSource", optional=True) = None,
    mode: I.choice("输入模式", list(MODES.keys())) = MODE0,
    expr: I.code(
        "表达式特征, 通过表达式构建特征, 简单易用",
        default=DEFAULT_EXPR,
        auto_complete_type="sql",
    ) = None,
    expr_filters: I.code(
        "表达式过滤条件, 每行一个条件, 多个条件之间是 AND 关系, 条件内可以使用 OR 组合",
        default=DEFAULT_EXPR_FILTERS,
        auto_complete_type="sql",
    ) = None,
    expr_tables: I.str(
        "表达式-默认数据表, 对于没有给出表名的字段, 默认来自这些表, 只填写需要的表, 可以提高性能, 多个表名用英文分号(;)分隔"
    ) = "cn_stock_prefactors",
    extra_fields: I.str("表达式-其他字段, 其他需要包含的字段, 会与expr合并起来, 非特征字段一般放在这里, 多个字段用英文逗号分隔") = "date, instrument",
    order_by: I.str("表达式-排序字段, 排序字段 e.g. date ASC, instrument DESC") = "date, instrument",
    expr_drop_na: I.bool("表达式-移除空值, 去掉包含空值的行, 用于表达式模式的参数") = True,
    expr_add_sql: I.bool("表达式-添加SQL特征语句, 在表达式模式下，把 SQL特征 输入的SQL语句加入到表达式模式构建的SQL前") = False,
    sql: I.code(
        "SQL特征, 通过SQL来构建特征, 更加灵活, 功能最全面",
        default=DEFAULT_SQL,
        auto_complete_type="sql",
    ) = None,
    extract_data: I.bool("抽取数据, 是否抽取数据, 如果抽取数据, 将返回一个BDB DataSource, 包含数据DataFrame") = False,
) -> [I.port("输出(SQL文件)", "data")]:
    """输入特征（因子）数据"""

    input_tables = _ds_to_tables([input_1, input_2, input_3])

    if "；" in expr_tables:
        raise Exception("检测到中文分号在 表达式-默认数据表 参数中，请使用英文分号")

    mode = MODES[mode]
    if mode == "expr":
        logger.info("expr mode")
        # if "date" not in expr or "instrument" not in expr:
        #     logger.warning("not found date/instrument in expr, the new version will not add date, instrument by default")
        final_sql = _build_sql_from_expr(
            expr + "\n" + extra_fields.replace(",", "\n"), expr_filters, expr_tables, order_by=order_by, expr_drop_na=expr_drop_na, input_tables=input_tables
        )
        if expr_add_sql:
            final_sql = sql.strip() + "\n" + final_sql
    else:
        logger.info("sql mode")
        final_sql = sql

    # 替换 input_*
    for x in input_tables["items"]:
        final_sql = re.sub(rf'\b{x["name"]}\b', x["table_id"], final_sql)

    final_sql = input_tables["sql"] + final_sql

    # 使用第一个input ds的 extra
    return I.Outputs(data=_create_ds_from_sql(final_sql, extract_data, input_1))


def post_run(outputs):
    """后置运行函数"""
    return outputs
