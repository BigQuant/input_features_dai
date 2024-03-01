"""input_features_dai 模块输入 SQL，可用于因子和特征抽取、数据标注等"""
from bigmodule import I  # noqa: N812

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


DEFAULT_SQL = """-- 使用DAI SQL获取数据，构建因子等，如下是一个例子作为参考
-- DAI SQL 语法: https://bigquant.com/wiki/doc/dai-PLSbc1SbZX#h-sql%E5%85%A5%E9%97%A8%E6%95%99%E7%A8%8B

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


def run(
    sql: I.code("特征(SQL)", default=DEFAULT_SQL, auto_complete_type="sql"),
) -> [I.port("输出(SQL文件)", "data")]:
    """输入特征（因子）数据"""
    import dai
    data = dai.DataSource.write_text(sql)
    return I.Outputs(data=data)


def post_run(outputs):
    """后置运行函数"""
    return outputs


