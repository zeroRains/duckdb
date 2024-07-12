import duckdb.functional
import pytest
import duckdb

import numpy as np
from duckdb.typing import BIGINT, DOUBLE, FLOAT, VARCHAR, BOOLEAN
import pandas as pd


def udf(a, b):
    return a

def main_test():
    print(duckdb.functional.PREDICTION)
    print(duckdb.functional.COMMON)
    
    con = duckdb.connect(":memory:")
    con.create_function("udf", udf,
                [DOUBLE, DOUBLE], DOUBLE, type="arrow", kind=duckdb.functional.PREDICTION)
    
    con.sql("create table t1(a double, b double);")
    con.sql("insert into t1 values (2.0, 3.0), (4.0, 7.0);")

    con.table("t1").show()

    res = con.sql('''
    explain SELECT udf(a,b), a, b FROM t1;
    ''').fetchall()

    for elem in res[0]:
         print(elem)

    res = con.sql('''
    SELECT udf(a,b), a, b FROM t1;
    ''').show()

    res = con.sql('''
    explain SELECT a, b FROM t1 where udf(a,b) > 2.2 and cos(a) > 0.5;
    ''').fetchall()

    for elem in res[0]:
         print(elem)

    res = con.sql('''
    SELECT a, b FROM t1 where udf(a,b) > 2.2 and cos(a) > 0.5;
    ''').show()


class TestPredictionUDF(object):
    def test_create_prediction_udf(self):
        main_test()


if __name__ == "__main__":
    main_test()