import duckdb.functional
import pytest
import duckdb

import numpy as np
from duckdb.typing import BIGINT, DOUBLE, FLOAT, VARCHAR, BOOLEAN
import pandas as pd


def udf(a, b):
    print(len(a))
    return a

con = duckdb.connect(":memory:")
con.sql("SET threads TO 1;")
import time
time.sleep(3)
con.create_function("udf", udf,
            [DOUBLE, DOUBLE], DOUBLE, type="arrow", kind=duckdb.functional.PREDICTION, batch_size=4444)

print(duckdb.functional.PREDICTION)
print(duckdb.functional.COMMON)


con.sql("create table t1(a double, b double);")
for i in range(4050):
    con.sql("insert into t1 values (2.0, 3.0), (4.0, 7.0);")

con.sql("create table t2(a double, b double);")
con.sql("insert into t2 values (2.0, 3.0), (4.0, 7.0);")
con.table("t1").show()

def project_test():
    # res = con.sql('''
    # explain SELECT udf(a,b), a, b FROM t1;
    # ''').fetchall()

    # for elem in res[0]:
    #      print(elem)
    print("______________________________________________")
    res = con.sql('''
    SELECT udf(a,b), a, b FROM t1;
    ''').show()

def filter_test():
    res = con.sql('''
    explain SELECT a, b FROM t1 where udf(a,b) > 2.2 and cos(a) > -1;
    ''').fetchall()

    for elem in res[0]:
         print(elem)

    res = con.sql('''
    SELECT a, b FROM t1 where udf(a,b) > 2.2 and cos(a) > -1;
    ''').show()

    res = con.sql('''
    explain SELECT t1.a, t2.b FROM t1,t2 where udf(t1.a, t2.b) > 2.2;
    ''').fetchall()

    for elem in res[0]:
         print(elem)
    
    res = con.sql('''
    SELECT t1.a, t2.b FROM t1,t2 where udf(t1.a, t2.b) > 2.2;
    ''').show()

    res = con.sql('''
    explain SELECT t1.a, t2.b FROM t1,t2 where udf(t1.a, t2.b) > 2.2 and t1.a+t2.b > 5.2;
    ''').fetchall()

    for elem in res[0]:
         print(elem)

    res = con.sql('''
    SELECT t1.a, t2.b FROM t1,t2 where udf(t1.a, t2.b) > 2.2 and t1.a+t2.b > 5.2;
    ''').show()


if __name__ == "__main__":
    project_test()
    # filter_test()