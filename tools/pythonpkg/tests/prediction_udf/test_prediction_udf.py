import duckdb.functional
import pytest
import duckdb


class TestPredictionUDF(object):
    def test_create_prediction_udf(self):
        print(duckdb.functional.PREDICTION)
        print(duckdb.functional.COMMON)
        
