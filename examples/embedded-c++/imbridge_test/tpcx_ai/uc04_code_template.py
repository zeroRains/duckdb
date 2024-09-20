import joblib
import pyarrow as pa
import numpy as np
import pandas as pd
from sklearn import feature_extraction, naive_bayes
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfTransformer

class MyProcess:
    def __init__(self):
        # load model part
        scale = 10
        name = "uc04"
        root_model_path = f"/root/workspace/duckdb/examples/embedded-c++/imbridge_test/data/tpcxai_datasets/sf{scale}"
        model_file_name = f"{root_model_path}/model/{name}/{name}.python.model"
        self.model = joblib.load(model_file_name)

    def process(self, table):
        data = table.to_pandas()
        # print(data.shape)
        res  = self.model.predict(data["text"])
        df = pd.DataFrame(res)
        return pa.Table.from_pandas(df)
