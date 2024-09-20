import joblib
import pyarrow as pa
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler


class MyProcess:
    def __init__(self):
        # load model part
        scale = 10
        name = "uc01"
        root_model_path = f"/root/workspace/duckdb/examples/embedded-c++/imbridge_test/data/tpcxai_datasets/sf{scale}"
        model_file_name = f"{root_model_path}/model/{name}/{name}.python.model"
        self.model = joblib.load(model_file_name)

    def process(self, table):
        # print(table.num_rows)
        data = table.to_pandas()
        res = self.model.predict(data)
        df = pd.DataFrame(res)
        return pa.Table.from_pandas(df)
