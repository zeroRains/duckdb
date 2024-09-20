import joblib
import numpy as np
import pyarrow as pa
import pandas as pd
import joblib
from surprise import SVD
from surprise import Dataset
from surprise.reader import Reader


class MyProcess:
    def __init__(self):
        # load model part
        scale = 10
        name = "uc10"
        root_model_path = f"/root/workspace/duckdb/examples/embedded-c++/imbridge_test/data/tpcxai_datasets/sf{scale}"
        model_file_name = f"{root_model_path}/model/{name}/{name}.python.model"
        self.model = joblib.load(model_file_name)

    def process(self, table):
        data = table.to_pandas()
        # print(data.shape)
        res =  self.model.predict(data)
        df = pd.DataFrame(res)
        return pa.Table.from_pandas(df)
