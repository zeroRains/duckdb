import pyarrow as pa
import joblib
import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from xgboost.sklearn import XGBClassifier


class MyProcess:
    def __init__(self):
        # load model part
        scale = 10
        name = "uc08"
        root_model_path = f"/root/workspace/duckdb/examples/embedded-c++/imbridge_test/data/tpcxai_datasets/sf{scale}"
        model_file_name = f"{root_model_path}/model/{name}/{name}.python.model"
        self.model = joblib.load(model_file_name)
        label_range = [3, 4, 5, 6, 7, 8, 9, 12, 15, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36,
                       37, 38, 39, 40, 41, 42, 43, 44, 999]
        self.sorted_labels = sorted(label_range, key=str)

    def process(self, table):
        def decode_label(label):
            return self.sorted_labels[label]

       
        data = table.to_pandas()
        sparse_data = csr_matrix(data)
        predictions = self.model.predict(sparse_data)
        dec_fun = np.vectorize(decode_label)

        res =  dec_fun(predictions)
        df = pd.DataFrame(res)
        return pa.Table.from_pandas(df)
