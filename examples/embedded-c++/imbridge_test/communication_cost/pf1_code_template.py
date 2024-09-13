import pandas as pd
import pyarrow as pa
import numpy as np

def process_table(table):
    data = table.to_pandas().values[:, -3]
    return pa.Table.from_pandas(data)
    

class MyProcess:
    def __init__(self):
        # load model part
        pass


    def process(self, table):
        # print(table.num_rows)
        return process_table(table)