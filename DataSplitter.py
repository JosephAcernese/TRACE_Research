import pandas as pd
import numpy as np



cur_month = pd.read_csv("./TRACEData/200207.csv", low_memory=False)

one_day = cur_month[cur_month["trd_rpt_dt"] == "2002-07-05"]

one_day.to_csv("./Testfile/TestData3.csv")



cur_month = pd.read_csv("./TRACEData/201507.csv", low_memory=False)

one_day = cur_month[cur_month["trd_rpt_dt"] == "2015-07-08"]

one_day.to_csv("./Testfile/TestData4.csv")