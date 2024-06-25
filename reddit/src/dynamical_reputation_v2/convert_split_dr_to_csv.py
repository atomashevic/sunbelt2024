import json
import sys
import pandas as pd
import numpy as np
import datetime
import glob


input_folder=sys.argv[1]
out_file=sys.argv[2]

data_dict = {}

files=glob.glob("%s/*"%input_folder)
for input_file in files:
    print(input_file)
    with open(input_file, 'r') as fp:
        for line in fp:
            time, res = line.split("\t")
            day, t0, t1 = time.strip("[]").split(",")
            N, Rtot = res.strip("[]\n").split(",")

            curr = data_dict.get((day, t0, t1), [0, 0])
            curr[0]+=int(N)
            curr[1]+=float(Rtot)
            data_dict[(day, t0, t1)] = curr


data = { "Day":[],
         "Firstdate":[],
         "Nusers":[],
         "Mean_dr":[],
         "Sum_dr":[]
         }

for key, val in data_dict.items():
    day, t0, t1=key
    N, Rtot = val
    R=Rtot/N

    t0d = datetime.datetime.fromtimestamp(int(t0)).date()
    data["Day"].append(int(day))
    data["Firstdate"].append(t0d)
    data["Nusers"].append(N)
    data["Mean_dr"].append(R)
    data["Sum_dr"].append(Rtot)

data_csv = pd.DataFrame.from_dict(data)
data_csv = data_csv.sort_values(by="Day", ignore_index=True)
data_csv["Date"] = (pd.to_datetime(data_csv["Firstdate"]) + pd.to_timedelta(data_csv['Day'],'d')).dt.date

data_csv.to_csv(out_file, index=None)
