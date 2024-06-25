import json
import sys
import pandas as pd
import numpy as np
import datetime

def read_file(input_file):

    data = {"Firstdate":[],
            "Lastdate":[],
            "Nusers":[],
            "Mean_dr":[],
            "Sum_dr":[]
              }

    with open(input_file, "r") as fp:
        for line in fp:
            time, res = line.split("\t")
            t0, t1 = time.strip("[]").split(",")
            N, R, Rtot = res.strip("[]\n").split(",")
            
            t0=t0.strip('\"').strip().strip('\"')
            t1=t1.strip('\"').strip().strip('\"')
            t0d = datetime.datetime.strptime(t0, '%Y-%m-%d').date()
            t1d = datetime.datetime.strptime(t1, '%Y-%m-%d').date()
            data["Firstdate"].append(t0d)
            data["Lastdate"].append(t1d)
            data["Nusers"].append(N)
            data["Mean_dr"].append(R)
            data["Sum_dr"].append(Rtot)
       
    data_csv = pd.DataFrame.from_dict(data)
    data_csv = data_csv.sort_values(by="Firstdate", ignore_index=True)
    #data_csv["Date"] = (pd.to_datetime(data_csv["Firstdate"]) + pd.to_timedelta(data_csv['Day'],'d')).dt.date

    return data_csv

input_file = sys.argv[1]
out_file = sys.argv[2]

data = read_file(input_file)
data.to_csv(out_file, index=None)

               
