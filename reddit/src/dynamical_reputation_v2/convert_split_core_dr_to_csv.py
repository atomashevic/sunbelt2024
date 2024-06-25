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
            t0, t1 = time.strip("[]").split(",")
            N, Rtot = res.strip("[]\n").split(",")
            
            t0=t0.strip('\"').strip().strip('\"')
            t1=t1.strip('\"').strip().strip('\"')
            
            curr = data_dict.get((t0, t1), [0, 0])
            curr[0]+=int(N)
            curr[1]+=float(Rtot)
            data_dict[( t0, t1)] = curr


data = { "Firstdate":[],
         "Lastdate":[],
         "Nusers":[],
         "Mean_dr":[],
         "Sum_dr":[]
         }

for key, val in data_dict.items():
    t0, t1=key
    N, Rtot = val
    if N==0:
       R=0
    else:
       R=Rtot/N

    t0d = datetime.datetime.strptime(t0, '%Y-%m-%d').date()
    t1d = datetime.datetime.strptime(t1, '%Y-%m-%d').date()
    
    data["Firstdate"].append(t0d)
    data["Lastdate"].append(t1d)
    data["Nusers"].append(N)
    data["Mean_dr"].append(R)
    data["Sum_dr"].append(Rtot)

data_csv = pd.DataFrame.from_dict(data)
data_csv = data_csv.sort_values(by="Firstdate", ignore_index=True)
data_csv.to_csv(out_file, index=None)
