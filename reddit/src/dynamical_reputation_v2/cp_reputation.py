from mrjob.job import MRJob
from mrjob.step import MRStep
import pandas as pd
import json
import datetime
from datetime import timedelta
import numpy as np

def read_statistics_file(filename):
    
    with open(filename, "r") as F:
        file = json.load(F)
        Tmin = file["Tmin"]
        Tmax = file["Tmax"]
                
    return Tmin, Tmax


def read_core_nodes_file(filename):
    
    with open(filename, "r") as F:
        l = F.readline()
        time, users = l.split("\t")
        users = users.strip().split()
                
    return time, users

def read_core_nodes_json(filename):
    
    with open(filename, "r") as F:
        users_netid = json.load(F)
        
    return users_netid


class CorePeripheryRep(MRJob):

    def configure_args(self):
        super(CorePeripheryRep, self).configure_args()
        self.add_passthru_arg('--info_file', help="Info file")
        self.add_passthru_arg('--core_nodes_file', help="Core nodes file")
    
    def mapper(self, _, line):
        
        filename=self.options.core_nodes_file
        user_netid = read_core_nodes_json(filename)
        
        stat, reputation = line.split("\t")
        user = str(stat.strip("[]").split(",")[0]).strip("\"").strip()
        reputation = json.loads(reputation.strip())
        
        if user in user_netid:
            nets = user_netid[user]
            for netid in nets:
                last_d = int(netid)+29
                yield netid, reputation[str(last_d)]        
            
    def reducer(self, key, values):
                                        
        netid=int(key)
        filename=self.options.info_file
        Tmin, Tmax = read_statistics_file(filename)
        tmin_d = datetime.datetime.fromtimestamp(int(float(Tmin))).date()
        last_d = int(netid)+29 #because python counts from 0
        net_date_start = tmin_d + datetime.timedelta(days=netid) #this is date for first net date
        net_date_end = tmin_d + datetime.timedelta(days=last_d) #this is date for first net date
                                        
        ls=list(values)
        N = len([i for i in ls if i>=1])
        if N==0:
            Rmean = 0
            Rtot = 0
        else:
            Rmean = np.mean([i for i in ls if i>=1])
            Rtot = np.sum([i for i in ls if i>=1])
        yield ([str(net_date_start), str(net_date_end)], [N, Rmean, Rtot])
            
        
    def steps(self):
        return [MRStep(mapper=self.mapper, 
                      reducer=self.reducer),
               ]

if __name__ == '__main__':
    CorePeripheryRep.run()

