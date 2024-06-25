from mrjob.job import MRJob
from mrjob.step import MRStep
import pandas as pd
import json
import datetime
from datetime import timedelta
import numpy as np

class CalculateReputation(MRJob):

    def mapper(self, _, line):
        data = json.loads(line)

        yield(None, data["created_utc"])

    def reducer(self, _, values):
        ls = list(values)
        
        Times = [int(i) for i in ls]
        
        Tmin = min(Times)
        Tmax = max(Times)
        Tmin_d = datetime.datetime.fromtimestamp(int(Tmin)).date()
        Tmax_d = datetime.datetime.fromtimestamp(int(Tmax)).date()
        #days = (datetime.datetime.fromtimestamp(int(Tmax)) - datetime.datetime.fromtimestamp(int(Tmin)) ).days
        days = (Tmax_d - Tmin_d).days
        results = {"Tmin": Tmin, "Tmin_d":str(Tmin_d), "Tmax": Tmax, "Tmax_d":str(Tmax_d), "days":days}
         
        
        yield results, 1
        
    def steps(self):
        return [MRStep(mapper=self.mapper,
                       reducer=self.reducer
                      )
                ]
        
        
if __name__ == '__main__':
    
    CalculateReputation.run()
