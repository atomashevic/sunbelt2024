from mrjob.job import MRJob
from mrjob.step import MRStep
import pandas as pd
import json
import datetime
from datetime import timedelta
import numpy as np

class Preprocess(MRJob):

    def configure_args(self):
        super(Preprocess, self).configure_args()
    
        self.add_passthru_arg('--split_date', help="Split Date")


    def mapper(self, _, line):
        data = json.loads(line)

        t = datetime.datetime.fromtimestamp(int(data["created_utc"])).date()
        thd =  pd.to_datetime(self.options.split_date, format='%Y-%m-%d').date()
        
        data['timestamp']=str(t)
        if t>=thd:

           yield(data, 0)

        
        
    def steps(self):
        return [MRStep(mapper=self.mapper)]
        
        
if __name__ == '__main__':
    
    Preprocess.run()
