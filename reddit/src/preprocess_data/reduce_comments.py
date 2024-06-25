from mrjob.job import MRJob
from mrjob.step import MRStep
import pandas as pd
import json
import datetime
from datetime import timedelta
import numpy as np

#keep only comments of depth 1 comment-submission, if parent id is submission we do not have t1

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
           pid=data["parent_id"]
           #if parent id starts with t1 it is comments, otherwise it is submission
           # first case is to keep only submissions-direct comments 
           # 2.case is to keep all interactions, comments-comments, or comment-parent submission
        
           if pid.split("_")[0] != 't1':

              yield(data, 0)

    def steps(self):
        return [MRStep(mapper=self.mapper)]
        
        
if __name__ == '__main__':
    
    Preprocess.run()
