from mrjob.job import MRJob
from mrjob.step import MRStep
import pandas as pd
import json
import datetime
from datetime import timedelta
import numpy as np


def calculate_user_reputation(dates, d_min, day_max, beta=0.999, Ib=1, alpha=2, decay_per_day = "True" ): 
    """Returns a dictionary of user's reputational values for each day
    Args:
        dates (DataFrame): Pandas DataFrame with timestamp and day columns.
        Each row is a single interaction by the same user.
        d_min (datetime): Date-time stamp of the first interaction: 'YYYY-MM-DDTHH:MM:SS.SSS'
        day_max (int): Upper limit for days after first interaction which are counted.
        beta (float, optional): Reputation decay parameter. Defaults to 0.999.
        Ib (float, optional): Basic reputational value of a single interaction. Defaults to 1.
        alpha (float, optional): Cumulation parameter. Defaults to 2.
        decay_per_day (str, optional): Perform decay in each day of inactivity? Defaults to "True".
    Returns:
        (dict): A dictionary of user's reputational values for each day
    """
    dates = dates.sort_values(by='Time')
    Ru = {}
    first_day = dates.iloc[0].days
    first_date = dates.iloc[0].Time
    if first_day > 0:
        for day in range(first_day):
            Ru[day] = 0.
    A = 1
    Ru[first_day] = Ib + Ib*alpha*(1.-1./(A+1))
    last_day = first_day
    last_activity_date = first_date
    last_activity_day = first_day
    for i in range(1,len(dates)):
        curr_day = dates.iloc[i].days
        curr_activity_date = dates.iloc[i].Time
        if curr_day > (last_day+1):
            for i in range(curr_day-last_day - 1):
                inactive_date = pd.to_datetime(d_min) + timedelta(days=int(last_day)+2)
                update_reputation_inactive(Ru, inactive_date, last_activity_date, last_activity_day,  last_day, beta=beta, decay_per_day = decay_per_day)
                last_day = last_day + 1
                A = 0
        A+=1
        update_reputation(Ru, A, curr_activity_date, last_activity_date, last_day, last_activity_day, curr_day, beta=beta,  Ib=Ib, alpha=alpha, decay_per_day=decay_per_day)
        last_activity_date = curr_activity_date
        last_activity_day = curr_day
        last_day = curr_day
    rest_days = day_max - last_day
    for i in range(rest_days):
        inactive_date = pd.to_datetime(d_min) + timedelta(days=int(last_day)+2)
        update_reputation_inactive(Ru, inactive_date, last_activity_date, last_activity_day,  last_day, beta=beta, decay_per_day = decay_per_day)
        last_day = last_day + 1
    return Ru
    
def update_reputation_inactive(R, inactive_date, last_activity_date, last_activity_day, last_day, beta=0.999, decay_per_day = 'True'):
    """Performs the decay of user's reputation during a period of inactivity.
    Args:
        R (dict): Dictionary of user's reputation which will be updated.
        inactive_date (datetime): Date-time stamp designating when the period of inactivity ends
        last_activity_date (datetime): Date-time stamp designating when the last recorded activity of user
        last_activity_day (int): Day in which last activity was recorded
        last_day (int): Last day for which reputation was previously updated
        beta (float, optional): Reputation decay parameter. Defaults to 0.999.
        decay_per_day (str, optional): Perform decay in each day of inactivity? Defaults to "True".
    """
    dt = ( pd.to_datetime(inactive_date) - pd.to_datetime(last_activity_date))/np.timedelta64(1,'D')
    if decay_per_day=='True':
        D = R[last_day]*np.power(beta, dt)
    else:
        D = R[last_activity_day]*np.power(beta, dt)  
    R[last_day+1] = D

def update_reputation(R, A, curr_date, last_activity_date, last_day, last_activity_day, curr_day, beta=0.999,  Ib=1, alpha=2, decay_per_day='True'):
    """Performs the decay of user's reputation during a period of inactivity.
    Args:
        R (dict): Dictionary of user's reputation which will be updated.
        A (int): Count of consecutive interactions within time-window frame
        curr_date (datetime): Date-time stamp of current activity when update function was called
        last_activity_date (datetime): Date-time stamp designating previous activity of the user
        last_activity_day (int): Day in which last activity was recorded
        last_day (int): Last day for which reputation was previously updated
        beta (float, optional): Reputation decay parameter. Defaults to 0.999.
        decay_per_day (str, optional): Perform decay in each day of inactivity? Defaults to "True".
    """
    dt = ( pd.to_datetime(curr_date) - pd.to_datetime(last_activity_date))/np.timedelta64(1,'D')
    In = Ib + Ib*alpha*(1.-1./(A+1))
    if (decay_per_day=='False') and (A==1):
        D = R[last_activity_day]*np.power(beta, dt)
    else:
        D = R[last_day]*np.power(beta, dt)
    y = R.get(curr_day, 0.)
    y = In+D
    R[curr_day] = y

def dynamical_reputation(ls):
    """
    input> ts_list = list of timestamps
    """
    
    data = {"Time":[], 
            "days":[]}
    Tmin, Tmax, ts = ls
    for t in ts:
           days = (datetime.datetime.fromtimestamp(int(t)) - datetime.datetime.fromtimestamp(int(Tmin)) ).days
           data["days"].append(days)
           data["Time"].append(datetime.datetime.fromtimestamp(int(t)))
           
    d_min = datetime.datetime.fromtimestamp(int(Tmin))
    day_max = (datetime.datetime.fromtimestamp(int(Tmax)) - datetime.datetime.fromtimestamp(int(Tmin)) ).days
    df = pd.DataFrame.from_dict(data)
    Ru = calculate_user_reputation(df, d_min, day_max, beta=0.96, Ib=1, alpha=2, decay_per_day="False")
    return Ru, Tmin, Tmax

def read_statistics_file(filename):
    
    with open(filename, "r") as F:
        file = json.load(F)
        Tmin = file["Tmin"]
        Tmax = file["Tmax"]
                
    return Tmin, Tmax
    

class CalculateReputation(MRJob):

    def configure_args(self):
        super(CalculateReputation, self).configure_args()
        #self.add_passthru_arg('--split_date', help="Split date")
        self.add_passthru_arg('--info_file', help="Info file")
    
    def mapper(self, _, line):
        
        user, posts = line.split("\t")
        posts = json.loads(posts)
        ts = []
        for p, tsp in posts.items():
            ts.extend(tsp)
            
        #load timestamps
        ts = [int(x) for x in ts]
       
        #read statistics from file
        filename=self.options.info_file
        Tmin, Tmax = read_statistics_file(filename)

        #ls = [(Tmin, Tmax, int(x)) for x in ts]
        
        Ru, Tmin, Tmax = dynamical_reputation((Tmin, Tmax, ts))
        data = {int(key): float(val) for key, val in Ru.items()}

        yield  (user, Tmin, Tmax), data
            
    #def reducer(self, key, values):
    #    ls = list(values)
    #    Ru, Tmin, Tmax = dynamical_reputation(ls)
    #    data = {int(key): float(val) for key, val in Ru.items()}
    #    yield (key, Tmin,Tmax), data
        
    #def mapper_2(self, key, reputations):
    #    
    #    user, Tmin, Tmax = key
    #    
    #    for k, v in reputations.items():
    #        yield (int(k), Tmin, Tmax), float(v)
            
    #def reducer_2(self, key, values):
    #    ls = list(values)
    #    N = len([i for i in ls if i>=1])
    #    if N==0:
    #        Rmean = 0
    #        Rtot = 0
    #    else:
    #        Rmean = np.mean([i for i in ls if i>=1])
    #        Rtot = np.sum([i for i in ls if i>=1])
    #     
    #    yield (key, [N, Rmean, Rtot])
        
    def steps(self):
        return [MRStep(mapper=self.mapper)
               ]

if __name__ == '__main__':
    CalculateReputation.run()

        
