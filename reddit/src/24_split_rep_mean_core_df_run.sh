#!/bin/bash

for reddit in  videos pics funny gaming 
do
   # reddit=${line} 
   
    qsub -v reddit=${reddit} 24_split_rep_mean_core_df_submit.sh
   

done
