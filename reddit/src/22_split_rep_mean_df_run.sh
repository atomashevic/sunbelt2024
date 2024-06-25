#!/bin/bash

for reddit in  pics funny
do
   # reddit=${line} 
   
    qsub -v reddit=${reddit} 22_split_rep_mean_df_submit.sh
   

done
