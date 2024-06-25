#!/bin/bash

while read line
do
   reddit=${line} 
   qsub -v reddit=${reddit}  01_data_submit.sh

done < Subreddits4



