#!/bin/bash

while read line
do
   reddit=${line} 
   qsub -v reddit=${reddit}  02_subnetworks_submit.sh

done < Subreddits4



