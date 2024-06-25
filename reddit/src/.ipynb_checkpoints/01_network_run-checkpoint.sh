#!/bin/bash

while read line
do
   reddit=${line} 
   qsub -v reddit=${reddit}  01_network_submit.sh

done < Subreddits3



