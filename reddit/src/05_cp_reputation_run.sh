#!/bin/bash

#for reddit in  technology  #MusicVideos gaming gifs pics technology videos video videogames
while read line
do
reddit=$line
        qsub -v reddit=${reddit} 05_cp_reputation_submit.sh

done < Subreddits2 



