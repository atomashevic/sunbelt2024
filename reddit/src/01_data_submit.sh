#!/bin/bash
#PBS -q standard
#PBS -l nodes=1:ppn=16
#PBS -e ${PBS_JOBID}.err
#PBS -o ${PBS_JOBID}.out
#PBS -l walltime=6:6:00:00

cd ${PBS_O_WORKDIR}

module load python/3.6.5
source $HOME/.venv/bin/activate
export PATH=$HOME/zstd/zstd-dev/programs:$PATH

#reddit="celebrities"
#split_date="2014-09-05"

echo ${reddit}

DATA_folder="$HOME/socio/reddit/Data/select_subreddits_1/data/${reddit}/"
RES_folder="$HOME/socio/2024_Sunbelt/data/reddit/${reddit}/"
mkdir -p ${RES_folder}

submissions="${DATA_folder}/${reddit}_submissions"
comments="${DATA_folder}/${reddit}_comments"

#reduce dataset
split_date='2014-01-01'

python preprocess_data/reduce_submissions.py --split_date ${split_date} ${submissions} > ${RES_folder}/${reddit}_submissions_1
echo "SUBMISSIONS LOADED"
echo "---------------------------------"
echo
python preprocess_data/reduce_comments.py --split_date ${split_date} ${comments} > ${RES_folder}/${reddit}_comments_1

echo "COMMENTS LOADED"
echo "---------------------------------"
echo

cut -f 1 ${RES_folder}/${reddit}_submissions_1 > ${RES_folder}/${reddit}_submissions
cut -f 1 ${RES_folder}/${reddit}_comments_1 > ${RES_folder}/${reddit}_comments

#rm ${RES_folder}/${reddit}_submissions_1
#rm ${RES_folder}/${reddit}_comments_1

#get users ts
python preprocess_data/get_time_series.py ${RES_folder}/${reddit}_submissions ${RES_folder}/${reddit}_comments ${RES_folder}/${reddit}_users_ts

echo "USERS TS"
echo "---------------------------------"
echo

#get statistics
python preprocess_data/reddit_stats.py -r local ${RES_folder}/${reddit}_submissions ${RES_folder}/${reddit}_comments >  ${RES_folder}/${reddit}_statistics_1

cut -f 1 ${RES_folder}/${reddit}_statistics_1 > ${RES_folder}/${reddit}_statistics
#rm ${RES_folder}/${reddit}_statistics_1

echo "STATISTICS"
echo "---------------------------------"
echo