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

stat_file="${RES_folder}/${reddit}_statistics"
network_file="${RES_folder}/${reddit}_network"
results="${RES_folder}/subnetworks/${reddit}_sn"
mkdir -p ${results}

python network_properties/subnetworks.py  "${stat_file}"  "${network_file}"  "${results}"





