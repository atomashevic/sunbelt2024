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

echo ${reddit}

RES_folder="$HOME/socio/2024_Sunbelt/data/reddit/${reddit}/"
mkdir -p ${RES_folder}

#calculate reputation
input_rep="${RES_folder}/${reddit}_users_ts"
stat_file="${RES_folder}/${reddit}_statistics"
reputation_inline="${RES_folder}/${reddit}_dr_inline"
reputation_mean="${RES_folder}/${reddit}_dr_mean"

# third calculate dynamical reputation
python dynamical_reputation_v2/dynamical_reputation.py -r local --info_file ${stat_file}  "${input_rep}"  > "${reputation_inline}"

#calculate mean dynamical reputation and remove large dr_inline file
python dynamical_reputation_v2/mean_dynamical_reputation.py -r local   "${reputation_inline}" > "${reputation_mean}" # --local-tmp-dir $HOME/tmp --num-cores 40
python dynamical_reputation_v2/convert_dr_to_csv.py  "${reputation_mean}"  "${reputation_mean}.csv"


