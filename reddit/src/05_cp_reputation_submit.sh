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

echo ${reddit} ${netid}

RES_folder="$HOME/socio/2024_Sunbelt/data/reddit/${reddit}/"
mkdir -p ${RES_folder}

inline_rep="${RES_folder}/${reddit}_dr_inline"
stat_file="${RES_folder}/${reddit}_statistics"
core_nodes_file="${RES_folder}/${reddit}_core_nodes.json"
core_rep="${RES_folder}/${reddit}_core_nodes_reputation"


python dynamical_reputation_v2/cp_reputation.py -r inline --info_file ${stat_file}  --core_nodes_file ${core_nodes_file} ${inline_rep}  > ${core_rep}

python dynamical_reputation_v2/convert_core_dr_to_csv.py  ${core_rep} ${core_rep}.csv
