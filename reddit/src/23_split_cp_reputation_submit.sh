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

echo ${reputation_inline}
file=$(basename "$reputation_inline")

reddit="$(cut -d'_' -f1 <<<$file)"
pid="$(cut -d'_' -f4 <<<$file)"


folder="$HOME/socio/2024_Sunbelt/data/reddit/$reddit/${reddit}_core_nodes_reputation_/"
mkdir -p $folder
core_nodes_reputation="$folder/${reddit}_core_nodes_reputation_${pid}"
echo $core_nodes_reputation

RES_folder="$HOME/socio/2024_Sunbelt/data/reddit/${reddit}/"
core_nodes_file="${RES_folder}/${reddit}_core_nodes.json"
stat_file="${RES_folder}/${reddit}_statistics"


python dynamical_reputation_v2/split_cp_reputation.py -r inline --info_file ${stat_file}  --core_nodes_file ${core_nodes_file} ${reputation_inline}  > ${core_nodes_reputation}


