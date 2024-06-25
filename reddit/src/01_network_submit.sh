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

DATA_folder="$HOME/socio/reddit/Data/select_subreddits_1/data/${reddit}/"
RES_folder="$HOME/socio/2024_Sunbelt/data/reddit/${reddit}/"
mkdir -p ${RES_folder}

submissions="${DATA_folder}/${reddit}_submissions"
comments="${DATA_folder}/${reddit}_comments"

#reduce dataset
split_date='2014-01-01'

#create network
python network_properties/network_mrjob.py -r local  ${RES_folder}/${reddit}_submissions ${RES_folder}/${reddit}_comments > ${RES_folder}/${reddit}_network

#calculate network properties

prop="${RES_folder}/${reddit}_properties"
stat_file="${RES_folder}/${reddit}_statistics"
network="${RES_folder}/${reddit}_network"

python network_properties/network_properties_mrjob.py -r local --info_file ${stat_file}  ${network}  > ${prop}
python network_properties/convert_file_to_csv.py ${prop} 30 "${prop}.csv"

#calculate reputation

#input_rep="${RES_folder}/${reddit}_users_ts"
#stat_file="${RES_folder}/${reddit}_statistics"
#reputation_inline="${RES_folder}/${reddit}_dr_inline"
#reputation_mean="${RES_folder}/${reddit}_dr_mean"

# third calculate dynamical reputation
#python dynamical_reputation_v2/dynamical_reputation.py -r local --info_file ${stat_file}  "${input_rep}"  > "${reputation_inline}"

#calculate mean dynamical reputation and remove large dr_inline file
#python dynamical_reputation_v2/mean_dynamical_reputation.py -r local   "${reputation_inline}" > "${reputation_mean}" # --local-tmp-dir $HOME/tmp --num-cores 40
#python dynamical_reputation_v2/convert_dr_to_csv.py  "${reputation_mean}"  "${reputation_mean}.csv"


