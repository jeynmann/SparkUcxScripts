echo '[log][ep num][port num][-d][-i][addr][core][dci]'

# ####################
# available arguments
if [[ -z $log ]];then
  log=$1
fi
if [[ -z $epn ]];then
  epn=$2
fi
if [[ -z $portn ]];then
  portn=$3
fi
if [[ -z $msg ]];then
  msg=$4
fi
if [[ -z $iter ]];then
  iter=$5
fi
if [[ -z $addr ]];then
  addr=$6
fi
if [[ -z $core0 ]];then
  core0=$7
fi
if [[ -z $dci ]];then
  dci=$8
fi

# ####################
# allow -x instead of loop in bash
epn=${epn:=1}
if [[ ${epn%%-x*} ]]; then
  epx=""
else
  epx=$epn
  epn=1
fi

printf "epn=$epn, "
printf "epx=$epx, "
printf "portn=${portn:=1}, "
printf "msg=${msg:=4194304}, "
printf "iter=${iter:=0}, "
printf "addr=${addr:=''}, "
printf "core0=${core0:=$(($RANDOM&15|16))}, "
printf "dci=${dci:=8}, "
printf "wind=${wind:=16}, "
echo ''

# ####################
# generate log name
if [[ $# -lt 1 ]]; then
  logdev=/dev/stdout
  log=''
elif [[ $1 == "1" ]]; then
  logdev=/dev/stdout
  log=''
elif [[ $1 == "0" ]]; then
  logdev=/dev/null
  log=''
elif [[ -z $log ]];then
  logdev=''
  log=''
elif [[ -z ${log%%/dev/*} ]];then
  logdev=$log
  log=''
elif [[ -z ${log%*.log} ]];then
  logdev=''
  while [[ -f $log ]];do
    log_sfx=$((log_sfx+1))
    log=$log_pfx.$log_sfx.log
  done
  echo '' > $log
else
  logdev=''
  log=$1
fi
echo "log     =${log:=$logdev}"

# ####################
# generate ports
for host in ${addr[@]};do
  for port in `seq $portn`;do
    port=$((port+3336))
    addrs="$addrs$host:$port "
  done
done
echo "addrs   =$addrs"

# ####################
# generate env
script_dir=$(dirname $0)
source $script_dir/io_demo_env.sh

# ####################
# generate path
echo "bin_path=${bin_path:="test/apps/iodemo"}"
#bin_path="install/bin"

# ####################
# generate servers
declare -A helper
helper["1"]=$addrs
helper["2"]="${helper['1']} ${helper['1']}"
helper["4"]="${helper['2']} ${helper['2']}"
helper["8"]="${helper['4']} ${helper['4']}"
helper["16"]="${helper['8']} ${helper['8']}"
helper["32"]="${helper['16']} ${helper['16']}"
helper["64"]="${helper['32']} ${helper['32']}"
helper["128"]="${helper['64']} ${helper['64']}"
helper["256"]="${helper['128']} ${helper['128']}"
helper["512"]="${helper['256']} ${helper['256']}"
helper["1024"]="${helper['512']} ${helper['512']}"
helper["2048"]="${helper['1024']} ${helper['1024']}"
helper["4096"]="${helper['2048']} ${helper['2048']}"
helper["8192"]="${helper['4096']} ${helper['4096']}"
helper["16384"]="${helper['8192']} ${helper['8192']}"
helper_k=("16384""8192" "4096" "2048" "1024" "512" "256" "128" "64" "32" "16" "8" "4" "2" "1" )

if [[ -z $servers ]];then
  i=0
  for k in ${helper_k[@]}; do
    j=$((i+k))
    while [[ $j -le $epn ]];do
      servers="$servers ${helper[$k]}"
      i=$((j))
      j=$((j+k))
    done
  done
fi

# echo $servers
# exit 0

# ####################
# generate commands

ctl=""
cmd="$bin_path/io_demo"
arg="$arg $epx -d $msg -i $iter -w $wind -a $wind -ninf -tinf -A $servers"

# ####################
# allow set -e by env
if [[ -z $core0 ]] && [[ $JUCX_SET_CORE ]];then
  core0=$JUCX_SET_CORE
fi

if [[ $JUCX_PROF ]]; then
  ctl="export UCX_PROFILE_MODE=log,accum && export UCX_PROFILE_FILE=ucx.prof && $ctl"
fi

if [[ $JUCX_SET_EVENT ]];then
  arg="-e $arg"
fi

if [[ $JUCX_PROG_COUNT ]]; then
  arg="-L$JUCX_PROG_COUNT $arg"
fi

if [[ $JUCX_ITER_PAUSE ]];then
  arg="-j$JUCX_ITER_PAUSE $arg"
fi

echo "cmd     =taskset -c $core0 $ctl $cmd $arg"
# date +"%x %X"
if [[ $log ]]; then
  if [[ $JUCX_LOG_ACT_ERR ]]; then
    nohup taskset -c $core0 $ctl $cmd $arg | grep -Pi 'act|err|diag|info' &>$log &
  else
    nohup taskset -c $core0 $ctl $cmd $arg &>$log &
  fi
else
  taskset -c $core0 $ctl $cmd $arg
fi
echo ok

# ####################
# ib alias
# sudo ip addr del 194.168.21.14/24 dev bond0 label bond0:1
# sudo ip addr del 194.168.21.13/24 dev bond0 label bond0:1
#
# sudo ip addr add 194.168.21.14/24 dev bond0 label bond0:1
# sudo ip addr add 194.168.21.13/24 dev bond0 label bond0:1
#
# sudo ip addr del 194.168.11.24/24 dev bond0 label bond0:1
# sudo ip addr add 194.168.11.24/24 dev bond0 label bond0:1
