#!/usr/bin/env bash

if [ "$1" == "-s" ];then
    pgrep ucx_perftest|xargs kill
    exit 0
fi

echo ucx_path=${ucx_path:=/usr}
echo ucx_tls=${ucx_tls:=dc_x,sm}
echo ucx_dev=${ucx_dev:=mlx5_bond_0:1}
echo ucx_gid=${ucx_gid:=3}
echo num=${num:=16}
echo type=${type:=ucp_am_bw}
echo size=${size:=$((8<<20))}
echo iter=${iter:=1024}
echo port=${port:=3337}
echo thread=${thread:=1}
echo host=${host:=}
echo log=${log:=./}

# pgrep ucx_perftest|xargs kill

export LD_LIBRARY_PATH=${ucx_path}/lib
export UCX_TLS=$ucx_tls
export UCX_NET_DEVICES=$ucx_dev
export UCX_IB_GID_INDEX=$ucx_gid

base_cmd="\
${ucx_path}/bin/ucx_perftest \
-t $type \
-s $size \
-n $iter \
-T $thread \
-M multi \
-f $host"

if (( num < 1 ));then
    $base_cmd -p $((port+i))
    exit
fi

for i in `seq $num`;do
    if [ $host ];then
        $base_cmd -p $((port+i)) > /dev/null &
    else
        $base_cmd -p $((port+i)) > /dev/null &
    fi
done

# NOTE: num must > 1
if [ "$host" ] && (( num > 1 ));then
    sleep 1
    thresh=num>>2
    pcount=`pgrep ucx_perftest|wc -l`
    while ((pcount > thresh));do
        sleep 1
        echo wait.. $pcount
        pcount=`pgrep ucx_perftest|wc -l`
    done
    sleep 1
    pgrep ucx_perftest|xargs kill
fi