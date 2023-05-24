
# env


if [[ -z $bin_path ]];then
    export bin_path=${bin_path:=/usr/bin}
fi
export work_path=`readlink -f $(dirname $bin_path)`
export LD_LIBRARY_PATH=`readlink -f $work_path/lib`
printf "LD=$LD_LIBRARY_PATH, "

export ucx_tls=${ucx_tls:=dc_x,tcp}
if [[ -z $UCX_TLS ]];then
  if [[ "$ucx_tls" == "tcp" ]];then
      export UCX_SOCKADDR_TLS_PRIORITY=tcp
      export UCX_TLS=tcp
      export UCX_NET_DEVICES=bond0
  elif [[ $ucx_tls$ucx_tls != ${ucx_tls%tcp*}${ucx_tls##*tcp} ]];then
      export UCX_SOCKADDR_TLS_PRIORITY=tcp 
      export UCX_TLS=$ucx_tls
      export UCX_NET_DEVICES=mlx5_bond_0:1,bond0
  else
      export UCX_TLS=$ucx_tls
      export UCX_NET_DEVICES=mlx5_bond_0:1
  fi
  printf "TLS=$UCX_TLS, "
  printf "DEVICES=$UCX_NET_DEVICES "
fi

export UCX_LOG_LEVEL=${UCX_LOG_LEVEL:=DEBUG}

# export UCX_DC_MLX5_NUM_DCI=$dci

# export UCX_TLS=rc_x
# export UCX_SOCKADDR_TLS_PRIORITY=tcp
# export UCX_TLS=dc_x,tcp
# export UCX_NET_DEVICES=mlx5_bond_0:1,bond0
# export UCX_RDMA_CM_RESERVED_QPN=no
# export UCX_SOCKADDR_TLS_PRIORITY=rdmacm,tcp,sockcm
# export UCX_TLS=dc_x
# export UCX_NET_DEVICES=mlx5_bond_0:1
# export UCX_IB_GID_INDEX=5
# export UCX_RNDV_THRESH=4kb
# export UCX_RNDV_SCHEME=am
# export UCX_RC_ROCE_PATH_FACTOR=2
# export UCX_DC_ROCE_PATH_FACTOR=2
# export UCX_DC_TX_POLL_ALWAYS=y
# export UCX_CM_USE_ALL_DEVICES=n

printf "\n"
