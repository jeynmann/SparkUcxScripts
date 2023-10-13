#!/usr/bin/env python3

import os
import time
import multiprocessing
# import paramiko
import pandas as pd

env = ''' \
UCX_LOG_LEVEL="FATAL" \
UCX_USE_MT_MUTEX="yes" \
UCX_SOCKADDR_TLS_PRIORITY="tcp,rdmacm" \
UCX_TLS="dc_x,tcp" \
UCX_NET_DEVICES="mlx5_bond_1:1,bond6" \
UCX_CM_USE_ALL_DEVICES="n" \
UCX_DC_MLX5_NUM_DCI="24" \
UCX_KEEPALIVE_INTERVAL="inf" \
UCX_ADDRESS_VERSION="v2" \
UCX_PROTO_ENABLE="n" \
'''

server_host = "1.1.60.11"
port = 4002
iter = 65536
demo_path = "./"

def check_task():
    try:
        with os.popen(f"ssh {server_host} 'while [ \"1\" ];do sleep 1;if [[ `netstat -antp|grep {port}` ]];then exit 0;fi;done' &>/dev/null") as f:
            f.readlines()
    except:
        pass

def server_task(cmd, que):
    with os.popen(f"ssh {server_host} 'pgrep ucx_perftest|xargs kill &>/dev/null'") as f:
        f.readlines()
    try:
        with os.popen(f"ssh {server_host} {cmd} &>/dev/null") as f:
            # print(f"ssh {server_host} {cmd}")
            if que:
                que.put("")
            f.readlines()
    except:
        pass

def client_task(cmd, que):
    lat = []
    with os.popen("pgrep ucx_perftest|xargs kill &>/dev/null") as f:
        f.readlines()
    with os.popen(f"cd {demo_path}") as f:
        f.readlines()
    try:
        with os.popen(cmd) as f:
            # print(cmd)
            for line in f.readlines():
                # print(line)
                lat = line.split(",")
    except:
        pass
    n = len(lat)
    if n <= 4:
        que.put(lat)
        return
    lat = [lat[1],lat[2],lat[3]]
    que.put(lat)


if __name__ == "__main__":
    log = {
        "mesg":[],
        "b_thresh":[],
        "z_thresh":[],
        "r_thresh":[],
        "lat 50":[],
        "lat avg":[],
        "lat --":[],
    }
    mesg_size = [10 << 10, 50 << 10, 100 << 10, 250 << 10, 500 << 10, 1000 << 10]
    b_thresh = ["auto", "1k", "1g"]
    z_thresh = ["auto", "1k", "10k", "40k"]
    r_thresh = ["auto", "1k", "10k", "40K", "100k"]

    for msize in mesg_size:
        for bth in b_thresh:
            for zth in z_thresh:
                for rth in r_thresh:
                    que = multiprocessing.Queue()
                    scmd = f"'cd {demo_path} && UCX_ZCOPY_THRESH={zth} UCX_RNDV_THRESH={rth} {env} ./install/bin/ucx_perftest -p {port} -t ucp_am_bw -s {msize} -n {iter} -T 1 -M multi -efv #> tmp.log'"
                    stask = multiprocessing.Process(target=server_task, args=[scmd, None])
                    stask.start()
                    ktask = multiprocessing.Process(target=check_task)
                    ktask.start()
                    ktask.join()
                    res = []
                    id = 0
                    while not res:
                        if id == 3:
                            print(f"time out {ccmd}")
                            id += 1
                        time.sleep(1)
                        ccmd = f"UCX_ZCOPY_THRESH={zth} UCX_RNDV_THRESH={rth} {env} ./install/bin/ucx_perftest -p {port} -t ucp_am_bw -s {msize} -n {iter} -T 1 -M multi -efv {server_host} &>/dev/stdout | grep {iter},"
                        ctask = multiprocessing.Process(target=client_task, args=[ccmd, que])
                        ctask.start()
                        res = que.get()
                        ctask.join()
                    stask.join()
                    log["mesg"].append(msize)
                    log["b_thresh"].append(bth)
                    log["z_thresh"].append(zth)
                    log["r_thresh"].append(rth)
                    log["lat 50"].append(res[0])
                    log["lat avg"].append(res[1])
                    log["lat --"].append(res[2])
                    # print(pd.DataFrame(log).to_markdown())
                    # exit(0)
    # ssh_client.close()
    print(pd.DataFrame(log).to_markdown())