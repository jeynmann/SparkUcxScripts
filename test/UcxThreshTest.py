#!/usr/bin/env python3

import os
import time
import multiprocessing
# import paramiko
import pandas as pd

env = ''' \
UCX_LOG_LEVEL="WARN" \
UCX_USE_MT_MUTEX="yes" \
UCX_SOCKADDR_TLS_PRIORITY="tcp,rdmacm" \
UCX_TLS="dc_x,tcp" \
UCX_NET_DEVICES="mlx5_bond_1:0,bond6" \
UCX_CM_USE_ALL_DEVICES="n" \
UCX_DC_MLX5_NUM_DCI="24" \
UCX_KEEPALIVE_INTERVAL="inf" \
UCX_ADDRESS_VERSION="v2" \
UCX_PROTO_ENABLE="n" \
'''

server_host = "192.168.20.11"
demo_path = "./"

def server_task(cmd, que):
    buf = []
    os.popen(f"ssh {server_host} 'pgrep demo|xargs kill &>/dev/null'")
    try:
        with os.popen(f"ssh {server_host} {cmd} &>/dev/null") as f:
            # print(f"ssh {server_host} {cmd}")
            for line in f.readlines():
                buf.append(line)
    except:
        pass
    if que:
        que.put(buf)

def client_task(cmd, que):
    lat = []
    os.popen("pgrep demo|xargs kill &>/dev/null")
    os.popen(f"cd {demo_path}")
    try:
        with os.popen(cmd) as f:
            # print(cmd)
            for line in f.readlines():
                lat.append(float(line))
    except:
        pass
    n = len(lat)
    if n == 0:
        que.put([0,0,0,0])
        return
    avg = sum(lat) / n
    sorted_lat = sorted(lat, reverse=False)
    lat = (avg,sorted_lat[n * 9 // 10],sorted_lat[n * 99 // 100],sorted_lat[-1])
    que.put(lat)


if __name__ == "__main__":
    log = {
        "mesg":[],
        "b_thresh":[],
        "z_thresh":[],
        "r_thresh":[],
        "lat avg":[],
        "lat 90":[],
        "lat 99":[],
        "lat 100":[],
    }
    mesg_size = [10 << 10, 50 << 10, 100 << 10, 250 << 10, 500 << 10, 1000 << 10]
    b_thresh = ["1k", "1g"]
    z_thresh = ["5k", "10k", "40k"]
    r_thresh = ["8k", "32K", "64K", "128K", "256K", "512K"]

    for msize in mesg_size:
        for bth in b_thresh:
            for zth in z_thresh:
                for rth in r_thresh:
                    que = multiprocessing.Queue()
                    scmd = f"'cd {demo_path} && UCX_ZCOPY_THRESH={zth} UCX_RNDV_THRESH={rth} {env} test/apps/iodemo/io_demo -d {msize} -i0 -A > tmp.log'"
                    stask = multiprocessing.Process(target=server_task, args=[scmd, None])
                    stask.start()
                    ccmd = f"UCX_ZCOPY_THRESH={zth} UCX_RNDV_THRESH={rth} {env} test/apps/iodemo/io_demo -d {msize} -i0 -A -w1 1.1.60.11 | grep 'latency:\d+\.\d+' -Po|cut -d':' -f2"
                    ctask = multiprocessing.Process(target=client_task, args=[ccmd, que])
                    ctask.start()
                    time.sleep(8)
                    os.system("pgrep demo|xargs kill")
                    ctask.join()
                    stask.terminate()
                    res = que.get()
                    log["mesg"].append(msize)
                    log["b_thresh"].append(bth)
                    log["z_thresh"].append(zth)
                    log["r_thresh"].append(rth)
                    log["lat avg"].append(res[0])
                    log["lat 90"].append(res[1])
                    log["lat 99"].append(res[2])
                    log["lat 100"].append(res[3])
                    print(pd.DataFrame(log).to_markdown())
    # ssh_client.close()
    print(pd.DataFrame(log).to_markdown())
