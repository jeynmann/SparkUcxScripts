#!/usr/bin/env python3

import os
import sys
import time
import multiprocessing

'''
example:
ucx_path=/mnt/d/code/ucx/install host=1.1.60.11 ./ucx_pf_mp.py
ucx_path=/mnt/d/code/ucx/install ./ucx_pf_mp.py
'''

__kill_cmd="pgrep ucx_perftest|xargs kill > /dev/null"

if __name__ == "__main__":
    arg = sys.argv
    if len(arg) > 1 and arg[1] == "-s":
        os.system(__kill_cmd)
        exit(0)

    ucx_path = os.environ.get("ucx_path", "/usr")
    ucx_tls = os.environ.get("ucx_tls", "dc_x,sm")
    ucx_dev = os.environ.get("ucx_dev", "mlx5_bond_0:1")
    ucx_gid = os.environ.get("ucx_gid", "3")
    num = int(os.environ.get("num", "8"))
    perf_type = os.environ.get("perf_type", "ucp_am_bw")
    perf_size = os.environ.get("perf_size", "$((8<<20))")
    perf_iter = os.environ.get("perf_iter", "8192")
    port = int(os.environ.get("port", "61440"))
    perf_thread = os.environ.get("perf_thread", "1")
    host = os.environ.get("host", "")
    log = os.environ.get("log", "./")
    timeout = int(os.environ.get("timeout", "20"))
    eth_ports = os.environ.get("eth_ports", "eth0,eth1").split(",")
    stat_wait = int(os.environ.get("stat_wait", "5"))

    # pgrep ucx_perftest|xargs kill

    base_env = f"\
    LD_LIBRARY_PATH={ucx_path}/lib \
    UCX_TLS={ucx_tls} \
    UCX_NET_DEVICES={ucx_dev} \
    UCX_IB_GID_INDEX={ucx_gid} \
    "

    base_cmd = f"\
    {ucx_path}/bin/ucx_perftest \
    -t {perf_type} \
    -s {perf_size} \
    -n {perf_iter} \
    -T {perf_thread} \
    -M multi \
    -f {host}"

    jobs = []
    ques = []
    if num < 1:
        os.system(f"{base_env} {base_cmd}")
        exit(0)

    def launch_perf(cmd, que):
        res = "0"
        try:
            with os.popen(cmd=cmd) as f:
                res = f.readlines()[-1].split()[5].strip()
        except:
            pass
        finally:
            if que:
                que.put(res)
    for i in range(num):
        cmd = f"{base_env} {base_cmd} -p {port+i}"
        que = multiprocessing.Queue() if host else None
        job = multiprocessing.Process(target=launch_perf, args=[cmd, que])
        jobs.append((i, job))
        ques.append(que)
    for _,j in jobs:
        j.start()
    def get_stat():
        tx_bytes_phy = 0
        for eth_port in eth_ports:
            with os.popen(f"ethtool -S {eth_port}|grep tx_bytes_phy:") as f:
                # print(f.readlines())
                tx_bytes_phy += int(f.readlines()[-1].split(":")[-1].strip())
        return tx_bytes_phy
    if not host:
        for _,j in jobs:
            j.join()
    else:
        if stat_wait:
            time.sleep(stat_wait)
            tx_bytes_phy = get_stat()
            time.sleep(1)
            next_tx_bytes_phy = get_stat()
            print((next_tx_bytes_phy - tx_bytes_phy) >> 27)
        bw = []
        expect = time.time() + timeout
        for _,j in jobs:
            j.join(max(expect - time.time(), 0))
        for i,j in jobs:
            if not j.is_alive() and ques[i]:
                bw.append(float(ques[i].get()))
            j.terminate()
        if not stat_wait:
            # print(bw)
            print(sum(bw)*8)