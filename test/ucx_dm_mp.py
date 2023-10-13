#!/usr/bin/env python3

import os
import sys
import time
import multiprocessing

'''
example:
ucx_path=/mnt/d/code/ucx/install host=1.1.60.11 num=4 stat_wait=10 ./ucx_dm_mp.py
ucx_path=/mnt/d/code/ucx/install ./ucx_dm_mp.py
'''

__kill_cmd="pgrep io_demo|xargs kill > /dev/null"

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
    perf_size = os.environ.get("perf_size", "$((8<<20))")
    perf_iter = os.environ.get("perf_iter", "0")
    port = int(os.environ.get("port", "61440"))
    host = os.environ.get("host", "")
    eth_ports = os.environ.get("eth_ports", "eth0,eth1").split(",")
    log = os.environ.get("log", "./")
    timeout = int(os.environ.get("timeout", "1"))
    stat_wait = int(os.environ.get("stat_wait", "5"))

    # pgrep ucx_perftest|xargs kill

    base_env = f"\
    LD_LIBRARY_PATH={ucx_path}/lib \
    UCX_TLS={ucx_tls} \
    UCX_NET_DEVICES={ucx_dev} \
    UCX_IB_GID_INDEX={ucx_gid} \
    "

    base_cmd = f"\
    {ucx_path}/bin/io_demo \
    -d {perf_size} \
    -i {perf_iter} \
    -A \
    {host}"

    jobs = []
    if num < 1:
        os.system(f"{base_env} {base_cmd}")
        exit(0)

    def launch_perf(cmd, que):
        res = "0"
        try:
            with os.popen(cmd=cmd) as f:
                res = []
                for l in f.readlines():
                    res = l.split()[-2].strip()
        except:
            pass
        finally:
            if que:
                que.put(res)
    for i in range(num):
        cmd = f"{base_env} {base_cmd} -p {port+i}"
        job = multiprocessing.Process(target=launch_perf, args=[cmd, None])
        jobs.append(job)
    for j in jobs:
        j.start()
    def get_stat():
        tx_bytes_phy = 0
        for eth_port in eth_ports:
            with os.popen(f"ethtool -S {eth_port}|grep tx_bytes_phy:") as f:
                # print(f.readlines())
                tx_bytes_phy += int(f.readlines()[-1].split(":")[-1].strip())
        return tx_bytes_phy
    if not host:
        for j in jobs:
            j.join()
    else:
        time.sleep(stat_wait)
        tx_bytes_phy = get_stat()
        time.sleep(1)
        next_tx_bytes_phy = get_stat()
        print((next_tx_bytes_phy - tx_bytes_phy) >> 27)
        # terminate
        expect = time.time() + timeout
        for j in jobs:
            j.join(max(expect - time.time(), 0))
            j.terminate()