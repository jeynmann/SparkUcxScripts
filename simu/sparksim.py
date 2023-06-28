#!/usr/bin/env python3

import os
import math
import sys
# import random
# import multiprocessing

if __name__ == '__main__':
    script_dir = os.path.dirname(sys.argv[0])

    if os.environ.get('stop'):
        with os.popen('pgrep demo|xargs kill -2 && sleep 2') as f:
            f.readlines()
        # with os.popen('pgrep demo|xargs kill -9 && sleep 2') as f:
        #     f.readlines()
        print("demo procs left to exit:")
        with os.popen('ps -ef|grep demo') as f:
            for l in f.readlines():
                print(l.strip())
        exit(0)

    print("============ demo ============")

    default_nodes = (
        '192.168.20.9',
        '192.168.20.10',
        '192.168.20.11',
        '192.168.20.12',
        '192.168.20.13',
        '192.168.20.14',
        '192.168.20.15',
    )
    nodes = os.environ.get('nodes')
    if nodes:
        nodes = nodes.split(',')
    else:
        nodes = default_nodes
    numHosts = int(os.environ.get('numHosts', len(nodes)))
    numExecutors = int(os.environ.get('numExecutors', 48))
    executorCores = int(os.environ.get('executorCores', 4))
    numListenerThreads = int(os.environ.get('numListenerThreads', 4))
    msgLen = int(os.environ.get('msgLen', 16 * 1024 * 1024))
    clientMode = int(os.environ.get('clientMode', 1))

    print("------------  arg ------------")
    print(f"numHosts={numHosts}")
    print(f"numExecutors={numExecutors}")
    print(f"executorCores={executorCores}")
    print(f"numListenerThreads={numListenerThreads}")
    print(f"msgLen={msgLen}")
    print(f"clientMode={clientMode}")
    print("------------  arg ------------")

    print("------------ info ------------")

    with os.popen("/usr/sbin/show_gids|grep -Po '\d+\.\d+\.\d+\.\d+'") as f:
        prefix = nodes[0][:nodes[0].rfind('.')] if nodes else '192.168.20'
        for l in f.readlines():
            if prefix in l:
                nodeIp = l.strip()
                break
        else:
            exit(0)
    with os.popen("lscpu|grep '^CPU(s)'|awk '{print $2}'") as f:
        coreNum = int(f.readline())
    print(f" node ip: {nodeIp}")

    print(f"  CPU(s): {coreNum}")

    print("------------ info ------------")

    print("------------ proc ------------")
    coreId = 13 # if clientMode == 0 else 45
    numExePerHost = math.ceil(numExecutors/numHosts)

    if clientMode != 0:
        remote = ''
        for node in nodes:
            if node != nodeIp:
                for _eid in range(numExePerHost):
                    for _sid in range(numListenerThreads):
                        port = _eid*numListenerThreads+_sid+3336
                        remote = f"{remote}{node}:{port} "
        # print(" remote: {remote}")

    for eid in range(numExePerHost):
        coreId = (coreId + 1) % coreNum
        cores = f"{coreId}"
        for _ in range(executorCores - 1):
            coreId = (coreId + 1) % coreNum
            cores = f"{cores},{coreId}"
        # # bind cores
        os.environ['core0'] = f"{cores}"
        os.environ['wind'] = '1'

        roce_factor = 3
        if clientMode != 0:
            print("************  cli ************")
            print(f"executor: {eid}")
            for cid in range(executorCores):
                locals = ''
                # connect to other executors in the same host
                for _eid in range(numExePerHost):
                    if (_eid != eid):
                        for _sid in range(numListenerThreads):
                            port = _eid*numListenerThreads+_sid+3336
                            locals = f"{locals}{nodeIp}:{port} "
                os.environ['servers'] = f"{remote}{locals}"
                # print("nodeIp={},remote={},locals={}".format(nodeIp, remote, locals))
                os.environ['arg'] = "-o read"
                cmd = f'sh {script_dir}/mclient.sh /tmp/$HOSTNAME.{eid}.{cid}.log 1 0 {msgLen} 0'
                with os.popen(cmd) as f:
                    for l in f.readlines():
                        print(l.strip())
        else:
            print("************  srv ************")
            print(f"executor: {eid}")
            for sid in range(numListenerThreads):
                port = eid*numListenerThreads+sid+3336
                os.environ['arg'] = f"-p {port}"
                cmd = f'sh {script_dir}/mclient.sh /tmp/server.$HOSTNAME.{eid}.{sid}.log 1 0 {msgLen} 0'
                with os.popen(cmd) as f:
                    for l in f.readlines():
                        print(l.strip())

    print("------------ proc ------------")

    print("============ demo ============")
