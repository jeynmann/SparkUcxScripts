#!/usr/bin/env python3

import multiprocessing
import paramiko
import os
import argparse

class MPLogs:
    __inst = None

    @staticmethod
    def inst():
        if not MPLogs.__inst:
            MPLogs.__inst = MPLogs()
        return MPLogs.__inst

    @staticmethod
    def clean():
        MPLogs.__inst == None

    def __init__(self) -> None:
        self.logs = {}

    def __getitem__(self, key):
        return self.logs[key]

    def __setitem__(self, key, val):
        self.logs[key] = val

    def add_proc(self, key):
        return self.logs.setdefault(key,multiprocessing.Queue())

    def len(self):
        return len(self.logs)

    def show(self):
        for k, q in self.logs.items():
            print('{:-^48s}\n'.format(k))
            for l in q.get():
                print(l)
            print('{:-^48s}\n'.format(k))


class MPSSHCmd:
    def __init__(self, username, password, servers, port="22", timeout=1):
        self._username = username
        self._password = password
        self._servers = servers
        self._port = port
        self._timeout = timeout

    @staticmethod
    def _exec_cmd(username, password, server, port, timeout, commands, mplogs = None):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        if password:
            ssh_client.connect(hostname=server, username=username, port=port, password=password, timeout=timeout)
        else:
            ssh_client.connect(hostname=server, username=username, port=port, timeout=timeout)
        output = []
        for cmd in commands:
            _, stdout, stderr = ssh_client.exec_command(cmd)
            output.append(f'$ {cmd}')
            output.append(stdout.read().decode("utf-8"))
            output.append(stderr.read().decode("utf-8"))
        if mplogs:
            mplogs.put(output)
        ssh_client.close()

    def __call__(self, commands):
        processes = []
        mplogs = MPLogs()
        for server in self._servers:
            my_log = mplogs.add_proc(server)
            processes.append(multiprocessing.Process(target=self._exec_cmd, args=(self._username, self._password, server, self._port, self._timeout, commands, my_log)))
        for th in processes:
            th.start()
        for th in processes:
            th.join()
        mplogs.show()


def _parse_args():
    default_servers = [
        "127.0.0.0",
        "127.0.0.1",
    ]
    default_commands = [
        'pwd',
        'cd /.autodirect/swgwork/zizhao/spark/setup_spark2 && fmt=y sh hdfs.sh',
        'ls -lh /images/*/'
    ]
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--servers', type=str, nargs='+', default=default_servers)
    parser.add_argument('-c', '--commands', type=str, nargs='+', default=default_commands)
    parser.add_argument('-u', '--username', type=str, default='')
    parser.add_argument('-p', '--password', type=str, default='')
    return parser.parse_args()

if __name__ == '__main__':
    args = _parse_args()
    username = args.username
    servers = args.servers
    commands = args.commands
    password = args.password
    if not username:
        with os.popen('whoami') as f:
            username = f.readline().strip()
    print(f'user: \e[31m{username}\e[0m')
    MPSSHCmd(username, password, servers)(commands)

