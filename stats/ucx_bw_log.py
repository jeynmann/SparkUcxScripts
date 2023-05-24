#!/usr/bin/env python3

import re
import os

read_stats = {}
write_stats = {}
host_stats = {}

host_pat = re.compile('gen-mtvr-\d+')
act_pat = re.compile(f'active: ?(\d+)/(\d+)')
read_pat = re.compile('read (\d+)\.\d+ MBs')
write_pat = re.compile('write (\d+)\.\d+ MBs')

if __name__ == '__main__':
    prefix = ''
    if os.environ.get('server'):
        prefix = 'server.'
    par = '/tmp'
    print("|{:<24s}|{:>8s}|".format("file", "rate"))
    print("|{:-<24s}|{:->8s}|".format("", ""))
    for file in sorted(os.listdir(par)):
        if not file.startswith(f'{prefix}gen-mtvr-'):
            continue
        host = host_pat.search(file).group(0)
        with open(f'{par}/{file}') as f:
            read_rate = []
            write_rate = []
            active_num = 0
            for l in f.readlines():
                act_mat = act_pat.search(l)
                if not act_mat:
                    continue
                act = int(act_mat[1])
                all = int(act_mat[2])
                if act != all:
                    continue
                if active_num < act:
                    active_num = act
                    read_rate = []
                    write_rate = []
                read_mat = read_pat.search(l)
                if read_mat:
                    read_rate.append(int(read_mat.group(1)))
                    continue
                write_mat = write_pat.search(l)
                if write_mat:
                    write_rate.append(int(write_mat.group(1)))
                    continue
            avg_rate = 0
            if read_rate:
                avg_rate = sum(read_rate)/len(read_rate)
                read_stats[file] = avg_rate
            elif write_rate:
                avg_rate = sum(write_rate)/len(write_rate)
                write_stats[file] = avg_rate
            else:
                print(file)
                continue
            if not host_stats.get(host):
                host_stats[host] = []
            host_stats[host].append(avg_rate)
        print("|{:<24s}|{:>8.1f}|".format(file, avg_rate))
    print("|{:<24s}|{:>8s}|".format("hostname", "rate"))
    print("|{:-<24s}|{:->8s}|".format("", ""))
    for host, rates in host_stats.items():
        print("|{:<24s}|{:>8.1f}|".format(host, sum(rates)))
