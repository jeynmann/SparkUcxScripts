#!/usr/bin/env python3

import re
import os

tp_pat = re.compile('^(\d+):(\d+):(\d+).(\d+)')
fg_pat = re.compile('<zzh>')
fs_pat = re.compile('Get start')
fe_pat = re.compile('Get done')
bk_pat = re.compile('(\d)+ blocks of size: (\d)+')

stats=[]

if __name__ == '__main__':
    par = '.'
    for dir in sorted(os.listdir(par)):
        file = f"{par}/{dir}/stderr"
        time_cost=0
        with open(file) as f:
            ts = 0
            te = 0
            tc = 0
            for l in f.readlines():
                fg_mat = fg_pat.search(l)
                if not fg_mat:
                    continue
                tp_mat = tp_pat.search(l)
                if not tp_mat:
                    continue
                tp = int(tp_mat.group(1)) * 3600 \
                    + int(tp_mat.group(2)) * 60 \
                    + int(tp_mat.group(3)) \
                    + int(tp_mat.group(4)) / 1000.
                fs_mat = fs_pat.search(l)
                if fs_mat:
                    if tc == 0:
                        ts = tp
                    tc += 1
                    continue
                fe_mat = fe_pat.search(l)
                if fe_mat:
                    tc -= 1
                    if tc == 0:
                        te = tp
                        time_cost += te - ts
                    continue
        # print(f"{dir}")
        app_id = int(re.search('\d+$', dir).group(0))
        stats.append((app_id, time_cost))
    print("|{:<8s}|{:>8s}|".format("executor","time/s"))
    print("|{:-<8s}|{:->8s}|".format("", ""))
    for id, td in stats:
        print("|{:>8d}|{:>8.1f}|".format(id, td))
