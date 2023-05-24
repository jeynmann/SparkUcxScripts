#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import re
import sys
import os


print("|{:>6s}|{:>6s}|{:>6s}|{:>6s}|{:<26s}|".format('type','real','user','sys','dir'))

l="user=1.05 sys=1.07, real=0.16 secs"
tm_pat = re.compile(
    'user=(?P<u>(\d+\.)?\d+) sys=(?P<s>(\d+\.)?\d+), real=(?P<r>(\d+\.)?\d+)')
fg_pat = re.compile('Full GC ')

rdir="/images/userlogs"
for dir in sorted(os.listdir(rdir)):
    mdir = ''
    mu = ms = mr = 0.
    mfu = mfs = mfr = 0.
    for sub in sorted(os.listdir(f"{rdir}/{dir}")):
        file=f"{rdir}/{dir}/{sub}/stdout"
        u = s = r = fu = fs = fr = 0.
        with open(file) as f:
            for l in f.readlines():
                tm_mat = tm_pat.search(l)
                fg_mat = fg_pat.search(l)
                if tm_mat:
                    u += float(tm_mat['u'])
                    s += float(tm_mat['s'])
                    r += float(tm_mat['r'])
                if fg_mat:
                    fu += float(tm_mat['u'])
                    fs += float(tm_mat['s'])
                    fr += float(tm_mat['r'])
        if mfr < fr:
            mdir = file
            mu,ms,mr = u,s,r
            mfu,mfs,mfr = fu,fs,fr
    print("|{:>6s}|{:>6.2f}|{:>6.2f}|{:>6.2f}|{:<26s}|".format('', mr, mu, ms, mdir))
    print("|{:>6s}|{:>6.2f}|{:>6.2f}|{:>6.2f}|{:<26s}|".format('full', mfr, mfu, mfs, mdir))
