#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import re
import sys
import os
# from functools import reduce

# 2023-05-26T16:46:34.273+0800: 4.831: [GC (Allocation Failure) [PSYoungGen: 4660736K->1164794K(5825536K)] 4806845K->3043591K(19806720K), 0.1932609 secs] [Times: user=0.42 sys=1.12, real=0.20 secs]
# 2023-06-07T07:10:38.158+0300: 1.167: [Full GC (Metadata GC Threshold) [PSYoungGen: 33593K->0K(5825536K)] [ParOldGen: 88K->32815K(7408128K)] 33681K->32815K(13233664K), [Metaspace: 20819K->20819K(1067008K)], 0.0270750 secs] [Times: user=0.06 sys=0.01, real=0.03 secs]
# 2023-06-07T07:10:53.459+0300: 16.469: [GC (Allocation Failure) [PSYoungGen: 4660736K->1164790K(5825536K)] 4693551K->3131175K(13233664K), 0.3855215 secs] [Times: user=0.49 sys=1.76, real=0.39 secs]
# 2023-06-07T07:11:10.334+0300: 33.344: [GC (Allocation Failure) [PSYoungGen: 5825526K->1164795K(5825536K)] 7791911K->6833071K(13233664K), 0.6909625 secs] [Times: user=0.87 sys=3.26, real=0.69 secs]
# 2023-06-07T07:11:11.026+0300: 34.035: [Full GC (Ergonomics) [PSYoungGen: 1164795K->0K(5825536K)] [ParOldGen: 5668276K->6814091K(13981184K)] 6833071K->6814091K(19806720K), [Metaspace: 32328K->32328K(1077248K)], 0.6147199 secs] [Times: user=1.89 sys=1.40, real=0.61 secs]
gc_pat = re.compile(
    'GC \((?P<GCtype>[\w ]+)\).* (?P<td>\d+\.\d+) secs.* user=(?P<u>(\d+\.)?\d+) sys=(?P<s>(\d+\.)?\d+), real=(?P<r>(\d+\.)?\d+)')
young_gc_pat = re.compile("PSYoungGen: (?P<start>\d+)K->(?P<end>\d+)K")
old_gc_pat = re.compile("ParOldGen: (?P<start>\d+)K->(?P<end>\d+)K")
meta_gc_pat = re.compile("Metaspace: (?P<start>\d+)K->(?P<end>\d+)K")


class GCStat:
    def __init__(self) -> None:
        self.reset()
        self.get = GCStat.__get

    def reset(self):
        self.max_gc = 0.
        # the log path for the max GC time
        self.max_path = ''
        # GC time cost and recycled memory size
        self.max_stats = {}

    def gc_log(self, file):
        gc_sum = 0.
        stats = {}
        with open(file) as f:
            for l in f.readlines():
                GCmat = gc_pat.search(l)
                if GCmat:
                    GCtype = GCmat["GCtype"]
                    u = float(GCmat['u'])
                    s = float(GCmat['s'])
                    r = float(GCmat['r'])
                    gc_sum += r
                    GCtime = float(GCmat["td"]) * 1000  # secs -> milli secs
                    GCmat_young = young_gc_pat.search(l)
                    GCsize_young = (
                        float(GCmat_young["start"]) - float(GCmat_young["end"])) / 2 ** 10  # KB -> MB
                    GCmat_old = old_gc_pat.search(l)
                    GCsize_old = 0 if not GCmat_old else (
                        float(GCmat_old["end"]) - float(GCmat_old["start"])) / 2 ** 10  # KB -> MB
                    GCmat_meta = meta_gc_pat.search(l)
                    GCsize_meta = 0 if not GCmat_meta else (
                        float(GCmat_meta["start"]) - float(GCmat_meta["end"])) / 2 ** 10  # KB -> MB
                    stat = stats.get(GCtype, [])
                    stat.append(
                        (GCtime, GCsize_young, GCsize_old, GCsize_meta, r, u, s))
                    stats[GCtype] = stat
        # print(stats)
        if self.max_gc < gc_sum:
            self.max_gc = gc_sum
            self.max_path = file
            self.max_stats = stats

    '''
    - gc_log_dir
       |- gc.1.log
       |- gc.2.log
    '''

    def gc_logdir(self, dir, aggregate=False):
        for sub in sorted(os.listdir(f"{dir}")):
            try:
                self.gc_log(self.get(dir, sub))
                if not aggregate:
                    self.print_gc_stat()
                    self.reset()
            except:
                pass
        if aggregate:
            self.print_gc_stat()
            self.reset()

    '''
    - application_dir
       |- container1/stdout
       |- container2/stdout
    '''

    def use_spark_log(self):
        def __inner_get(dir, sub):
            return f"{dir}/{sub}/stdout"
        self.get = __inner_get

    def use_spark_gclog(self):
        def __inner_get(dir, sub):
            return f"{dir}/{sub}/gc.log"
        self.get = __inner_get

    @staticmethod
    def print_gc_head():
        print("|{:>22s}|{:>4s}|{:>8s}|{:>8s}|{:>8s}|{:>4s}|{:>6s}|{:>6s}|{:>6s}|{:>6s}|".format(
            'type', 'num', 'time/ms', 'size/M', 'rate:G/s', 'Y/O%', 'real', 'user', 'sys', 'appid'))
        print("|{:->22s}|{:->4s}|{:->8s}|{:->8s}|{:->8s}|{:->4s}|{:->6s}|{:->6s}|{:->6s}|{:->6s}|".format(
            '', '', '', '', '', '', '', '', '', '',))

    def print_gc_stat(self):
        if not self.max_stats:
            return
        sum_stat = {}
        for gctype, stat in self.max_stats.items():
            cur_stat = [sum(i) for i in zip(*stat)]
            cur_stat.append(len(stat))
            sum_stat[gctype] = cur_stat
        for gctype, cur_stat in sum_stat.items():
            # (GCtime, GCsize_young, GCsize_old, GCsize_meta, r, u, s)
            print("|{:>22s}|{:>4d}|{:>8.2f}|{:>8.2f}|{:>8.2f}|{:>4.1f}|{:>6.2f}|{:>6.2f}|{:>6.2f}|{:>6s}|".format(
                gctype, cur_stat[-1], cur_stat[0], cur_stat[1] + cur_stat[3], (cur_stat[1] + cur_stat[3]) / cur_stat[0], cur_stat[2] * 100 / cur_stat[1], cur_stat[4], cur_stat[5], cur_stat[6], self.max_path.split('/')[-3].split('_')[-1]))

    @staticmethod
    def __get(dir, sub):
        return f"{dir}/{sub}"


if __name__ == "__main__":
    stat = GCStat()
    GCStat.print_gc_head()
    if len(sys.argv) > 1:
        stat = GCStat()
        dir = sys.argv[1]
        stat.gc_logdir(dir)
        sys.exit(0)

    dir = "userlogs"
    appno = "1685332126803"
    ignores_range = range(1, 82)
    allowed_range = range(82, 82)
    ignores = set()
    for i in ignores_range:
        ignores.add(f"application_{appno}_{i:0>4}")
    allowed = set()
    for i in allowed_range:
        allowed.add(f"application_{appno}_{i:0>4}")
    for entry in sorted(os.listdir(dir)):
        if not entry.startswith('application_'):
            continue
        if ignores and entry in ignores:
            continue
        if allowed and entry not in allowed:
            continue
        stat = GCStat()
        stat.use_spark_gclog()
        stat.gc_logdir(f"{dir}/{entry}", True)
