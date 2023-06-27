#!/usr/bin/env python3

import os
import sys
import re
import logging
import pandas as pd


class ResultParser:
    def __init__(self, log=None):
        logh = logging.StreamHandler(
            sys.stderr) if not log else logging.FileHandler(log, encoding="utf-8")
        logh.setFormatter(
            logging.Formatter(fmt="%(asctime)s %(message)s", datefmt="%X"))
        self.log = logging.Logger('Result parser', logging.INFO)
        self.log.addHandler(logh)
        self.stat = {}

    def parse(self, file):
        # table = pd.read_csv(file)
        # print(f"processing: {date_mat.group(0)}")
        # date_pat = re.compile('\d+-\d+-\d+')
        # date_mat = date_pat.search(table.columns[0])
        # table.columns = table.iloc[0]
        # table = table.drop(0)
        # cols = table.index.size
        # ele_pat = re.compile('(".+"|[^,]+)')
        stat = self.stat
        ele_id = {}
        is_head = True
        with open(file) as f:
            for l in f.readlines():
                # Check if it is datetime line
                date_mat = re.compile('\d+-\d+-\d+').search(l)
                if date_mat:
                    print(f"processing: {date_mat.group(0)}")
                    continue
                # Result data line
                ele = [e.strip() for e in l.strip().split(',')]
                ele_size = len(ele)
                if is_head:
                    for i in range(ele_size):
                        ele_id[ele[i]] = i
                    is_head = False
                    if "status" not in ele_id:
                        break
                else:
                    try:
                        if "success" not in ele[ele_id["status"]]:
                            continue
                        time = float(
                            ele[ele_id["sum"]].removesuffix('min'))
                        read = float(
                            ele[ele_id["shuffle read stage"]].removesuffix('min'))
                        write = float(
                            ele[ele_id["shuffle write stage"]].removesuffix('min'))
                        #
                        mappers = int(ele[ele_id["numMappers"]])
                        kvpairs = int(ele[ele_id["numKVPairs"]])
                        keysize = int(ele[ele_id["KeySize"]])
                        data = "{:.2}".format(
                            mappers * kvpairs * keysize / 2 ** 40)
                        apps = ele[ele_id["application"]]
                        exes = ele[ele_id["numExecutors"]]
                        cores = ele[ele_id["executorCores"]]
                        if apps and ('Group' not in apps):
                            print(file)
                            exit(0)
                        key_dat = f"{data},{apps},{exes},{cores}"
                        #
                        mode = ele[ele_id["mode"]]
                        sparkucx = ele[ele_id.get("sparkucx_version", 0)]
                        ucx = ele[ele_id.get("ucx_version", 0)]
                        key_mod = f"{mode},{sparkucx},{ucx}"
                        data_stat = stat.get(key_dat, {})
                        mode_stat = data_stat.get(key_mod, [])
                        mode_stat.append((time, read, write))
                        data_stat[key_mod] = mode_stat
                        stat[key_dat] = data_stat
                    except ValueError as e:
                        self.log.warning(l.strip())
                        self.log.warning(repr(e))
                        continue
                    except Exception as e:
                        self.log.warning(l.strip())
                        buf = ""
                        for k, i in ele_id.items():
                            buf = f"{buf}, {k}:{ele[i]}"
                        self.log.warning(buf)
                        self.log.error(f"Exception in {file}:\n\t{repr(e)}")
                        exit(0)

    def process(self, file=None):
        stat = self.stat
        show_stat = [self.get_head()]

        def get_base(data_stat, key):
            for k in data_stat.keys():
                if key in k:
                    base_stat = data_stat[k]
                    base_pps = self.get_pps(zip(*base_stat))
                    base_str = self.get_base_str(base_pps)
                    show_stat.append(f"{data_exe_core},{k},{base_str}\n")
                    # print(show_stat)
                    return base_stat, base_pps
            return None, None
        for data_exe_core, data_stat in sorted(stat.items(), key=lambda x: x[0]):
            base_stat, base_pps = get_base(data_stat, 'TCP,')
            if not base_stat:
                base_stat, base_pps = get_base(data_stat, 'Brianv1,')
            if not base_stat:
                continue
            for mode, mode_stat in data_stat.items():
                if mode_stat is base_stat:
                    continue
                mode_pps = self.get_pps(zip(*mode_stat))
                mode_str = self.get_diff_str(base_pps, mode_pps)
                show_stat.append(
                    f"{data_exe_core},{mode},{mode_str}\n")
        if not file:
            for l in show_stat:
                print(l)
        else:
            with open(file, 'w') as f:
                f.writelines(show_stat)
            tb = pd.read_csv(file)
            tb.to_markdown(sys.stdout)
            # exit(0)

    def get_head(self):
        return "Data/T,Application,Executors,Cores,Mode,Sparkucx,Ucx,Num,Time,Time50,Time80,Time99,Read,Read50,Read80,Read99,Write,Write50,Write80,Write99\n"

    def get_pps(self, stat_list):
        pps = []
        n = 0
        for ele in stat_list:
            if not n:
                n = len(ele)
                pps.append(n)
            ele = sorted(ele)
            n50 = (n - 1) * 50 // 100
            n99 = (n - 1) * 80 // 100
            n100 = (n - 1) * 99 // 100
            pps.append(sum(ele) / n)
            pps.append(ele[n50])
            pps.append(ele[n99])
            pps.append(ele[n100])
        return pps

    def get_base_str(self, base_pps):
        stat_str = "{}".format(base_pps[0])
        for ele in base_pps[1:]:
            stat_str = "{},{:.2}".format(stat_str, ele)
        return stat_str

    def get_diff_str(self, base_pps, next_pps):
        stat_str = "{}".format(next_pps[0])
        for i in range(1, len(base_pps)):
            stat_str = "{},{:>}%".format(
                stat_str, int((base_pps[i]/next_pps[i])*100))
        return stat_str


# /mnt/c/Users/zizhao/local_tmp/result/result_cx6.csv
if __name__ == "__main__":
    root_dir = [
        "/mnt/c/Users/zizhao/local_tmp/result/",
        "/mnt/c/Users/zizhao/local_tmp/sparkCX6/"
    ]
    for dir in root_dir:
        dir_parser = ResultParser()
        for file in sorted(os.listdir(dir)):
            if not file.startswith("result"):
                continue
            if file.endswith("diff.csv"):
                continue
            if not file.endswith(".csv"):
                continue
            path = f"{dir}/{file}"
            parser = ResultParser()
            try:
                dir_parser.parse(path)
                parser.parse(path)
                parser.process(f"{path}.diff.csv")
            except Exception as e:
                print(f"Exception in {path}:\n\t{repr(e)}")
                # exit(0)
        dir_parser.process(f"{dir}/result_total.diff.csv")
