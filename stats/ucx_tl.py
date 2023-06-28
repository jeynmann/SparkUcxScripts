#!/usr/bin/env python3
import os
import sys
import re


'''
12:08:17.102 [Executor task launch worker for task 1002] UcxWorkerWrapper DEBUG Sent message on UcpEndpoint(id=140522747388096, UcpEndpointParams{name=Endpoint to 12,errorHandlingMode=UCP_ERR_HANDLING_MODE_PEER,socketAddress=/192.168.20.12:54992,) to 12 to fetch 35 blocks on tag -1891616621 id 8589934596in 266231 ns
12:08:17.102 [Executor task launch worker for task 1002] UcxWorkerWrapper DEBUG Received rndv data of size: 3801692 for tag -1891616633 in 355008291 ns time from amHandle: 45901141 ns
'''

tp_pat = re.compile('(\d+):(\d+):(\d+)\.?(\d+)? ')
send_pat = re.compile('task (\d+).*?UcxWorkerWrapper.*? Sent message')
recv_pat = re.compile('task (\d+).*?UcxWorkerWrapper.*? Received')
tag_td_pat = re.compile('tag -?(\d+).*?in (\d+) ns')
size_pat = re.compile('size: (\d+)')


class Stat:
    _stat_dict = {}
    _size_dict = {}
    _intv_dict = {}
    _req_time_list = []
    _req_intv_list = []
    _max_req_time = 0
    _max_req_num = 0
    _max_req_intv = 0
    _max_req_intv_task = 0
    _max_req_intv_tag = 0
    _total_num = 0
    _req_num = 0

    def __init__(self) -> None:
        self.tag = 0
        self.task = 0
        self.send_tp = 0
        self.recv_tp = 0
        self.send_td = 0
        self.recv_td = 0
        self.size = 0
        self._total_td = 0

    def total_time(self):
        return self._total_td

    @classmethod
    def total_num(cls):
        return cls._total_num

    @classmethod
    def req_time_stat(cls):
        sorted_list = sorted(cls._req_time_list)
        num = len(sorted_list)
        avg = sum(sorted_list) // num
        p80 = sorted_list[(num - 1) * 80 // 100]
        p99 = sorted_list[(num - 1) * 99 // 100]
        return cls._max_req_time, avg, p80, p99

    @classmethod
    def req_int_stat(cls):
        sorted_list = sorted(cls._req_intv_list)
        num = len(sorted_list)
        avg = sum(sorted_list) // num
        p80 = sorted_list[(num - 1) * 80 // 100]
        p99 = sorted_list[(num - 1) * 99 // 100]
        return cls._max_req_intv, avg, p80, p99

    @classmethod
    def req_int_tag(cls):
        return cls._max_req_intv_task, cls._max_req_intv_tag

    @classmethod
    def max_req_num(cls):
        return cls._max_req_num

    @classmethod
    def stat_dict(cls):
        return cls._stat_dict

    @classmethod
    def size_dict(cls):
        return cls._size_dict

    @classmethod
    def parse(cls, line):
        s_mat = send_pat.search(line)
        r_mat = recv_pat.search(line)
        if not s_mat and not r_mat:
            return
        tp_mat = tp_pat.search(line)
        if not tp_mat:
            return
        # print(len(tp_mat.groups()))
        # may not have "ms" in timestamp
        if tp_mat[4]:
            tp = (int(tp_mat[1])*3600 + int(tp_mat[2])*60 +
                  int(tp_mat[3]))*1000 + int(tp_mat[4])
        else:
            tp = (int(tp_mat[1])*3600 + int(tp_mat[2])*60 +
                  int(tp_mat[3]))*1000
        tag_td_mat = tag_td_pat.search(line)
        tag = tag_td_mat[1]
        td = tag_td_mat[2]
        if s_mat:
            task = s_mat.group(1)
            stat = Stat()
            stat.send_tp = tp
            stat.send_td = td
            stat.tag = tag
            cls._stat_dict[tag] = stat
            cls._req_num += 1
            cls._total_num += 1
            if cls._req_num > cls._max_req_num:
                cls._max_req_num = cls._req_num
            fetch_iv = tp - cls._intv_dict.get(task, tp)
            if cls._max_req_intv < fetch_iv:
                cls._max_req_intv = fetch_iv
                cls._max_req_intv_task = task
                cls._max_req_intv_tag = tag
                # print(f"task {task} tag {tag}")
                # print(f"tag {tag} int {fetch_iv}")
                # tt=cls._intv_dict.get(task, tp)
                # hh=tt//3600000
                # tt-=hh*3600000
                # mm=tt//60000
                # tt-=mm*60000
                # ss=tt//1000
                # tt-=ss*1000
                # tt2=tp
                # hh2=tt2//3600000
                # tt2-=hh2*3600000
                # mm2=tt2//60000
                # tt2-=mm2*60000
                # ss2=tt2//1000
                # tt2-=ss2*1000
                # print(f"task {task} tag {tag} interval {fetch_iv} {hh}:{mm}:{ss}.{tt}-{hh2}:{mm2}:{ss2}.{tt2}")
            cls._req_intv_list.append(fetch_iv)
        else:
            task = r_mat.group(1)
            stat = cls._stat_dict.get(tag)
            if not stat:
                return
            stat.recv_tp = tp
            stat.recv_td = td
            # reserve 1 significant num
            fetch_td = stat.recv_tp - stat.send_tp
            if cls._max_req_time < fetch_td:
                cls._max_req_time = fetch_td
            tens = int(fetch_td/10)
            hans = int(fetch_td/100)
            kilos = int(fetch_td/1000)
            stat._total_td = kilos * 1000 if kilos else hans * \
                100 if hans else tens * 10  # 1 significant num
            # stat._total_td = int(float("{:.1g}".format(intfetch_td))) # 1 significant num
            cls._req_num -= 1
            size = int(int(size_pat.search(line).group(1)) /
                       1024 / 1024)  # precision: MB
            cls._size_dict[size] = cls._size_dict.get(size, 0) + 1
            cls._intv_dict[task] = tp
            cls._req_time_list.append(fetch_td)

    @classmethod
    def clear(cls):
        cls._stat_dict = {}
        cls._size_dict = {}
        cls._intv_dict = {}
        cls._req_time_list = []
        cls._req_intv_list = []
        cls._max_req_time = 0
        cls._max_req_num = 0
        cls._max_req_intv = 0
        cls._total_num = 0
        cls._req_num = 0

def show_stat(title):
    print("{:+^32}".format(title))
    iv_pak = Stat.req_int_stat()
    tm_pak = Stat.req_time_stat()
    print("Num of requests       :{}".format(Stat.total_num()))
    print("Max requests in flight:{}".format(Stat.max_req_num()))
    print("Max requests time cost:{} ms, avg:{} ms, 80%:{} ms, 99%:{} ms".format(
        tm_pak[0], tm_pak[1], tm_pak[2], tm_pak[3]))
    print("Max requests interval :{} ms, avg:{} ms, 80%:{} ms, 99%:{} ms".format(
        iv_pak[0], iv_pak[1], iv_pak[2], iv_pak[3]))
    task, tag = Stat.req_int_tag()
    print("Max requests interval : task-{} tag-{}".format(task,tag))
    # # size stats
    # print("{:>8} | {}".format("size/MB", "count"))
    # print("{:->9}|{:->5}".format("", ""))
    # for size, count in sorted(Stat.size_dict().items()):
    #     print("{:8} | {}".format(size, count))
    # # time stats
    # aggregated_stats = {}
    # stats = Stat.stat_dict()
    # for tag, stat in stats.items():
    #     k = int(stat.total_time())
    #     aggregated_stats[k] = aggregated_stats.get(k, 0) + 1
    # # limit = 10
    # print("")
    # print("{:>8} | {}".format("time/ms", "count"))
    # print("{:->9}|{:->5}".format("", ""))
    # for time, count in sorted(aggregated_stats.items(), reverse=True):
    #     # if limit == 0:
    #     #     break
    #     # limit -= 1
    #     print("{:8} | {}".format(time, count))

def parse_file(file, clear=True):
    if clear:
        Stat.clear()
    with open(file, 'r') as f:
        for l in f.readlines():
            Stat.parse(l)
    if clear:
        show_stat(file[file.rfind('/')+1:])

def parse_dir(dir, clear=True):
    if clear:
        Stat.clear()
    for subdir in sorted(os.listdir(dir)):
        file = f"{dir}/{subdir}/stderr"
        parse_file(file, clear=False)
    if clear:
        show_stat(dir[dir.rfind('/')+1:])

if __name__ == '__main__':
    ignores = set()
    # for i in range(147,159):
    #     ignores.add(f"application_1683712749957_{i:0>4}")
    # print(ignores)
    allowd = set()
    allowd.add("application_1683808826051_0051")
    allowd.add("application_1683808826051_0052")
    allowd.add("application_1683808826051_0053")
    allowd.add("application_1683808826051_0054")
    allowd.add("application_1683808826051_0068")
    allowd.add("application_1683808826051_0069")
    allowd.add("application_1683808826051_0070")
    allowd.add("application_1683808826051_0071")
    # for i in range(147,159):
    #     allowd.add(f"application_1683712749957_{i:0>4}")
    root = "userlogs"
    for dir in sorted(os.listdir(root)):
        if ignores and dir in ignores:
            continue
        if allowd and dir not in allowd:
            continue
        parse_dir(f"{root}/{dir}")
