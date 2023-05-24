#!/usr/bin/env python3

import re
import json
import os
import lz4framed

'''
v=182   # last app id 
d=6     # different groups
t=5     # repeat times
for i in range(d):
    print([v + i * t + j for j in range(t)])

3 norm
3 pre
2 norm
2 pre
1 norm
1 pre
tcp
'''


class Abs:
    def __init__(self) -> None:
        self.launch_jo = None
        self.finish_jo = None
        self._id = -1
        self._launch_time = 0
        self._finish_time = 0

    def id(self):
        return self._id

    def total_time(self):
        return round((self._finish_time - self._launch_time) / 1000, 1)


class Environment(Abs):
    inst = None
    objs = {}
    alias_keys = {
        'mode': 'spark.executorEnv.UCX_SOCKADDR_TLS_PRIORITY',
        'seg': 'spark.executorEnv.UCX_TCP_TX_SEG_SIZE',
        'tls': 'spark.executorEnv.UCX_TLS',
        'listener': 'spark.shuffle.ucx.numListenerThreads',
        'client': 'spark.shuffle.ucx.numClientWorkers',
        'io': 'spark.shuffle.ucx.numIoThreads',
        'wake': 'spark.shuffle.ucx.useWakeup',
        'buffer': 'spark.shuffle.ucx.memory.preAllocateBuffers',
        'blocks': 'spark.shuffle.ucx.maxBlocksPerRequest',
        'flysz': 'spark.reducer.maxSizeInFlight',
        'flyno': 'spark.reducer.maxReqsInFlight',
        'cores': 'spark.executor.cores',
        'emem': 'spark.executor.memory',
        'dmem': 'spark.driver.memory',
        'driver': 'spark.driver.extraClassPath',
        'mutex': 'spark.executorEnv.UCX_USE_MT_MUTEX',
        'para': 'spark.default.parallelism',
        'm%': 'spark.memory.fraction',
        's%': 'spark.memory.storageFraction',
        'msh%': 'spark.shuffle.memoryFraction',
        'mst%': 'spark.storage.memoryFraction'
    }

    def __init__(self) -> None:
        super().__init__()
        self.prop = {}
        self.apps = {}

    def __eq__(self, __value) -> bool:
        return self.prop.__eq__(__value.prop) if type(__value) is Environment else False

    def parse_launch(self, launch_jo):
        spark_prop = launch_jo['Spark Properties']
        for alias, key in self.alias_keys.items():
            val = spark_prop.get(key)
            self.prop[alias] = val if val else ''
        self.parse_other()
        self._id = hash(repr(self.prop))
        self.launch_jo = launch_jo

    def parse_finish(self, finish_jo):
        self.finish_jo = finish_jo

    def parse_other(self):
        prop = self.prop
        flyno = prop['flyno']
        flysz = prop['flysz']
        blks = prop['blocks']
        buff = prop['buffer']
        mu = prop['mutex']
        iot = prop['io']
        wake = prop['wake']
        seg = prop['seg']
        drv = prop['driver']
        mfra = prop['m%']
        sfra = prop['s%']
        msh = prop['msh%']
        mst = prop['mst%']
        if mfra:
            other = f"m%:{mfra}"
        elif sfra:
            other = f"s%:{sfra}"
        elif msh:
            other = f"msh%:{msh}"
        elif mst:
            other = f"mst%:{mst}"
        elif flyno:
            other = f"fly:{flyno}"
        elif flysz:
            other = f"fly:{flysz}"
        elif blks:
            other = f"blk:{blks}"
        elif buff:
            other = re.sub(re.compile("\d+:"), '', buff)
        elif wake:
            other = "wake:y" if 'false' in wake else "wake:n"
        elif mu:
            other = 'mutex:y' if 'yes' in mu else 'mutex:n'
        elif iot:
            other = f"io:{iot}"
        elif seg:
            other = f"seg:{seg}"
        elif drv:
            other = 'patch' if 'ucx_rel' in drv else 'master'
        else:
            other = ''
        prop['other'] = other

    @classmethod
    def clear(cls):
        cls.inst = None
        cls.objs.clear()

    @classmethod
    def create(cls, js_obj):
        event = js_obj["Event"]
        if event in "SparkListenerEnvironmentUpdate":
            obj = Environment()
            obj.parse_launch(js_obj)
            obj.parse_finish(js_obj)
            cls.objs.setdefault(obj.id(), obj)
            cls.inst = cls.objs[obj.id()]
            return cls.inst
        return None

    @staticmethod
    def s_id(js_obj):
        return int(re.search(re.compile("\d+$"), js_obj['Spark Properties']["spark.app.id"]).group(0))


class Application(Abs):
    inst = None
    objs = {}

    def __init__(self) -> None:
        super().__init__()
        self.job = {}
        self.stage = {}
        self.task = {}
        self.env = None

    def parse_environ(self):
        self.env = Environment.inst

    def parse_launch(self, launch_jo):
        self._id = self.s_id(launch_jo)
        self.launch_jo = launch_jo
        self._launch_time = self.launch_jo["Timestamp"]

    def parse_finish(self, finish_jo):
        self.finish_jo = finish_jo
        self._finish_time = self.finish_jo["Timestamp"]

    @classmethod
    def clear(cls):
        cls.inst = None
        cls.objs.clear()

    @classmethod
    def create(cls, js_obj):
        event = js_obj["Event"]
        if event in "SparkListenerApplicationStart":
            obj = Application()
            obj.parse_launch(js_obj)
            obj.parse_environ()
            cls.inst = obj
            cls.objs[obj.id()] = obj
            Environment.inst.apps[obj.id()] = obj
            return obj
        if event in "SparkListenerApplicationEnd":
            obj = cls.inst
            obj.parse_finish(js_obj)
            return obj
        return None

    @staticmethod
    def s_id(js_obj):
        # return int(re.search(re.compile("\d+$"), js_obj["App ID"]).group(0))
        return int(re.sub(re.compile("[^\d]"), "", js_obj["App ID"]))


class Job(Abs):
    objs = {}

    def __init__(self) -> None:
        super().__init__()

    def parse_launch(self, launch_jo):
        self.launch_jo = launch_jo
        self._id = self.s_id(launch_jo)
        self._launch_time = self.launch_jo["Submission Time"]

    def parse_finish(self, finish_jo):
        self.finish_jo = finish_jo
        self._finish_time = self.finish_jo["Completion Time"]

    @classmethod
    def clear(cls):
        cls.objs.clear()

    @classmethod
    def create(cls, js_obj):
        event = js_obj["Event"]
        if event in "SparkListenerJobStart":
            obj = Job()
            obj.parse_launch(js_obj)
            cls.objs[obj.id()] = obj
            Application.inst.job[obj.id()] = obj
            return obj
        if event in "SparkListenerJobEnd":
            obj = cls.objs.get(Job.s_id(js_obj))
            obj.parse_finish(js_obj)
            return obj
        return None

    @staticmethod
    def s_id(js_obj):
        return js_obj["Job ID"]


class Stage(Abs):
    objs = {}

    def __init__(self) -> None:
        super().__init__()

    def parse_launch(self, launch_jo):
        self._id = self.s_id(launch_jo)
        self.launch_jo = launch_jo
        self._launch_time = self.launch_jo["Stage Info"]["Submission Time"]

    def parse_finish(self, finish_jo):
        self.finish_jo = finish_jo
        self._finish_time = self.finish_jo["Stage Info"]["Completion Time"]

    @classmethod
    def clear(cls):
        cls.objs.clear()

    @classmethod
    def create(cls, js_obj):
        event = js_obj["Event"]
        if event in "SparkListenerStageSubmitted":
            obj = Stage()
            obj.parse_launch(js_obj)
            cls.objs[obj.id()] = obj
            Application.inst.stage[obj.id()] = obj
            return obj
        if event in "SparkListenerStageCompleted":
            obj = cls.objs[Stage.s_id(js_obj)]
            obj.parse_finish(js_obj)
            return obj
        return None

    @staticmethod
    def s_id(js_obj):
        return js_obj["Stage Info"]["Stage ID"]


class Task(Abs):
    objs = {}

    def __init__(self) -> None:
        super().__init__()
        self._sid = -1

    def parse_launch(self, launch_jo):
        self._id = self.s_id(launch_jo)
        self._sid = self.s_sid(launch_jo)
        self.launch_jo = launch_jo
        self._launch_time = self.launch_jo["Task Info"]["Launch Time"]

    def parse_finish(self, finish_jo):
        self.finish_jo = finish_jo
        self._finish_time = self.finish_jo["Task Info"]["Finish Time"]

    @classmethod
    def clear(cls):
        cls.objs.clear()

    @classmethod
    def create(cls, js_obj):
        event = js_obj["Event"]
        if event in "SparkListenerTaskStart":
            obj = Task()
            obj.parse_launch(js_obj)
            Task.objs[obj.id()] = obj
            Application.inst.task[obj.id()] = obj
            return obj
        if event in "SparkListenerTaskEnd":
            obj = Task.objs.get(Task.s_id(js_obj))
            obj.parse_finish(js_obj)
            return obj
        return None

    @staticmethod
    def s_id(js_obj):
        return js_obj["Task Info"]["Task ID"]

    @staticmethod
    def s_sid(js_obj):
        return js_obj["Task Info"]["Stage ID"]


class AutoStats:
    @staticmethod
    def show(head='master'):
        print("|{:<8s}|{:>4s}|{:>4s}|{:>8s}|{:>8s}|{:>8s}|{:>8s}|{:>8s}|{:>8s}|{:>6s}|".format(
            "branch", "core", "lisn", "tls", "other", "time", "stage0", "stage1", "stage2", "count"))
        print("|{:-<8s}|{:->4s}|{:->4s}|{:->8s}|{:->8s}|{:->8s}|{:->8s}|{:->8s}|{:->8s}|{:->6s}|".format(
            "", "", "", "", "", "", "", "", "", ""))
        for _, stats in Environment.objs.items():
            key = stats.prop
            apps = list(stats.apps.values())
            num = len(apps)
            if num == 0:
                return
            core = key['cores']
            lisn = key['listener']
            tls = key['tls']
            other = key['other']
            app = sum((apps[i].total_time() for i in range(num)))/num
            st0 = sum((apps[i].stage[0].total_time() for i in range(num)))/num
            st1 = sum((apps[i].stage[1].total_time() for i in range(num)))/num
            st2 = sum((apps[i].stage[2].total_time() for i in range(num)))/num
            print("|{:<8s}|{:>4s}|{:>4s}|{:>8s}|{:>8s}|{:>8.1f}|{:>8.1f}|{:>8.1f}|{:>8.1f}|{:>6d}|".format(
                head, core, lisn, tls, other, app, st0, st1, st2, num))


class GroupStats:
    def __init__(self) -> None:
        self.keys = GroupStats.create_keys(20)
        self.groups = GroupStats.create_groups(20, 1, 64)
        self.key_to_gid = {}
        self.app_to_gid = {}
        self.app_st_group = {}
        gid = 1
        for key in self.keys:
            self.key_to_gid[key] = gid
            gid += 1
        gid = 1
        for group in self.groups:
            self.app_st_group[gid] = {}
            for aid in group:
                self.app_to_gid[aid] = gid
            gid += 1

    @staticmethod
    def create_keys(group_counts):
        keys = [("master", i) for i in range(group_counts)]
        print(keys)
        return keys

    @staticmethod
    def create_groups(group_counts, member_counts, start_id):
        groups = [[start_id + j + member_counts * i
                   for j in range(member_counts)]
                  for i in range(group_counts)]
        print(groups)
        return groups

    def app(self, app):
        gid = self.app_to_gid.get(app.id())
        if not gid:  # 数据不需要分组
            return
        stat = self.app_st_group.get(gid)
        if not stat.get('app'):
            stat['num'] = 0
            stat['app'] = 0
            stat['stage'] = [0, 0, 0]
            stat['env'] = app.env.prop
        stat['num'] += 1
        stat['app'] += app.total_time()
        for i in range(3):
            stat['stage'][i] += app.stage[i].total_time()

    def show(self):
        print("|{:<8s}|{:>4s}|{:>4s}|{:>8s}|{:>8s}|{:>8s}|{:>8s}|{:>8s}|{:>8s}|".format(
            "branch", "core", "lisn", "tls", "other", "time", "stage0", "stage1", "stage2"))
        print("|{:-<8s}|{:->4s}|{:->4s}|{:->8s}|{:->8s}|{:->8s}|{:->8s}|{:->8s}|{:->8s}|".format(
            "", "", "", "", "", "", "", "", ""))
        for key in self.keys:
            gid = self.key_to_gid.get(key)
            stat = self.app_st_group.get(gid)
            if not stat:  # 从该分组开始没有数据
                return
            env = stat['env']
            core = env['cores']
            lisn = env['listener']
            tls = env['tls']
            buff = re.sub(re.compile("\d+:"), '', env['buffer'])
            blks = env['blocks']
            other = buff if buff else (blks if blks else '')
            num = stat['num']
            app = stat['app']/num
            st0 = stat['stage'][0]/num
            st1 = stat['stage'][1]/num
            st2 = stat['stage'][2]/num
            print("|{:<8s}|{:>4s}|{:>4s}|{:>8s}|{:>8s}|{:>8.1f}|{:>8.1f}|{:>8.1f}|{:>8.1f}|".format(
                key[0], core, lisn, tls, other, app, st0, st1, st2))


if __name__ == '__main__':
    ignores = set()
    for i in range(1,25):
        ignores.add(f"application_1684835692144_{i:0>4}")
    allowed = set()
    for i in range(6,6):
        allowed.add(f"application_1684724279252_{i:0>4}")
    print("|{:<18s}|{:>4s}|{:>4s}|{:>8s}|{:>8s}|{:>8s}|{:>8s}|{:>8s}|{:>8s}|".format(
        "id", "core", "lisn", "tls", "other", "time", "stage0", "stage1", "stage2"))
    print("|{:-<18s}|{:->4s}|{:->4s}|{:->8s}|{:->8s}|{:->8s}|{:->8s}|{:->8s}|{:->8s}|".format(
        "", "", "", "", "", "", "", "", ""))
    logdir = os.environ.get('logdir')
    if not logdir:
        logdir = '/images/hadoop_log'
    for file in sorted(os.listdir(logdir)):
        if file.endswith("inprogress"):
            continue
        if ignores and file in ignores:
            continue
        if allowed and file not in allowed:
            continue
        filelines=None
        if file.endswith("lz4"):
            with open(f"{logdir}/{file}",'rb') as f:
                filelines = [lz4framed.decompress(l) for l in f.readlines()]
        else:
            with open(f"{logdir}/{file}") as f:
                filelines = f.readlines()
        for l in filelines:
            js_obj = json.loads(l)
            try:
                try_stage = Stage.create(js_obj)
                if try_stage:
                    continue
                try_app = Application.create(js_obj)
                if try_app:
                    continue
                try_env = Environment.create(js_obj)
                if try_env:
                    continue
            except:
                continue
        try:
            for aid, app in Application.objs.items():
                key = app.env.prop
                log = "|{:<18d}|{:>4s}|{:>4s}|{:>8s}|{:>8s}|".format(
                    app.id(), key['cores'], key['listener'], key['tls'], key['other'])
                log = "{}{:>8.1f}|".format(log, app.total_time())
                for sid in range(3):
                    stat = Application.inst.stage[sid]
                    log = "{}{:>8.1f}|".format(log, stat.total_time())
            print(log)
        except:
            print(f"ignore {Application.inst.id()}")
            Environment.objs[Application.inst.env.id()].apps.pop(
                Application.inst.id())
            Application.objs.pop(Application.inst.id())
    print('')
    AutoStats.show()
