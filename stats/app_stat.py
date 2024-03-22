#!/usr/bin/env python3

import re
import json
import os
import argparse
import time
import lz4framed
import pandas as pd


class AliasExistsError(KeyError):
    pass


class Context:
    def __init__(self):
        self._envs = {}
        self._apps = {}
        self._errs = []

    @property
    def envs(self):
        return self._envs

    @property
    def apps(self):
        return self._apps

    @property
    def errs(self):
        return self._errs

    def commit(self, app, err=None):
        if not err:
            assert app.is_done(), "Incomplete log"
            self._apps[app.event_id()] = app
            self._envs.setdefault(app.env.event_id(), {})
            self._envs[app.env.event_id()][app.event_id()] = app
        else:
            self._errs.append(err)


class Event:
    def __init__(self):
        self._id = self._launch_time = self._finish_time = -1

    def try_parse(self, jstr):
        js_obj = self.try_load(jstr)
        return self._parse(js_obj) if js_obj else None

    def _parse(self, js_obj):
        if self._id == -1 and self.has_id(js_obj):
            self._id = self.parse_id(js_obj)
        if self._launch_time == -1 and self.has_launch_stamp(js_obj):
            self._launch_time = self.parse_launch_stamp(js_obj)
        if self._finish_time == -1 and self.has_finish_stamp(js_obj):
            self._finish_time = self.parse_finish_stamp(js_obj)
            self.parse_done()
        return self

    def try_load(self, jstr):
        raise NotImplementedError("try_load() not implemented")

    def has_id(self, _js_obj):
        return False

    def parse_id(self, _js_obj):
        return -1

    def has_launch_stamp(self, _js_obj):
        return False

    def parse_launch_stamp(self, _js_obj):
        return -1

    def has_finish_stamp(self, _js_obj):
        return False

    def parse_finish_stamp(self, _js_obj):
        return -1

    def parse_done(self):
        pass

    def is_done(self):
        return self._finish_time != -1

    def event_id(self):
        return self._id

    def compute_total_time(self):
        assert self._finish_time != -1 and self._launch_time != - 1, \
            "launch/finish time not parsed"
        return round((self._finish_time - self._launch_time) / 1000, 1)


class Environment(Event):
    PROPERTIES = {
        'spark.executorEnv.UCX_USE_MT_MUTEX': ['UCX_USE_MT_MUTEX', ],
        'spark.executorEnv.UCX_SOCKADDR_TLS_PRIORITY': ['UCX_SOCKADDR_TLS_PRIORITY', ],
        'spark.executorEnv.UCX_IB_TX_CQE_ZIP_ENABLE': ['UCX_IB_TX_CQE_ZIP_ENABLE', ],
        'spark.executorEnv.UCX_IB_RX_CQE_ZIP_ENABLE': ['UCX_IB_RX_CQE_ZIP_ENABLE', ],
        'spark.executorEnv.UCX_IB_PCI_RELAXED_ORDERING': ['UCX_IB_PCI_RELAXED_ORDERING', ],
        'spark.executorEnv.UCX_DC_MLX5_DCT_PORT_AFFINITY': ['UCX_DC_MLX5_DCT_PORT_AFFINITY', ],
        'spark.executorEnv.UCX_RC_SRQ_TOPO': ['UCX_RC_SRQ_TOPO', ],
        'spark.executorEnv.UCX_RNDV_SCHEME': ['UCX_RNDV_SCHEME', ],
        'spark.executorEnv.UCX_RNDV_THRESH': ['UCX_RNDV_THRESH', ],
        'spark.executorEnv.UCX_ZCOPY_THRESH': ['UCX_ZCOPY_THRESH', ],
        'spark.executorEnv.UCX_TLS': ['UCX_TLS', ],
        'spark.shuffle.ucx.numListenerThreads': ['numListenerThreads', 'listener', ],
        'spark.shuffle.ucx.numClientWorkers': ['numClientWorkers', ],
        'spark.shuffle.ucx.numIoThreads': ['numIoThreads', ],
        'spark.shuffle.ucx.useWakeup': ['useWakeup', ],
        'spark.shuffle.ucx.memory.preAllocateBuffers': ['preAllocateBuffers', ],
        'spark.shuffle.ucx.maxBlocksPerRequest': ['maxBlocksPerRequest', ],
        'spark.reducer.maxSizeInFlight': ['maxSizeInFlight', ],
        'spark.reducer.maxReqsInFlight': ['maxReqsInFlight', ],
        'spark.executor.instances': ['instances', 'exe', ],
        'spark.executor.cores': ['cores', ],
        'spark.executor.memory': ['memory', ],
        'spark.executor.extraClassPath': ['extraClassPath', ],
        'spark.driver.memory': ['driver.memory', ],
        'spark.driver.extraClassPath': ['driver.extraClassPath', ],
        'spark.memory.fraction': ['fraction', ],
        'spark.memory.storageFraction': ['storageFraction', ],
        'spark.shuffle.manager': ['manager', ],
        'spark.shuffle.memoryFraction': ['shuffle.memoryFraction', ],
        'spark.storage.memoryFraction': ['storage.memoryFraction', ],
        'spark.shuffle.service.enabled': ['enable', ],
        'spark.nvkv.nvkvRemoteReadBufferSize': ['nvkvRemoteReadBufferSize', ],
        'spark.nvkv.nvkvNumOfReadBuffers': ['nvkvNumOfReadBuffers', ],
    }
    other_allows = set()

    alias_map = {}

    for _property, _aliases in PROPERTIES.items():
        for alias in _aliases:
            if alias in alias_map:
                raise AliasExistsError(
                    f"{_property} and {alias_map.get(alias)} use same alias {alias}")
            alias_map[alias] = _property
        alias_map[_property] = _property

    def __init__(self):
        super().__init__()
        self.__prop = {}

    def __eq__(self, __value) -> bool:
        return self.__prop == __value.prop if isinstance(__value, Environment) else False

    @property
    def other(self):
        if "other" not in self.__prop:
            self.parse_other()
        return self.__prop["other"]

    @property
    def prop(self):
        return self.__prop

    def try_load(self, jstr):
        if "SparkListenerEnvironmentUpdate" not in jstr:
            return None
        return json.loads(jstr)

    def has_id(self, _js_obj):
        return True

    def parse_id(self, js_obj):
        spark_prop = js_obj['Spark Properties']
        prop = self.__prop
        for key, alias in self.PROPERTIES.items():
            val = spark_prop.get(key)
            prop[key] = val if val else ''
            for _ in alias:
                prop[_] = prop[key]
        if 'UcxShuffleManager' in prop['manager']:
            prop['tls'] = "ucx/{}".format(prop['UCX_TLS'])
        else:
            prop['tls'] = "tcp"
        prop['jar'] = prop['extraClassPath'].split(':')[-1]
        self.parse_system_prop(js_obj["System Properties"])
        self.parse_other()
        return hash(repr(self.__prop))

    def parse_system_prop(self, sys_prop):
        command = sys_prop["sun.java.command"]
        argpat = re.compile(r"spark-internal (\d+) (\d+) (\d+) (\d+)")
        argmat = argpat.search(command)
        if argmat:
            self.__prop["shuffle"] = "{}GB".format(
                int(argmat.group(1))
                * int(argmat.group(2))
                * int(argmat.group(3))
                // 2**30
            )
            self.__prop["cmd"] = str(argmat.group(0))

    def parse_other(self):
        other = ''
        for k, v in self.__prop.items():
            if v and k in self.other_allows:
                if other:
                    other = "{},{}={}".format(other, k, v)
                else:
                    other = "{}={}".format(k, v)
        self.__prop["other"] = other

    def is_done(self):
        return self.event_id() != -1

    @staticmethod
    def add_properties(props):
        for prop in props:
            Environment.add_property(prop)

    @staticmethod
    def add_property(prop):
        properties = Environment.PROPERTIES
        allows = Environment.other_allows
        alias_map = Environment.alias_map
        if prop in properties:
            shortname = properties[prop][-1]
            allows.add(shortname)
        elif prop in alias_map:
            realname = alias_map[prop]
            shortname = properties[realname][-1]
            allows.add(shortname)
        else:
            prop_items = prop.split('.')
            for i in range(len(prop_items) - 1, -1, -1):
                shortname = ''.join(prop_items[i:])
                if shortname not in alias_map:
                    properties[prop] = [shortname]
                    alias_map[prop] = alias_map[shortname] = prop
                    allows.add(shortname)
                    break

    @staticmethod
    def get_properties():
        return Environment.PROPERTIES.keys()


class Application(Event):
    def __init__(self):
        super().__init__()
        self.__env = Environment()
        self.__stage = [Stage(self), Stage(self), Stage(self)]
        self.__stage_id = 0
        self.__prop = self.__env.prop

    @property
    def env(self):
        return self.__env

    @property
    def prop(self):
        return self.__prop

    @property
    def stage(self):
        return self.__stage

    def try_parse(self, jstr):
        if not self.__env.is_done():
            return self.__env.try_parse(jstr)
        assert "JobFailed" not in jstr, json.loads(
            jstr)["Job Result"]["Exception"]["Message"][:32]
        if self.event_id() != -1 and self.__stage_id < 3:
            return self.__stage[self.__stage_id].try_parse(jstr)
        return super().try_parse(jstr)

    def try_load(self, jstr):
        if "SparkListenerApplication" not in jstr:
            return None
        return json.loads(jstr)

    def has_id(self, _js_obj):
        return True

    def parse_id(self, js_obj):
        app_id = int(re.sub(re.compile(r"[^\d]"), "", js_obj["App ID"]))
        self.__prop["id"] = app_id
        self.__prop["time"] = "NA"
        return app_id

    def has_launch_stamp(self, js_obj):
        return js_obj["Event"] in "SparkListenerApplicationStart"

    def parse_launch_stamp(self, js_obj):
        return js_obj["Timestamp"]

    def has_finish_stamp(self, js_obj):
        return js_obj["Event"] in "SparkListenerApplicationEnd"

    def parse_finish_stamp(self, js_obj):
        return js_obj["Timestamp"]

    def parse_done(self):
        self.__prop["time"] = self.compute_total_time()
        for i in range(len(self.__stage)):
            self.__prop[f"stage{i}"] = self.__stage[i].compute_total_time()

    def next_stage(self):
        self.__stage_id += 1


class Stage(Event):
    def __init__(self, app):
        super().__init__()
        self.__app = app

    def try_load(self, jstr):
        if "SparkListenerTask" in jstr or "SparkListenerStage" not in jstr:
            return None
        return json.loads(jstr)

    def has_id(self, js_obj):
        return js_obj["Event"] in "SparkListenerStageSubmitted"

    def parse_id(self, js_obj):
        return js_obj["Stage Info"]["Stage ID"]

    def has_launch_stamp(self, js_obj):
        return js_obj["Event"] in "SparkListenerStageCompleted"

    def parse_launch_stamp(self, js_obj):
        return js_obj["Stage Info"]["Submission Time"]

    def has_finish_stamp(self, js_obj):
        return js_obj["Event"] in "SparkListenerStageCompleted"

    def parse_finish_stamp(self, js_obj):
        return js_obj["Stage Info"]["Completion Time"]

    def parse_done(self):
        self.__app.next_stage()


class PrettyStats:
    APP_TABLE_HEAD = (
        "id",
        "exe",
        "shuffle",
        "cores",
        "tls",
        "time",
        "stage0",
        "stage1",
        "stage2",
        "cmd",
        "other",
    )

    AGGR_TABLE_HEAD = (
        "exe",
        "shuffle",
        "cores",
        "tls",
        "time",
        "stage0",
        "stage1",
        "stage2",
        "count",
        "other",
    )

    def __init__(self, ctx):
        self._ctx = ctx

    def show_errs(self):
        for err in self._ctx.errs:
            print(err)

    def show(self, fmt):
        app_table = {}
        for app in self._ctx.apps.values():
            for k in self.APP_TABLE_HEAD:
                app_table.setdefault(k, []).append(app.prop[k])
        df = pd.DataFrame(app_table)
        print(PrettyStats.make_fmt(df, fmt))
        print('')

    def show_aggregated(self, fmt):
        aggr_table = {}
        for stats in self._ctx.envs.values():
            apps = list(stats.values())
            num = len(apps)
            if num == 0:
                continue
            prop = apps[0].prop
            prop["count"] = num
            prop["time"] = sum((apps[i].prop["time"] for i in range(num)))/num
            for stage_id in range(3):
                stage_name = f"stage{stage_id}"
                prop[stage_name] = sum((apps[i].prop[stage_name]
                                       for i in range(num)))/num
            for k in self.AGGR_TABLE_HEAD:
                aggr_table.setdefault(k, []).append(prop[k])
        print(PrettyStats.make_fmt(aggr_table, fmt))

    @staticmethod
    def make_fmt(app_table, fmt):
        df = pd.DataFrame(app_table)
        if fmt in "dataframe":
            return df
        return getattr(df, f"to_{fmt}")()


class EventLogsDir:
    # Length of cluster time stamp(CTS) in application id.
    # e.g. CTS of 'application_1710126456662_0030' is 1710126456662.
    CLUSTER_TIME_STAMP_LEN = 13

    def __init__(self, logs_dir) -> None:
        self._logs_dir = logs_dir
        self._log_files = sorted(os.listdir(logs_dir))

    def get_highest_app_id(self):
        app_id = 0
        for log_file in self._log_files:
            elems = log_file.split('_')
            if len(elems) < 3:
                continue
            try:
                tmp = int(elems[1])
            except ValueError:
                continue
            if tmp > app_id:
                app_id = tmp
        return app_id

    def get_app_list(self, app_id):
        if not app_id:
            return self._log_files
        if len(app_id) > EventLogsDir.CLUSTER_TIME_STAMP_LEN:
            app_filter = "_{}_{}".format(
                app_id[:EventLogsDir.CLUSTER_TIME_STAMP_LEN],
                app_id[EventLogsDir.CLUSTER_TIME_STAMP_LEN:])
        else:
            app_filter = "_{}".format(app_id)
        return [f for f in self._log_files if app_filter in f]


def main():
    parser = argparse.ArgumentParser(
        description='Parse spark application spark-events')
    parser.add_argument('-l', '--list', help="show spark configurations. "
                        "Logs will be aggregated by these configurations",
                        action='store_true')
    parser.add_argument('-f', '--format', help="set output format",
                        choices=['markdown', 'csv', 'html', 'dataframe'], default='dataframe')
    parser.add_argument('-d', '--dir', type=str, default='.',
                        help='set directory of spark-events')
    parser.add_argument('-a', '--app_id', type=str, default='',
                        help='filter logs by application id. e.g. `-a 1705319987792`, '
                        '`-a 17053199877920003`..')
    parser.add_argument('-c', '--config', type=str, nargs='+', default='',
                        help='show additional configurations. '
                        'These will be printed in "other" column. '
                        'e.g. `-c spark.executor.extraClassPath`, '
                        '`-c memory maxReqsInFlight storage.memoryFraction`,..')
    args = parser.parse_args()

    Environment.add_properties(args.config)
    if args.list:
        print("\n".join(Environment.get_properties()))
        exit(0)

    logs_dir = args.dir
    app_id = args.app_id

    event_logs = EventLogsDir(logs_dir)
    apps = event_logs.get_app_list(app_id)

    ctx = Context()
    total_apps = len(apps)
    fin_apps = 0
    time_start = time.perf_counter()
    for i in range(total_apps):
        log_file = apps[i]
        if log_file.endswith("inprogress") or os.path.isdir(f"{logs_dir}/{log_file}"):
            continue
        filelines = None
        if log_file.endswith("lz4"):
            with open(f"{logs_dir}/{log_file}", 'rb', encoding='utf8') as f:
                filelines = [lz4framed.decompress("".join(f.readlines()))]
        else:
            with open(f"{logs_dir}/{log_file}", encoding='utf8') as f:
                filelines = f.readlines()
        try:
            app = Application()
            for l in filelines:
                app.try_parse(l)
            ctx.commit(app)
        except AssertionError as e:
            ctx.commit(app, (log_file, e))
        fin_apps += len(filelines)
        lines_ps = fin_apps // (time.perf_counter() - time_start)
        progress = (i + 1) * 100 // total_apps
        print("\rParsing: {}%|{}l/s: ".format(progress, lines_ps),
              "▓" * (progress >> 1), end="", flush=True)
    print("", flush=False)

    pretty = PrettyStats(ctx)
    pretty.show_errs()
    pretty.show(fmt=args.format)
    if len(apps) > 1:
        pretty.show_aggregated(fmt=args.format)


if __name__ == '__main__':
    main()
