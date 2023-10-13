#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import os
import sys
import time


class CMStat:
    ev_pats = {}
    ev_str = {}
    id_pat = None
    tp_pat = re.compile('^\[(?P<tp>\d+)\.\d+\]')
    # tp_pat = re.compile(' \d+:\d+:(?P<tp>\d+)\.\d+ ')

    @staticmethod
    def rdma_pats_gen():
        CMStat.ev_pats['addr'] = re.compile('RDMA_CM_EVENT_ADDR_RESOLVED')
        CMStat.ev_pats['rout'] = re.compile('RDMA_CM_EVENT_ROUTE_RESOLVED')
        CMStat.ev_pats['accp'] = re.compile('RDMA_CM_EVENT_CONNECT_REQUEST')
        CMStat.ev_pats['conn'] = re.compile('RDMA_CM_EVENT_CONNECT_RESPONSE')
        CMStat.ev_pats['estb'] = re.compile('RDMA_CM_EVENT_ESTABLISHED')
        CMStat.ev_pats['dcon'] = re.compile('RDMA_CM_EVENT_DISCONNECTED')

        CMStat.ev_str['addr'] = 'Addr Resolved'
        CMStat.ev_str['rout'] = 'Route Resolved'
        CMStat.ev_str['accp'] = 'Connect Request'
        CMStat.ev_str['conn'] = 'Connect Response'
        CMStat.ev_str['estb'] = 'Established'
        CMStat.ev_str['dcon'] = 'Disconnected'

        CMStat.id_pat = re.compile('cm_id (?P<id>0x[\d\w]+)')
        CMStat.create_pat = re.compile('rdma_create_id on client')
        CMStat.cm_pat = re.compile('cm (?P<cm>0x[\d\w]+)')
        CMStat.ev_pat = re.compile('event_channel (?P<ev>0x[\d\w]+)')
        CMStat.fd_pat = re.compile('fd (?P<fd>0x[\d\w]+)')

    @staticmethod
    def tcp_pats_gen():
        CMStat.ev_pats['addr'] = re.compile('CLOSED -(>|&gt;) CONNECTING')
        CMStat.ev_pats['rout'] = re.compile('CONNECTING -(>|&gt;) CONNECTING')
        CMStat.ev_pats['accp'] = re.compile('CLOSED -(>|&gt;) (ACCEPTING|RECV_MAGIC_NUMBER)')
        CMStat.ev_pats['conn'] = re.compile('CONNECTING -(>|&gt;) CONNECTED')
        CMStat.ev_pats['estb'] = re.compile('(RECV_MAGIC_NUMBER|ACCEPTING) -(>|&gt;) CONNECTED')
        CMStat.ev_pats['dcon'] = re.compile(' -(>|&gt;) CLOSED')

        CMStat.ev_str['addr'] = 'Closed -> Connecting'
        CMStat.ev_str['rout'] = 'Connecting -> Connecting'
        CMStat.ev_str['accp'] = 'Closed -> Accepting'
        CMStat.ev_str['conn'] = 'Connecting -> Connected'
        CMStat.ev_str['estb'] = 'Accepting -> Connected'
        CMStat.ev_str['dcon'] = '.. -> Closed'

        CMStat.id_pat = re.compile('tcp_ep (?P<id>0x[\d\w]+)')

    def __init__(self) -> None:
        self.ev = ['addr', 'rout', 'accp', 'conn', 'estb', 'dcon']
        self.ev_num = len(self.ev)
        self.ev_id = {}
        self.ev_name = {}
        i = 0
        for e in self.ev:
            self.ev_id[e] = i
            self.ev_name[i] = e
            i += 1
        self.id_time = {}

    def parse(self, file):
        with open(file) as f:
            for line in f.readlines():
                for name, pat in self.ev_pats.items():
                    if pat.search(line):
                        try:
                            id = self.id_pat.search(line).group(1)
                            if not self.id_time.get(id):
                                self.id_time[id] = {}
                            self.id_time[id][name] = int(
                                self.tp_pat.search(line).group(1))
                        except Exception as e:
                            print(line)
                            print(repr(e))
                            exit(0)

    def show(self):
        stat_time = {}
        stat_time['add2rou'] = {}
        stat_time['rou2con'] = {}
        stat_time['con2dco'] = {}
        stat_time['acp2est'] = {}
        stat_time['est2dco'] = {}

        ev_time = {}
        for e in self.ev:
            ev_time[e] = {}

        for id,tp in self.id_time.items():
            try:
                if tp.get('conn'):
                    stat_time['add2rou'][tp['rout']-tp['addr']] = 1 + \
                        stat_time['add2rou'].get(tp['rout']-tp['addr'], 0)
                    stat_time['rou2con'][tp['conn']-tp['rout']] = 1 + \
                        stat_time['rou2con'].get(tp['conn']-tp['rout'], 0)
                    if tp.get('dcon'):
                        stat_time['con2dco'][tp['dcon']-tp['conn']] = 1 + \
                            stat_time['con2dco'].get(tp['dcon']-tp['conn'], 0)
                if tp.get('estb'):
                    stat_time['acp2est'][tp['estb']-tp['accp']] = 1 + \
                        stat_time['acp2est'].get(tp['estb']-tp['accp'], 0)
                    if tp.get('dcon'):
                        stat_time['est2dco'][tp['dcon']-tp['estb']] = 1 + \
                            stat_time['est2dco'].get(tp['dcon']-tp['estb'], 0)
                for e in self.ev:
                    if tp.get(e):
                        ev_time[e][tp[e]] = ev_time[e].get(tp[e], 0)+1
            except Exception as e:
                print(id,tp)
                print(repr(e))
                exit(0)

        for stat, item in stat_time.items():
            print(f'    - {stat}')
            print('    |{:^8}|{:^8}|'.format('time', 'count'))
            print('    |{:-^8}|{:-^8}|'.format('', ''))
            for dur, cnt in item.items():
                print(f'    |{dur:^8}|{cnt:^8}|')

        for e, time_count in ev_time.items():
            print(f'    - {self.ev_str[e]}')
            print('    |{:^24}|{:^12}|{:^8}|{:^4}|'.format(
                'date', 'stamp', 'count', 'time'))
            print('    |{:-^24}|{:-^12}|{:-^8}|{:-^4}|'.format('', '', '', ''))
            beg = 0
            sum = 0
            for t, c in time_count.items():
                if not beg:
                    beg = t
                sum += c
                print(f'    |{time.ctime(t):^24}|{t:^12}|{sum:^8}|{t-beg:^4}|')


if __name__ == '__main__':
    argn = len(sys.argv)
    if argn > 1:
        # print(sys.argv)
        file = sys.argv[1]
        stats = CMStat()
        if argn > 2 and 'rdma' in sys.argv[2]:
            print("use rdma parser")
            stats.rdma_pats_gen()
        else:
            print("use tcp parser")
            stats.tcp_pats_gen()
        print(f'file://{file}')
        stats.parse(file)
        stats.show()
        exit(0)
    ignores = set()
    appid = "1687355954209"
    for i in range(196, 222):
        ignores.add(f"application_{appid}_{i:0>4}")
    allowed = set()
    for i in range(27, 28):
        allowed.add(f"application_{appid}_{i:0>4}")
    path = '/images/userlogs/'
    for app in os.listdir(path):
        if ignores and app in ignores:
            continue
        if allowed and app not in allowed:
            continue
        for container in os.listdir(f'{path}/{app}'):
            file = f'{path}/{app}/{container}/stdout'
            print(f'file://{file}')
            stats = CMStat()
            stats.tcp_pats_gen()
            stats.parse(file)
            stats.show()
            break
        break
