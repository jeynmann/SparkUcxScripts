#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import os
import sys
import time

create_pat=re.compile('rdma_create_id on client')

ev=['addr','rout','conn','resp','estb','dcon']
ev_num=len(ev)
ev_id={}
ev_name={}

i=0
for e in ev:
    ev_id[e]=i
    ev_name[i]=e
    i+=1

ev_pats={}
ev_str={}
# ------------- rdma -------------
ev_pats['addr']=re.compile('RDMA_CM_EVENT_ADDR_RESOLVED')
ev_pats['rout']=re.compile('RDMA_CM_EVENT_ROUTE_RESOLVED')
ev_pats['conn']=re.compile('RDMA_CM_EVENT_CONNECT_REQUEST')
ev_pats['resp']=re.compile('RDMA_CM_EVENT_CONNECT_RESPONSE')
ev_pats['estb']=re.compile('RDMA_CM_EVENT_ESTABLISHED')
ev_pats['dcon']=re.compile('RDMA_CM_EVENT_DISCONNECTED')

ev_str['addr']='Addr Resolved'
ev_str['rout']='Route Resolved'
ev_str['conn']='Connect Request'
ev_str['resp']='Connect Response'
ev_str['estb']='Established'
ev_str['dcon']='Disconnected'

id_pat=re.compile('cm_id (?P<id>0x[\d\w]+)')
cm_pat=re.compile('cm (?P<cm>0x[\d\w]+)')
ev_pat=re.compile('event_channel (?P<ev>0x[\d\w]+)')
fd_pat=re.compile('fd (?P<fd>0x[\d\w]+)')
tp_pat=re.compile('^\[(?P<tp>\d+)\.\d+\]')
# ------------- rdma -------------

# ------------- tcp -------------
#ev_pats['addr']=re.compile('CLOSED -(>|&gt;) CONNECTING')
#ev_pats['rout']=re.compile('CONNECTING -(>|&gt;) CONNECTING')
#ev_pats['conn']=re.compile('CLOSED -(>|&gt;) ACCEPTING')
#ev_pats['resp']=re.compile('CONNECTING -(>|&gt;) CONNECTED')
#ev_pats['estb']=re.compile('ACCEPTING -(>|&gt;) CONNECTED')
#ev_pats['dcon']=re.compile(' -(>|&gt;) CLOSED')
#
#ev_str['addr']='Closed -> Connecting'
#ev_str['rout']='Connecting -> Connecting'
#ev_str['conn']='Closed -> Accepting'
#ev_str['resp']='Connecting -> Connected'
#ev_str['estb']='Accepting -> Connected'
#ev_str['dcon']='.. -> Closed'

#id_pat=re.compile('tcp_ep (?P<id>0x[\d\w]+)')
# ------------- tcp -------------

id_time={}

path='/tmp'
if len(sys.argv) > 1:
    path=sys.argv[0]

for dir in os.listdir(path):
    file=f'{path}/{dir}/stdout'
    with open(file) as f:
        for line in f.readlines():
            for name,pat in ev_pats.items():
                if pat.search(line):
                    id=id_pat.search(line).group(1)
                    if not id_time.get(id):
                        id_time[id]={}
                    id_time[id][name]=int(tp_pat.search(line).group(1))
    #
    break

stat_time={}
stat_time['add2rou']={}
stat_time['rou2rep']={}
stat_time['rep2dco']={}
stat_time['req2est']={}
stat_time['est2dco']={}

ev_time={}
for e in ev:
    ev_time[e]={}

for tp in id_time.values():
    if tp.get('addr'):
        stat_time['add2rou'][tp['rout']-tp['addr']] = 1+stat_time['add2rou'].get(tp['rout']-tp['addr'],0)
        stat_time['rou2rep'][tp['resp']-tp['rout']] = 1+stat_time['rou2rep'].get(tp['resp']-tp['rout'],0)
        if tp.get('dcon'):
            stat_time['rep2dco'][tp['dcon']-tp['resp']] = 1+stat_time['rep2dco'].get(tp['dcon']-tp['resp'],0)
    if tp.get('conn'):
        stat_time['req2est'][tp['estb']-tp['conn']] = 1+stat_time['req2est'].get(tp['estb']-tp['conn'],0)
        if tp.get('dcon'):
            stat_time['est2dco'][tp['dcon']-tp['estb']] = 1+stat_time['est2dco'].get(tp['dcon']-tp['estb'],0)
    for e in ev:
        if tp.get(e):
            ev_time[e][tp[e]]=ev_time[e].get(tp[e],0)+1

for stat,item in stat_time.items():
    print(f'    - {stat}')
    print('    |{:^8}|{:^8}|'.format('time','count'))
    print('    |{:-^8}|{:-^8}|'.format('',''))
    for dur,cnt in item.items():
        print(f'    |{dur:^8}|{cnt:^8}|')

for e,time_count in ev_time.items():
    print(f'    - {ev_str[e]}')
    print('    |{:^24}|{:^12}|{:^8}|'.format('time','stamp','count'))
    print('    |{:-^24}|{:-^12}|{:-^8}|'.format('','',''))
    for t,c in time_count.items():
        print(f'    |{time.ctime(t):^24}|{t:^12}|{c:^8}|')

