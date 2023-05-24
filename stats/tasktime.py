#!/usr/bin/env python
# -*- coding: utf-8 -*-

#!/usr/bin/env python3
import re
import os
import math


pat=re.compile(' (?P<hh>\d+):(?P<mm>\d+):(?P<ss>\d+(\.\d+)*).*? [Tt]ask (?P<task>\d+)')
stage_pat=re.compile('stage (?P<stage>\d+\.\d+)')

task_stat={}
start=0
file='stderr'
par='application_1679408925599_0011'
par='.'
for dir in os.listdir(path=par):
    file=f'{par}/{dir}/stderr'
    with open(file,'r+') as rf:
        for l in rf.readlines():
            mat = pat.search(l)
            if mat:
                tp = int(mat.group('hh'))*3600 + \
                    int(mat.group('mm'))*60 + \
                    float(mat.group('ss'))
                if not start:
                    start = tp
                task = mat.group('task')
                if task_stat.get(task):
                    task_stat[task]['during'][-1] = tp - start
                    task_stat[task]['count'] += 1
                else:
                    task_stat[task] = {'during':[tp - start, tp - start],'count':1,'stage':''}
                if not task_stat[task]['stage']:
                    stage_mat = stage_pat.search(l)
                    if stage_mat:
                        task_stat[task]['stage'] = stage_mat.group('stage')
                        task_stat[task]['file'] = file

print('|{:>5}|{:>8}|{:>8}|{:>8}|{:>5}|{:>5}|'.format('task','cost','start','end','stage','count'))
print('|{:->5}|{:->8}|{:->8}|{:->8}|{:->5}|{:->5}|'.format('','','','','',''))
sorted_stat = sorted(task_stat.items(), key=lambda _: _[1]['during'][0])
for task,stat in sorted_stat:
    td=stat['during'][1] - stat['during'][0]
    td=0.5 if td < 1 else td
    tc=int(math.log2(td))
    tc=0 if tc < 0 else tc
    print('|{:>5}|{:>8.3f}|{:>8.3f}|{:>8.3f}|{:>5}|{:>5}|{}|{}'.format(task,
                                                        stat['during'][1] - stat['during'][0],
                                                        stat['during'][0], stat['during'][1],
                                                        stat['stage'],
                                                        stat['count'],
                                                        stat['file'],
                                                        f'{{:#<{tc}}}'.format('')))
# os.rename('stdout.bak','stdout')
