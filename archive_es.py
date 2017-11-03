#!/usr/bin/env python
# -*- coding: utf-8 -*-
import elasticsearch
import curator
import re
from curator.utils import get_date_regex
from elasticsearch.client import SnapshotClient
from elasticsearch.client import IndicesClient
import time
import subprocess
import sys

'''
查找{N}天之前的索引
for day in days:
    创建一天的快照仓库
    创建快照
    将快照文件夹打包
    删除ES快照仓库
    删除快照文件夹
    删除索引
endfor
'''

#按照日期进行分组，每组包含日期当中所有的索引
def get_archive_group(days_before):
    client = get_es_client();
    m = curator.IndexList(client)
    timestring="%Y.%m.%d"
    m.filter_by_age(source="name",direction="older",timestring=timestring,unit='days',unit_count=days_before)
    indices = m.working_list()
    group = {}
    regex = r'(?P<date>{0})'.format(get_date_regex(timestring))
    pattern = re.compile(regex)
    for indice in indices:
        match = pattern.search(indice)
        if not match.group('date'):
            continue
        date = match.group('date')
        if date not in group:
            group[date] = []
            
        group[date].append(indice)
        
    return group
    
def get_es_client():
    return elasticsearch.Elasticsearch()    
    

client = get_es_client();
m = curator.IndexList(client)
indice_client = IndicesClient(client)
days_before = int(sys.argv[1])
repo_path = sys.argv[2]
archive_path = sys.argv[3]
suffix = sys.argv[4]

groups = get_archive_group(days_before)
for group in groups:
    indices = groups[group]
    indices = (',').join(indices)
    #reopen indices
    indice_client.open(indices)
    print indices
    time.sleep(100)
    snap = SnapshotClient(client)
    #create repository
    snap.create_repository('backup',{
            "type":"fs",
            "settings":{
                    "compress":True,
                    "location":repo_path+"/backup"
            }
    })
    #create snapshot
    snap.create(repository='backup',snapshot=group,body={
            "indices":indices,
            "ignore_unavailable":True,
            "include_global_state":False
    },wait_for_completion=True,request_timeout=800)
    #tar snapshot folder and move to special dir
    cmd = "cd %s && tar cjSf %s.tar.bz2 %s && mv %s.tar.bz2 %s.tar.bz2" % (repo_path,'backup','backup','backup',archive_path+'/backup_es_snap_'+group+suffix)
    print cmd
    subprocess.call(cmd,shell=True)
    
    #删除ES快照仓库
    snap.delete_repository('backup')
    #删除快照文件夹
    subprocess.call("rm -rf "+repo_path+"/backup",shell=True)
    #删除索引
    #indice_client.delete(indices)