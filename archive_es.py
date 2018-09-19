#!/usr/bin/env python3
from elasticsearch import Elasticsearch
import re
from elasticsearch.client import (
    SnapshotClient,
    IndicesClient
)
import subprocess
import argparse
import time
import curator
from curator.utils import get_date_regex

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


def get_archive_group(days_before,es_client):
    '''
        按照日期进行分组，每组包含日期当中所有的索引
    '''
    query = curator.IndexList(es_client)
    timestring = "%Y.%m.%d"
    query.filter_by_age(source="name",direction="older",timestring=timestring,unit='days',unit_count=days_before)
    indices = query.working_list()
    group = {}
    regex = r'(%s)' % get_date_regex(timestring)
    pattern = re.compile(regex)
    for indice in indices:
        match = pattern.search(indice)
        if not match:
            continue
        date = match.group(1)
        if date not in group:
            group[date] = []
            
        group[date].append(indice)
        
    return group
    

def do_archive(groups,repo_path,archive_path,suffix,es_client,dry_run):
    indice_client = IndicesClient(es_client)
    for group in groups:
        indices = groups[group]
        indices = (',').join(indices)
        #reopen indices
        if dry_run is not True:
            indice_client.open(indices)
        print("try open indices:",indices)
        time.sleep(120)


        snap = SnapshotClient(es_client)
        #create repository
        print("create elasticsearch repo")
        if dry_run is not True:
            snap.create_repository('backup',{
                    "type":"fs",
                    "settings":{
                            "compress":False,
                            "location":repo_path+"/backup"
                    }
            })

        #create snapshot
        print("create elasticsearch snap:",indices)
        if dry_run is not True:
            snap.create(repository='backup',snapshot=group,body={
                    "indices":indices,
                    "ignore_unavailable":True,
                    "include_global_state":False
            },params={
                "wait_for_completion":True,
                "request_timeout":800
            })
        #tar snapshot folder and move to special dir
        cmd = "cd %s && tar cjSf %s.tar.bz2 %s && mv %s.tar.bz2 %s.tar.bz2" % (repo_path,'backup','backup','backup',archive_path+'/backup_es_snap_'+group+suffix)
        print("compress snapshot:",cmd)
        if dry_run is not True:
            subprocess.call(cmd,shell=True)
        
        #删除ES快照仓库
        print("clear snapshot")
        if dry_run is not True:
            snap.delete_repository('backup')
            #删除快照文件夹
            subprocess.call("rm -rf "+repo_path+"/backup",shell=True)

        #删除索引
        print("delete indices:",indices)
        if dry_run is not True:
            indice_client.delete(indices)


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Archive ES old indices")
    parser.add_argument("--host",default="127.0.0.1",help="elasticsearch host")
    parser.add_argument("--port",default="9200",help="elasticsearch port")
    parser.add_argument("--before",default=90,help="to archive how many days before indices")
    parser.add_argument("--dry_run",default="n",choices=['y','n'],help="test run with out realy operate y/n")
    parser.add_argument("--repo_path",default="/tmp/es_snap_repo/",help="path for elasticsearch repo")
    parser.add_argument("--archive_path",default="/tmp/es_archive/",help="path for compressed backup snapshot")
    parser.add_argument("--suffix",default="",help="snapshot backup file suffix")

    args = parser.parse_args()

    if args.dry_run == 'y':
        dry_run = True
    else:
        dry_run = False

    es_client = Elasticsearch(["%s:%s" %(args.host,args.port)])
    groups = get_archive_group(args.before,es_client)

    do_archive(groups,args.repo_path,args.archive_path,args.suffix,es_client,dry_run)