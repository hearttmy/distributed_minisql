# -*- coding: UTF-8 -*-
import hashlib
import logging
import random
import sys
import time
from threading import Condition

from kazoo.client import KazooClient

hosts = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'
logging.basicConfig(level=logging.WARNING, stream=sys.stdout)
# 创建一个客户端，可以指定多台zookeeper，
zk = KazooClient(hosts=hosts, logger=logging)
server_list = ["minisql1", "minisql2", "minisql3"]
condition = Condition()
dataWatchFinished = 1


def cmd_get_sql():
    sql = ''
    s = input('MiniSQL>  ')
    while True:
        if s.rstrip().endswith(';'):
            sql += ' ' + s
            return sql
        else:
            sql += ' ' + s
            s = input()


def get_min_records():
    tmp = []
    for server in server_list:
        tmp.append(zk.get("/servers/{}/info/recordNum".format(server)))


def get_min_tables():
    tmp = []
    for server in server_list:
        tmp.append(zk.get("/servers/{}/info/tableNum".format(server)))


def get_target_server(sql):
    tmp = sql.lstrip(' ')
    ans = ['minisql1']
    if tmp[0] == 'create' and tmp[1] == 'table':  # backup
        pass
    elif tmp[0] == 'select':  # balancing
        pass
    else:
        pass
    return ans


def get_path_list(target_server, sql):
    ans = []
    m = hashlib.sha256()
    for server in target_server:
        m.update((str(random.random()) + sql + str(time.time())).encode('utf-8'))
        node_name = m.hexdigest()
        ans.append('/servers/{}/instructions/{}'.format(server, node_name))
    return ans


def deleteFinishedNode(path_list):
    for path in path_list:
        if zk.exists(path):
            zk.delete(path, recursive=True)


def set_sql_and_watchers(path_list, sql):
    for path in path_list:
        zk.create(path, sql)
        zk.ensure_path(path + '/result')
        zk.DataWatch(path + '/result', watch_result_node)


# 当节点kazoo的数据变化时这个函数会被调用
# 如果节点被删除这个函数也会被调用，但是data和stat都是None
def watch_result_node(data, stat):
    global dataWatchFinished
    condition.acquire()

    if stat and data:
        dataWatchFinished += 1
        data_str = data.decode("utf-8")
        print("Result: \n" + data_str)

    condition.notify()
    condition.release()


if __name__ == '__main__':
    m = hashlib.sha256()
    zk.start()
    target_server = []
    path_list = []
    node_name = 'test'
    while True:
        condition.acquire()
        while dataWatchFinished != 1:
            condition.wait()

        deleteFinishedNode(path_list)

        sql = cmd_get_sql()
        target_server = get_target_server(sql)
        path_list = get_path_list(target_server, sql)
        print(path_list)

        # quit and file exec
        set_sql_and_watchers(path_list, bytes(sql, encoding="utf-8"))

        dataWatchFinished = 0
        condition.release()
